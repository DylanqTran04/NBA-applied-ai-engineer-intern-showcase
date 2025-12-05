from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlalchemy as sa
from backend.config import DB_DSN, EMBED_MODEL, LLM_MODEL
from backend.utils import ollama_embed, ollama_generate
from sqlalchemy import text

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4200"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
eng = sa.create_engine(DB_DSN)


class Q(BaseModel):
    question: str


@app.post("/api/chat")
def answer(q: Q):
    print('Received question')
    qvec = ollama_embed(EMBED_MODEL, q.question)

    # Add current date context for temporal awareness
    from datetime import datetime
    import re as regex_module
    current_date = datetime.now()
    current_calendar_year = current_date.year

    # NBA seasons span two calendar years (e.g., 2024-25 season)
    # "This year" = current season = 2025-26 season = games in 2025
    # "Last year" = previous season = 2024-25 season = games in 2024
    current_season_year = current_calendar_year  # 2025
    last_season_year = current_calendar_year - 1  # 2024

    # Detect temporal references in the question
    q_lower_temporal = q.question.lower()
    year_filter = None
    date_filter = None
    championship_query = False
    average_query = False
    most_recent_game = False

    # Check for championship/finals queries
    if any(keyword in q_lower_temporal for keyword in ['championship', 'champion', 'finals', 'won the championship', 'nba finals']):
        championship_query = True
        print("Detected championship query")

    # Check for average queries
    if any(keyword in q_lower_temporal for keyword in ['average', 'avg', 'per game', 'ppg', 'rpg', 'apg', 'averages']):
        average_query = True
        print("Detected average query")

    # Check for "last game" or "most recent game" queries
    if any(keyword in q_lower_temporal for keyword in ['last game', 'most recent game', 'latest game', 'most recent', 'last match']):
        most_recent_game = True
        print("Detected most recent game query")

    if 'last year' in q_lower_temporal:
        year_filter = last_season_year  # 2024 for 2024-25 season
        print(f"Detected 'last year' (season {year_filter}-{year_filter+1}): filtering to {year_filter}")
    elif 'this year' in q_lower_temporal:
        year_filter = current_season_year  # 2025 for 2025-26 season
        print(f"Detected 'this year' (season {year_filter}-{year_filter+1}): filtering to {year_filter}")
    elif 'christmas' in q_lower_temporal:
        # For Christmas questions, look for games on 12-25
        date_filter = '12-25'  # Month-day format
        print(f"Detected Christmas date filter: {date_filter}")

    # Check for any 4-digit year in the question (2020-2029)
    if not year_filter:
        import re
        year_match = re.search(r'\b(202[0-9])\b', q.question)
        if year_match:
            year_filter = int(year_match.group(1))
            print(f"Detected year filter: {year_filter}")

    # Check if question mentions a specific player
    player_filter = None
    with eng.begin() as cx:
        # Common NBA nicknames and abbreviations
        nickname_map = {
            'sga': 'Shai Gilgeous-Alexander',
            'wemby': 'Victor Wembanyama',
            'wembanyama': 'Victor Wembanyama',
            'luka': 'Luka Dončić',
            'doncic': 'Luka Dončić',
            'lebron': 'LeBron James',
            'giannis': 'Giannis Antetokounmpo',
            'jokic': 'Nikola Jokić',
            'embiid': 'Joel Embiid',
            'steph': 'Stephen Curry',
            'curry': 'Stephen Curry',
            'kd': 'Kevin Durant',
            'durant': 'Kevin Durant',
            'ad': 'Anthony Davis',
            'dame': 'Damian Lillard',
            'lillard': 'Damian Lillard',
            'kawhi': 'Kawhi Leonard',
            'pg': 'Paul George',
            'cp3': 'Chris Paul',
            'book': 'Devin Booker',
            'booker': 'Devin Booker',
            'tatum': 'Jayson Tatum',
            'ant': 'Anthony Edwards',
            'ja': 'Ja Morant',
            'morant': 'Ja Morant',
            'harden': 'James Harden',
            'kyrie': 'Kyrie Irving',
            'irving': 'Kyrie Irving',
        }

        # Try to find player names mentioned in the question
        all_players = cx.execute(
            text("SELECT player_id, first_name, last_name FROM players")
        ).mappings().all()

        q_lower = q.question.lower()

        # First check for nicknames (with word boundaries)
        import re
        for nickname, full_name in nickname_map.items():
            # Use word boundaries to avoid false matches
            if re.search(r'\b' + re.escape(nickname) + r'\b', q_lower):
                # Find the player by their full name
                for player in all_players:
                    player_full = f"{player['first_name']} {player['last_name']}"
                    if full_name.lower() == player_full.lower():
                        player_filter = player['player_id']
                        print(f"Detected player via nickname '{nickname}': {player['first_name']} {player['last_name']} (ID: {player_filter})")
                        break
                if player_filter:
                    break

        # If no nickname match, check for regular names (with word boundaries)
        if not player_filter:
            for player in all_players:
                full_name = f"{player['first_name']} {player['last_name']}".lower()
                last_name = player['last_name'].lower()
                first_name = player['first_name'].lower()

                # Use word boundaries to avoid matching substrings
                # Only match first name if it's at least 4 characters (avoid common words like "ja", "chris")
                if (re.search(r'\b' + re.escape(full_name) + r'\b', q_lower) or
                    re.search(r'\b' + re.escape(last_name) + r'\b', q_lower) or
                    (len(first_name) >= 4 and re.search(r'\b' + re.escape(first_name) + r'\b', q_lower))):
                    player_filter = player['player_id']
                    print(f"Detected player: {player['first_name']} {player['last_name']} (ID: {player_filter})")
                    break

    # Retrieve relevant games
    with eng.begin() as cx:
        # Special handling for championship queries
        if championship_query:
            # We only have regular season data, not playoff/finals data
            # Find the team with the best regular season record
            year_clause = "WHERE EXTRACT(YEAR FROM g.game_timestamp::timestamp) = :year" if year_filter else ""
            params = {"k": 5}
            if year_filter:
                params["year"] = year_filter

            # Get team with best record (most wins)
            best_team = cx.execute(
                text(
                    "SELECT t.team_id, t.city || ' ' || t.name as team_name, COUNT(*) as wins "
                    "FROM game_details g "
                    "JOIN teams t ON g.winning_team_id = t.team_id "
                    f"{year_clause} "
                    "GROUP BY t.team_id, t.city, t.name "
                    "ORDER BY wins DESC LIMIT 1"
                ),
                params,
            ).mappings().first()

            if best_team:
                print(f"Best regular season record: {best_team['team_name']} with {best_team['wins']} wins")

                # Create info for context - being honest about data limitations
                champion_row = {
                    'team_id': best_team['team_id'],
                    'team_name': best_team['team_name'],
                    'wins': best_team['wins'],
                    'regular_season_only': True  # Flag to indicate we only have regular season data
                }

                # Get some of their games as evidence
                game_rows = cx.execute(
                    text(
                        "SELECT g.game_id, g.game_timestamp, "
                        "g.home_team_id, ht.city || ' ' || ht.name as home_team, "
                        "g.away_team_id, at.city || ' ' || at.name as away_team, "
                        "g.home_points, g.away_points, "
                        "CASE WHEN g.home_points > g.away_points THEN ht.city || ' ' || ht.name "
                        "     ELSE at.city || ' ' || at.name END as winner "
                        "FROM game_details g "
                        "JOIN teams ht ON g.home_team_id = ht.team_id "
                        "JOIN teams at ON g.away_team_id = at.team_id "
                        f"{year_clause} AND g.winning_team_id = :team_id "
                        "ORDER BY g.game_timestamp DESC "
                        "LIMIT :k"
                    ),
                    {**params, "team_id": best_team['team_id']},
                ).mappings().all()
            else:
                champion_row = None
                game_rows = []
        # If a player is detected, get games they played in instead of vector search
        elif player_filter:
            year_clause = "AND EXTRACT(YEAR FROM g.game_timestamp::timestamp) = :year" if year_filter else ""
            # If asking for most recent/last game, show only 1-3 games in DESC order
            if most_recent_game:
                params = {"player_id": player_filter, "k": 3}
                order_direction = "DESC"
            else:
                params = {"player_id": player_filter, "k": 10}
                order_direction = "ASC"

            if year_filter:
                params["year"] = year_filter

            game_rows = cx.execute(
                text(
                    "SELECT DISTINCT g.game_id, g.game_timestamp, "
                    "g.home_team_id, ht.city || ' ' || ht.name as home_team, "
                    "g.away_team_id, at.city || ' ' || at.name as away_team, "
                    "g.home_points, g.away_points, "
                    "CASE WHEN g.home_points > g.away_points THEN ht.city || ' ' || ht.name "
                    "     ELSE at.city || ' ' || at.name END as winner "
                    "FROM game_details g "
                    "JOIN teams ht ON g.home_team_id = ht.team_id "
                    "JOIN teams at ON g.away_team_id = at.team_id "
                    "JOIN player_box_scores p ON g.game_id = p.game_id "
                    f"WHERE p.person_id = :player_id {year_clause} "
                    f"ORDER BY g.game_timestamp {order_direction} "
                    "LIMIT :k"
                ),
                params,
            ).mappings().all()
        else:
            # For non-player queries, add year or date filter if detected
            where_clauses = []
            if year_filter:
                where_clauses.append("EXTRACT(YEAR FROM g.game_timestamp::timestamp) = :year")
            if date_filter:
                where_clauses.append("TO_CHAR(g.game_timestamp::timestamp, 'MM-DD') = :date")

            where_clause = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            # If asking for most recent game, use timestamp ordering instead of vector similarity
            if most_recent_game:
                params = {"k": 3}
                if year_filter:
                    params["year"] = year_filter
                if date_filter:
                    params["date"] = date_filter

                game_rows = cx.execute(
                    text(
                        "SELECT g.game_id, g.game_timestamp, "
                        "g.home_team_id, ht.city || ' ' || ht.name as home_team, "
                        "g.away_team_id, at.city || ' ' || at.name as away_team, "
                        "g.home_points, g.away_points, "
                        "CASE WHEN g.home_points > g.away_points THEN ht.city || ' ' || ht.name "
                        "     ELSE at.city || ' ' || at.name END as winner "
                        "FROM game_details g "
                        "JOIN teams ht ON g.home_team_id = ht.team_id "
                        "JOIN teams at ON g.away_team_id = at.team_id "
                        f"{where_clause} "
                        "ORDER BY g.game_timestamp DESC LIMIT :k"
                    ),
                    params,
                ).mappings().all()
            else:
                params = {"q": str(qvec), "k": 5}
                if year_filter:
                    params["year"] = year_filter
                if date_filter:
                    params["date"] = date_filter

                game_rows = cx.execute(
                    text(
                        "SELECT g.game_id, g.game_timestamp, "
                        "g.home_team_id, ht.city || ' ' || ht.name as home_team, "
                        "g.away_team_id, at.city || ' ' || at.name as away_team, "
                        "g.home_points, g.away_points, "
                        "CASE WHEN g.home_points > g.away_points THEN ht.city || ' ' || ht.name "
                        "     ELSE at.city || ' ' || at.name END as winner "
                        "FROM game_details g "
                        "JOIN teams ht ON g.home_team_id = ht.team_id "
                        "JOIN teams at ON g.away_team_id = at.team_id "
                        f"{where_clause} "
                        "ORDER BY g.embedding <-> CAST(:q AS vector) LIMIT :k"
                    ),
                    params,
                ).mappings().all()

        # Also retrieve player stats from those games
        game_ids = [r["game_id"] for r in game_rows]
        player_rows = []
        season_averages = None

        if game_ids:
            # If a specific player is detected, get their stats from those games
            if player_filter:
                # If this is an average query, calculate season averages
                if average_query:
                    year_clause_avg = "AND EXTRACT(YEAR FROM g.game_timestamp::timestamp) = :year" if year_filter else ""
                    avg_params = {"player_id": player_filter}
                    if year_filter:
                        avg_params["year"] = year_filter

                    season_averages = cx.execute(
                        text(
                            "SELECT "
                            "COUNT(*) as games_played, "
                            "ROUND(AVG(p.points)::numeric, 1) as avg_points, "
                            "ROUND(AVG(p.offensive_reb + p.defensive_reb)::numeric, 1) as avg_rebounds, "
                            "ROUND(AVG(p.assists)::numeric, 1) as avg_assists, "
                            "(pl.first_name || ' ' || pl.last_name) as player_name "
                            "FROM player_box_scores p "
                            "JOIN players pl ON p.person_id = pl.player_id "
                            "JOIN game_details g ON p.game_id = g.game_id "
                            f"WHERE p.person_id = :player_id {year_clause_avg} "
                            "GROUP BY pl.first_name, pl.last_name"
                        ),
                        avg_params,
                    ).mappings().first()

                    if season_averages:
                        print(f"Season averages: {season_averages['player_name']} - {season_averages['avg_points']} PPG, {season_averages['avg_rebounds']} RPG, {season_averages['avg_assists']} APG over {season_averages['games_played']} games")

                # Order player stats to match game ordering (DESC for most recent, ASC otherwise)
                player_order = "DESC" if most_recent_game else "ASC"
                player_rows = cx.execute(
                    text(
                        "SELECT p.game_id, p.person_id as player_id, "
                        "(pl.first_name || ' ' || pl.last_name) as player_name, "
                        "p.points, (p.offensive_reb + p.defensive_reb) as rebounds, "
                        "p.assists, p.team_id, "
                        "t.city || ' ' || t.name as team_name, "
                        "g.game_timestamp, "
                        "CASE WHEN g.home_team_id = p.team_id THEN at.city || ' ' || at.name "
                        "     ELSE ht.city || ' ' || ht.name END as opponent_team "
                        "FROM player_box_scores p "
                        "JOIN players pl ON p.person_id = pl.player_id "
                        "JOIN teams t ON p.team_id = t.team_id "
                        "JOIN game_details g ON p.game_id = g.game_id "
                        "JOIN teams ht ON g.home_team_id = ht.team_id "
                        "JOIN teams at ON g.away_team_id = at.team_id "
                        "WHERE p.person_id = :player_id AND p.game_id = ANY(:game_ids) "
                        f"ORDER BY g.game_timestamp {player_order} "
                        "LIMIT 10"
                    ),
                    {"player_id": player_filter, "game_ids": game_ids},
                ).mappings().all()
            else:
                # No specific player - get top performers from the games
                player_rows = cx.execute(
                    text(
                        "SELECT p.game_id, p.person_id as player_id, "
                        "(pl.first_name || ' ' || pl.last_name) as player_name, "
                        "p.points, (p.offensive_reb + p.defensive_reb) as rebounds, "
                        "p.assists, p.team_id, "
                        "t.city || ' ' || t.name as team_name, "
                        "g.game_timestamp, "
                        "CASE WHEN g.home_team_id = p.team_id THEN at.city || ' ' || at.name "
                        "     ELSE ht.city || ' ' || ht.name END as opponent_team "
                        "FROM player_box_scores p "
                        "JOIN players pl ON p.person_id = pl.player_id "
                        "JOIN teams t ON p.team_id = t.team_id "
                        "JOIN game_details g ON p.game_id = g.game_id "
                        "JOIN teams ht ON g.home_team_id = ht.team_id "
                        "JOIN teams at ON g.away_team_id = at.team_id "
                        "WHERE p.game_id = ANY(:game_ids) "
                        "ORDER BY p.points DESC, rebounds DESC, p.assists DESC "
                        "LIMIT 10"
                    ),
                    {"game_ids": game_ids},
                ).mappings().all()

    # Build context with both game and player data
    ctx_parts = []

    # Add season averages if this is an average query for a player
    if average_query and season_averages:
        ctx_parts.append(f"=== SEASON AVERAGES ===")
        season_label = f"{year_filter}-{(year_filter+1) % 100:02d} season" if year_filter else "the season"
        ctx_parts.append(f"{season_averages['player_name']} averaged {season_averages['avg_points']} points, {season_averages['avg_rebounds']} rebounds, and {season_averages['avg_assists']} assists per game over {season_averages['games_played']} games in {season_label}.")
        ctx_parts.append("")

    # Add championship info if this is a championship query
    if championship_query and 'champion_row' in locals() and champion_row:
        ctx_parts.append(f"=== IMPORTANT: DATA LIMITATION ===")
        ctx_parts.append("The database only contains REGULAR SEASON games. Playoff and NBA Finals data is NOT available.")
        season_label = f"{year_filter}-{(year_filter+1) % 100:02d}" if year_filter else "the season"
        ctx_parts.append(f"Based on regular season data: The {champion_row['team_name']} had the best record in {season_label} with {champion_row['wins']} wins.")
        ctx_parts.append("Note: This does NOT indicate who won the NBA Championship/Finals, as playoff data is not included.")
        ctx_parts.append("")

    ctx_parts.append("=== GAMES ===")
    for r in game_rows:
        ctx_parts.append(
            f"{r['home_team']} {r['home_points']} vs {r['away_team']} {r['away_points']} "
            f"on {r['game_timestamp'][:10]}. Winner: {r['winner']}"
        )

    # Only include player stats if a player was detected in the question
    if player_filter and player_rows:
        ctx_parts.append("\n=== PLAYER STATS ===")
        for p in player_rows:
            ctx_parts.append(
                f"{p['player_name']} ({p['team_name']}) vs {p['opponent_team']} on {p['game_timestamp'][:10]}: "
                f"{p['points']} pts, {p['rebounds']} reb, {p['assists']} ast"
            )

    ctx = "\n".join(ctx_parts)

    # Add helpful instruction with temporal context
    temporal_context = f"Today's date: {current_date.strftime('%Y-%m-%d')}. Current NBA season: {current_season_year}-{current_season_year+1} (games in {current_season_year}). Last season: {last_season_year}-{last_season_year+1} (games in {last_season_year})."

    # Add specific filtering info if applied
    filter_info = ""
    if date_filter:
        filter_info = " (filtered to show only games on December 25)"
    elif year_filter:
        season_label = f"{year_filter}-{(year_filter+1) % 100:02d}"  # e.g., "2024-25"
        filter_info = f" (filtered to show only games from {season_label} season, calendar year {year_filter})"

    # Update chronological ordering info based on query type
    if most_recent_game:
        chronological_info = "Games and stats are listed in REVERSE chronological order (MOST RECENT first)."
        if player_filter:
            chronological_info += " The FIRST game/stats listed is the player's MOST RECENT game."
    else:
        chronological_info = "Games and stats are listed chronologically (earliest first)."
        if player_filter:
            chronological_info += " The first game/stats listed is the player's earliest game in the data."

    instruction = f"{temporal_context}\n\nAnswer based only on the context above{filter_info}. {chronological_info}"

    # Add date interpretation help
    instruction += f"\n\nDate references: 'last year' = {last_season_year}-{(last_season_year+1) % 100:02d} season (games in {last_season_year}), 'this year' = {current_season_year}-{(current_season_year+1) % 100:02d} season (games in {current_season_year}), 'Christmas' = December 25."

    resp = ollama_generate(LLM_MODEL, f"{instruction}\n\nContext:\n{ctx}\n\nQ:{q.question}\nA:")

    # Combine evidence from both games and players with detailed info
    evidence = []

    # For player questions, prioritize player stats evidence
    if player_filter and player_rows:
        # Show only the first 3 most relevant player stat lines
        for p in player_rows[:3]:
            evidence.append({
                "table": "player_box_scores",
                "id": int(p["player_id"]),
                "details": f"{p['player_name']} vs {p['opponent_team']}: {p['points']} pts, {p['rebounds']} reb, {p['assists']} ast",
                "date": p['game_timestamp'][:10] if 'game_timestamp' in p else None
            })
        # Add just the first game for context
        if game_rows:
            evidence.append({
                "table": "game_details",
                "id": int(game_rows[0]["game_id"]),
                "details": f"{game_rows[0]['home_team']} {game_rows[0]['home_points']} vs {game_rows[0]['away_team']} {game_rows[0]['away_points']}",
                "date": game_rows[0]['game_timestamp'][:10]
            })
    else:
        # For game questions, only show games (no player stats unless player was detected)
        for r in game_rows[:3]:  # Only top 3 games
            evidence.append({
                "table": "game_details",
                "id": int(r["game_id"]),
                "details": f"{r['home_team']} {r['home_points']} vs {r['away_team']} {r['away_points']}",
                "date": r['game_timestamp'][:10]
            })

    return {
            "answer": resp,
            "evidence": evidence,
        }