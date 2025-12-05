import os
import json
import re
import sqlalchemy as sa
from sqlalchemy import text
from backend.config import DB_DSN, EMBED_MODEL, LLM_MODEL
from backend.utils import ollama_embed, ollama_generate

BASE_DIR = os.path.dirname(__file__)
QUESTIONS_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "part1", "questions.json"))
ANSWERS_PATH = os.path.normpath(os.path.join(BASE_DIR, "..", "part1", "answers.json"))


def retrieve_games(cx, qvec, k=10):
    """Retrieve relevant games using vector similarity"""
    sql = (
        "SELECT g.game_id, g.game_timestamp, "
        "g.home_team_id, ht.city || ' ' || ht.name as home_team, "
        "g.away_team_id, at.city || ' ' || at.name as away_team, "
        "g.home_points, g.away_points, "
        "CASE WHEN g.home_points > g.away_points THEN ht.city || ' ' || ht.name "
        "     ELSE at.city || ' ' || at.name END as winner, "
        "1 - (g.embedding <=> (:q)::vector) AS score "
        "FROM game_details g "
        "JOIN teams ht ON g.home_team_id = ht.team_id "
        "JOIN teams at ON g.away_team_id = at.team_id "
        "ORDER BY g.embedding <-> (:q)::vector LIMIT :k"
    )
    return cx.execute(text(sql), {"q": qvec, "k": k}).mappings().all()


def retrieve_player_stats(cx, game_ids):
    """Retrieve player box scores for relevant games"""
    if not game_ids:
        return []

    sql = """
        SELECT p.game_id, p.person_id as player_id,
               (pl.first_name || ' ' || pl.last_name) as player_name,
               p.points,
               (p.offensive_reb + p.defensive_reb) as rebounds,
               p.assists, p.team_id,
               t.city || ' ' || t.name as team_name,
               g.game_timestamp,
               ht.city || ' ' || ht.name as home_team,
               at.city || ' ' || at.name as away_team,
               g.home_points, g.away_points
        FROM player_box_scores p
        JOIN players pl ON p.person_id = pl.player_id
        JOIN game_details g ON p.game_id = g.game_id
        JOIN teams t ON p.team_id = t.team_id
        JOIN teams ht ON g.home_team_id = ht.team_id
        JOIN teams at ON g.away_team_id = at.team_id
        WHERE p.game_id IN :game_ids
        ORDER BY p.points DESC, (p.offensive_reb + p.defensive_reb) DESC, p.assists DESC
        LIMIT 20
    """
    return cx.execute(text(sql), {"game_ids": tuple(game_ids)}).mappings().all()


def extract_json_from_text(text):
    """Extract JSON object from LLM response"""
    # Try to find JSON object in the response
    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
    if json_match:
        try:
            return json.loads(json_match.group())
        except:
            pass
    return None


def extract_number(text):
    """Extract first number from text"""
    match = re.search(r'\d+', str(text))
    return int(match.group()) if match else 0


def answer_game_question(question, game_rows, question_data):
    """Answer questions about games using simple extraction"""
    ctx_lines = []
    for r in game_rows[:5]:
        ctx_lines.append(
            f"Game ID {r['game_id']}: Date {r['game_timestamp']}, "
            f"{r['home_team']} ({r['home_points']}) vs {r['away_team']} ({r['away_points']}), "
            f"Winner: {r['winner']}"
        )
    context = "\n".join(ctx_lines)

    prompt = f"""Context (NBA game data):
{context}

Question: {question}

Instructions:
- Answer ONLY using the context above
- Provide specific numbers and team names
- Be concise and factual

Answer:"""

    response = ollama_generate(LLM_MODEL, prompt)

    # Parse response based on expected return format
    result = {}

    if "points" in question_data["return"] and "winner" not in question_data["return"]:
        # Looking for a single team's score
        points = extract_number(response)
        result["points"] = points
    elif "winner" in question_data["return"]:
        # Extract winner and score
        winner = ""
        score = ""

        # Look for team names in response
        for word in response.split():
            word_clean = word.strip('.,!?')
            if any(team in word_clean for team in ['Warriors', 'Kings', 'Thunder', 'Timberwolves',
                                                     'Nuggets', 'Denver', 'Golden', 'Lakers', 'Celtics',
                                                     'Mavericks', 'Hawks', 'Jazz', 'Rockets', 'Spurs']):
                if not winner:
                    winner = word_clean

        # Look for score pattern like "134-114" or "134 to 114"
        score_match = re.search(r'(\d{2,3})\s*[-to]+\s*(\d{2,3})', response)
        if score_match:
            score = f"{score_match.group(1)}-{score_match.group(2)}"

        result["winner"] = winner
        result["score"] = score

    return result


def answer_player_question(question, player_rows, question_data):
    """Answer questions about players using detailed stats"""
    if not player_rows:
        result = {}
        for key in question_data["return"]:
            if key == "evidence":
                continue
            result[key] = 0 if question_data["return"][key] == "int" else ""
        return result

    # Build context with top players
    ctx_lines = []
    for r in player_rows[:10]:
        ctx_lines.append(
            f"{r['player_name']} (Team {r['team_id']}): "
            f"Game {r['game_id']} on {r['game_timestamp']}, "
            f"{r['points']} pts, {r['rebounds']} reb, {r['assists']} ast"
        )
    context = "\n".join(ctx_lines)

    prompt = f"""Context (NBA player statistics):
{context}

Question: {question}

Instructions:
- Answer ONLY using the player stats above
- Use exact player names from the context
- Provide specific numbers
- If asking about triple-double, need at least 10 in pts/reb/ast

Answer:"""

    response = ollama_generate(LLM_MODEL, prompt)

    # Parse response
    result = {}

    # Extract player name - try to find from our context first
    player_name = ""
    for r in player_rows[:10]:
        if r['player_name'].lower() in response.lower():
            player_name = r['player_name']
            break

    if not player_name:
        # Try to extract from response
        for r in player_rows[:5]:
            result_player = r
            break
        player_name = result_player.get('player_name', '') if player_rows else ''

    result["player_name"] = player_name

    # Extract numbers
    numbers = re.findall(r'\b(\d+)\b', response)

    if "points" in question_data["return"]:
        if numbers:
            result["points"] = int(numbers[0])
        else:
            # Use top scorer from context
            result["points"] = player_rows[0]['points'] if player_rows else 0

    if "rebounds" in question_data["return"]:
        if len(numbers) > 1:
            result["rebounds"] = int(numbers[1])
        else:
            result["rebounds"] = player_rows[0]['rebounds'] if player_rows else 0

    if "assists" in question_data["return"]:
        if len(numbers) > 2:
            result["assists"] = int(numbers[2])
        else:
            result["assists"] = player_rows[0]['assists'] if player_rows else 0

    # If we got no good answer, use the top player from our query
    if not player_name and player_rows:
        top_player = player_rows[0]
        result["player_name"] = top_player['player_name']
        if "points" in result:
            result["points"] = top_player['points']
        if "rebounds" in result:
            result["rebounds"] = top_player['rebounds']
        if "assists" in result:
            result["assists"] = top_player['assists']

    return result


def main():
    print("Starting RAG Pipeline with Improved Extraction")
    eng = sa.create_engine(DB_DSN)

    with open(QUESTIONS_PATH, encoding="utf-8") as f:
        questions = json.load(f)

    answers = []

    with eng.begin() as cx:
        for q in questions:
            print(f"\nProcessing question {q['id']}: {q['question']}")

            # Embed the question
            qvec = ollama_embed(EMBED_MODEL, q["question"])

            # Retrieve relevant games (more games for better coverage)
            game_rows = retrieve_games(cx, qvec, k=10)
            game_ids = [r["game_id"] for r in game_rows]

            # Determine if we need player stats
            needs_player_data = "player_name" in q["return"]

            if needs_player_data:
                # Retrieve player stats
                player_rows = retrieve_player_stats(cx, game_ids)

                # Generate answer using player context
                result = answer_player_question(q["question"], player_rows, q)

                # Set evidence to top player IDs
                if player_rows:
                    evidence = [
                        {"table": "player_box_scores", "id": int(r["player_id"])}
                        for r in player_rows[:5]
                    ]
                else:
                    evidence = [{"table": "player_box_scores", "id": 0}]
            else:
                # Generate answer using game context
                result = answer_game_question(q["question"], game_rows, q)

                # Set evidence to top game IDs
                evidence = [
                    {"table": "game_details", "id": int(r["game_id"])}
                    for r in game_rows[:5]
                ]

            result["evidence"] = evidence

            answers.append({
                "id": q["id"],
                "result": result
            })

            print(f"  ✓ Result: {result}")

    # Write answers to file
    with open(ANSWERS_PATH, "w", encoding="utf-8") as f:
        json.dump(answers, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"Finished! Answers written to {ANSWERS_PATH}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
