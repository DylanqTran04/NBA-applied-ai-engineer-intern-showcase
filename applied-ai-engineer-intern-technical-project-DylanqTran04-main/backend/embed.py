import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from backend.config import DB_DSN, EMBED_MODEL
from backend.utils import ollama_embed

# Example of a row embedding for the game_details table
# TODO: Customize this
def row_text(r):
    ts = pd.to_datetime(r.game_timestamp, utc=True)
    date = ts.strftime('%Y-%m-%d')
    home = int(r.home_team_id)
    away = int(r.away_team_id)
    hp = int(r.home_points)
    ap = int(r.away_points)

    return (
        "game | "
        f"season:{int(r.season)} | "
        f"date:{date} | "
        f"home_team_id:{home} | "
        f"away_team_id:{away} | "
        f"home_points:{hp} | "
        f"away_points:{ap} | "
    )


def main():
    print("Starting Embedding Process")
    eng = sa.create_engine(DB_DSN)
    with eng.begin() as cx:
        cx.execute(text('ALTER DATABASE nba REFRESH COLLATION VERSION'))
        # TODO: Try with different embeddings, feel free to try different index type/distance functions as well
        cx.execute(text("ALTER TABLE IF EXISTS game_details ADD COLUMN IF NOT EXISTS embedding vector(768);"))
        cx.execute(text("CREATE INDEX IF NOT EXISTS idx_game_details_embedding ON game_details USING hnsw (embedding vector_cosine_ops);"))
        df = pd.read_sql(
            "SELECT game_id, season, game_timestamp, home_team_id, away_team_id, home_points, away_points FROM game_details ORDER BY game_timestamp DESC, game_id DESC",
            cx,
        )
        for _, r in df.iterrows():
            vec = ollama_embed(EMBED_MODEL, row_text(r))
            cx.execute(text("UPDATE game_details SET embedding = :v WHERE game_id = :gid"), {"v": vec, "gid": int(r.game_id)})
    print(f"Finished Embeddings: {len(df)} Rows Updated")


if __name__ == "__main__":
    main()
