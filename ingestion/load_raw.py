"""
ingestion/load_raw.py

Loads raw JSON from data/raw/ into DuckDB as a raw table.

Why a separate script?
- Ingestion and loading are two different responsibilities.
  Fetching from an API is one job. Loading into a database is another.
  Mixing them makes both harder to test and debug.
"""
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

DB_PATH = Path("data/crypto.duckdb")
RAW_PATH = Path("data/raw")


def get_latest_raw_file() -> Path:
    """Find today's raw file."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    raw_file = RAW_PATH / f"date={today}" / "coins.json"

    if not raw_file.exists():
        raise FileNotFoundError(
            f"No raw file found for today ({today}). "
            f"Run fetch_coins.py first."
        )
    return raw_file


def load_to_duckdb(raw_file: Path) -> None:

    with open(raw_file) as f:
        payload = json.load(f)

    coins = payload["coins"]
    ingested_at = payload["ingested_at"]

    for coin in coins:
        coin["ingested_at"] = ingested_at

    # Write to a temp file so DuckDB can read it properly
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".json",
        delete=False
    ) as tmp:
        json.dump(coins, tmp)
        tmp_path = tmp.name

    try:
        conn = duckdb.connect(str(DB_PATH))
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("DROP TABLE IF EXISTS raw.coin_prices")

        conn.execute(f"""
            CREATE TABLE raw.coin_prices AS
            SELECT * FROM read_json_auto('{tmp_path}')
        """)

        count = conn.execute(
            "SELECT COUNT(*) FROM raw.coin_prices"
        ).fetchone()[0]
        logger.info(f"Loaded {count} records into raw.coin_prices")

        sample = conn.execute("""
            SELECT id, symbol, current_price, ingested_at
            FROM raw.coin_prices
            ORDER BY market_cap_rank
            LIMIT 3
        """).fetchall()

        logger.info("Sample rows:")
        for row in sample:
            logger.info(f"  {row}")

        conn.close()

    finally:
        # Always clean up the temp file, even if something crashes
        os.unlink(tmp_path)


def run():
    raw_file = get_latest_raw_file()
    logger.info(f"Loading {raw_file}...")
    load_to_duckdb(raw_file)
    logger.info("Load complete.")


if __name__ == "__main__":
    run()
