"""
ingestion/fetch_coins.py

Fetches top 20 coins from coingecko and saves them to a raw json file
to local storage partition by date.

Why partition by date?
- Makes backfilling easy - rerun a specific date without touching others
- Mirrors how real data lakes are structured in production

"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

logger = logging.getLogger(__name__)

COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"

PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 20,
    "page": 1,
    "sparkline": False,
    "price_change_percentage": "24h"
}

RAW_PATH = Path("data/raw")


def fetch_coins():
    logger.info("Fetching coin data from CoinGecko...")
    try:
        response = requests.get(COINGECKO_URL, params=PARAMS, timeout=30)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Fetched {len(data)} coins.")
        return data
    except requests.RequestException as e:
        logger.error(f"Error fetching data: {e}")
        return []


def save_raw(coins: list[dict], date_str: str) -> None:
    output_dir = RAW_PATH / f"date={date_str}"
    output_dir.mkdir(parents=True, exist_ok=True)

    payload = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "coingecko",
        "record_count": len(coins),
        "coins": coins
    }

    output_file = output_dir / "coins.json"
    with open(output_file, "w") as f:
        json.dump(payload, f, indent=2)

    logger.info(f"Saved {len(coins)} records to {output_file}")


def run():
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        coins = fetch_coins()
        save_raw(coins, today)
        logger.info("Ingestion completed successfully.")
    except requests.HTTPError as e:
        logger.error(f"API Error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    run()
