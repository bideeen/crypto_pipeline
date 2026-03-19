"""
run_pipeline.py

Master pipeline runner. Executes the full pipeline in order:
    1. Fetch raw data from CoinGecko
    2. Load raw data into DuckDB
    3. Run dbt transformations
    4. Run dbt tests

Run:
    python run_pipeline.py

In production this script would be triggered by Airflow.
Running it manually here simulates what Airflow does.
"""

import logging
import subprocess
import sys
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def run_step(step_name: str, cmd: list[str], cwd: str = ".") -> None:
    """
    Run a shell command and raise immediately on failure.
    In production you never swallow errors silently.
    """
    logger.info(f"Starting: {step_name}")
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=False,
        text=True
    )
    if result.returncode != 0:
        logger.error(f"FAILED: {step_name}")
        logger.error("Pipeline halted. Fix the error above before retrying.")
        sys.exit(1)

    logger.info(f"Completed: {step_name}")


def main():
    start = datetime.now(timezone.utc)
    logger.info("=" * 50)
    logger.info("Crypto pipeline started")
    logger.info(f"Run time (UTC): {start.isoformat()}")
    logger.info("=" * 50)

    run_step(
        "Fetch raw data from CoinGecko",
        [sys.executable, "ingestion/fetch_coins.py"]
    )

    run_step(
        "Load raw data into DuckDB",
        [sys.executable, "ingestion/load_raw.py"]
    )

    run_step(
        "Run dbt transformations",
        ["dbt", "run"],
        cwd="dbt_project"
    )

    run_step(
        "Run dbt data quality tests",
        ["dbt", "test"],
        cwd="dbt_project"
    )

    end = datetime.now(timezone.utc)
    duration = (end - start).total_seconds()

    logger.info("=" * 50)
    logger.info(f"Pipeline completed successfully in {duration:.1f}s")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()