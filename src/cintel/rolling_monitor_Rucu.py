"""
rolling_monitor_RucuAvinash.py - Project script (example).

Author: Rucu Sethu
Date: 2026-03-31

Time-Series System Metrics Data

- Data is taken from a system that records operational metrics over time.
- Each row represents one observation for that month.
- The CSV file includes these columns:
  - Month: when the observation occurred
  - Monthly_Profit: Monthly profit of the store
  - Monthly Expenses: Monthly expenses incurred by the store
  - total_customer_wait_time_ms: total wait time in milliseconds

Purpose

- Read time-series system metrics from a CSV file.
- Demonstrate rolling monitoring using a moving window.
- Compute rolling averages to smooth short-term variation.
- Save the resulting monitoring signals as a CSV artifact.
- Log the pipeline process to assist with debugging and transparency.

Questions to Consider

- How does system behavior change over time?
- Why might a rolling average reveal patterns that individual observations hide?
- How can smoothing short-term variation help us understand longer-term trends?

Paths (relative to repo root)

    INPUT FILE: data/system_metrics_timeseries_case.csv
    OUTPUT FILE: artifacts/rolling_metrics_case.csv

Terminal command to run this file from the root project folder

    uv run python -m cintel.rolling_monitor_case

OBS:
  Don't edit this file - it should remain a working example.
  Use as much of this code as you can when creating your own pipeline script,
  and change the monitoring logic as needed for your project.
"""

# === DECLARE IMPORTS ===

import logging
from pathlib import Path
from typing import Final

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import polars as pl
from datafun_toolkit.logger import get_logger, log_header, log_path

# === CONFIGURE LOGGER ===

LOG: logging.Logger = get_logger("P5", level="DEBUG")

# === DEFINE GLOBAL PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

DATA_FILE: Final[Path] = DATA_DIR / "system_metrics_timeseries_Rucu.csv"
OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "rolling_metrics_Rucu.csv"

# === DEFINE THE MAIN FUNCTION ===


def main() -> None:
    """Run the pipeline.

    log_header() logs a standard run header.
    log_path() logs repo-relative paths (privacy-safe).
    """
    log_header(LOG, "CINTEL")

    LOG.info("========================")
    LOG.info("START main()")
    LOG.info("========================")

    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_FILE", DATA_FILE)
    log_path(LOG, "OUTPUT_FILE", OUTPUT_FILE)

    # Ensure artifacts directory exists
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path(LOG, "ARTIFACTS_DIR", ARTIFACTS_DIR)

    # ----------------------------------------------------
    # STEP 1: READ CSV DATA FILE INTO A POLARS DATAFRAME (TABLE)
    # ----------------------------------------------------
    df = pl.read_csv(DATA_FILE)

    LOG.info(f"Loaded {df.height} time-series records")

    # ----------------------------------------------------
    # STEP 2: SORT DATA BY TIME
    # ----------------------------------------------------
    # Time-series analysis requires observations to be ordered.
    df = df.sort("month")

    LOG.info("Sorted records by month")

    # ----------------------------------------------------
    # STEP 3: DEFINE ROLLING WINDOW RECIPES
    # ----------------------------------------------------
    # A rolling window computes statistics over the most recent
    # N observations. The window "moves" forward one row at a time.

    # Example: if WINDOW_SIZE = 3
    # row 1 → mean of rows [1]
    # row 2 → mean of rows [1,2]
    # row 3 → mean of rows [1,2,3]
    # row 4 → mean of rows [2,3,4]

    WINDOW_SIZE: int = 3
    # ----------------------------------------------------
    # STEP 4: AVERAGE MONTHLY PROFIT GROUPED BY BRANCH
    # ----------------------------------------------------
    df = pl.read_csv(DATA_FILE)
    avg_store_profit_recipe = df.group_by("branch_name").agg(
        pl.col("monthly_profit").mean().alias("avg_store_profit")
    )

    # ----------------------------------------------------
    # STEP 5: DEFINE ROLLING MEAN FOR # OF MONTHLY_PROFIT
    # ----------------------------------------------------
    # The `MONTHLY_PROFIT` column holds the profit for each month.
    monthly_profit_rolling_mean_recipe: pl.Expr = (
        pl.col("monthly_profit")
        .rolling_mean(WINDOW_SIZE)
        .alias("monthly_profit_rolling_mean")
    )

    # --------------------------------------------------------------------
    # STEP 6: Modification. Define ROLLING SD FOR # MONTHLY PROFIT
    # --------------------------------------------------------------------
    monthly_profit_rolling_SD_recipe: pl.Expr = (
        pl.col("monthly_profit")
        .rolling_std(WINDOW_SIZE)
        .alias("monthly_profit_rolling_SD")
    )
    # ------------------------------------------------------------------
    # STEP 7: DEFINE ROLLING MEAN FOR # OF MONTHLY EXPENSES
    # -----------------------------------------------------------------
    # The `monthly_expenses` column holds the  expenses for each month
    monthly_expenses_rolling_mean_recipe: pl.Expr = (
        pl.col("monthly_expenses")
        .rolling_mean(WINDOW_SIZE)
        .alias("monthly_expenses_rolling_mean")
    )
    # --------------------------------------------------------------------
    # STEP 8: MODIFICATION:DEFINE ROLLING SD FOR # OF MONTHLY EXPENSES
    # -------------------------------------------------------------------
    monthly_expenses_rolling_SD_recipe: pl.Expr = (
        pl.col("monthly_expenses")
        .rolling_std(WINDOW_SIZE)
        .alias("monthly_expenses_rolling_SD")
    )
    # STEP 9: DEFINE ROLLING MEAN FOR CUSTOMER_WAIT_TIME_SECONDS
    # ----------------------------------------------------
    # The `CUSTOMER_WAIT_TIME_SECONDS` column holds the total customer_wait time in milliseconds for each month.
    CUSTOMER_WAIT_TIME_SECONDS_rolling_mean_recipe: pl.Expr = (
        pl.col("customer_wait_time_seconds")
        .rolling_mean(WINDOW_SIZE)
        .alias("CUSTOMER_WAIT_TIME_SECONDS_rolling_mean")
    )
    # ---------------------------------------------------------------
    # STEP 10: MODIFICATION: DEFINE ROLLING SD # OF CUSTOMER_WAIT_TIME_SECONDS
    # --------------------------------------------------------------
    customer_wait_time_SD_recipe: pl.Expr = (
        pl.col("customer_wait_time_seconds")
        .rolling_std(WINDOW_SIZE)
        .alias("customer_wait_time_seconds_rolling_SD")
    )
    # STEP 11: APPLY THE ROLLING RECIPES IN A NEW DATAFRAME
    # ----------------------------------------------------
    # with_columns() evaluates the recipes and adds the new columns
    df_final = df.with_columns(
        [
            monthly_profit_rolling_mean_recipe,
            monthly_expenses_rolling_mean_recipe,
            CUSTOMER_WAIT_TIME_SECONDS_rolling_mean_recipe,
            monthly_profit_rolling_SD_recipe,
            monthly_expenses_rolling_SD_recipe,
            customer_wait_time_SD_recipe,
        ]
    )

    # -----------------------------------------------------
    # STEP 12: Add avg_store_profit to df
    # -----------------------------------------------------
    df_final = df_final.join(avg_store_profit_recipe, on="branch_name", how="left")

    LOG.info("Computed rolling mean signals")

    # ----------------------------------------------------
    # STEP 13: SAVE RESULTS AS AN ARTIFACT
    # ----------------------------------------------------
    df_final.write_csv(OUTPUT_FILE)
    LOG.info(f"Wrote rolling monitoring file: {OUTPUT_FILE}")

    LOG.info("========================")
    LOG.info("Pipeline executed successfully!")
    LOG.info("========================")
    LOG.info("END main()")
    # -----------------------------------------------------
    # STEP 14: Visualize the rolling SD in a bar chart
    # -----------------------------------------------------
    df = pd.read_csv(OUTPUT_FILE)
    # Keep only rows with SD values
    sd_cols = [
        "monthly_profit_rolling_SD",
        "monthly_expenses_rolling_SD",
        "customer_wait_time_seconds_rolling_SD",
    ]
    df_sd = df.dropna(subset=sd_cols)

    # Create an index for x-axis
    x = np.arange(len(df_sd))
    # Bar width
    width = 0.50
    plt.figure(figsize=(12, 6))
    # Plot grouped bars
    plt.bar(
        x - width / 2,
        df_sd["monthly_profit_rolling_SD"],
        width,
        label="Monthly Profit SD",
    )
    plt.bar(
        x + width / 2,
        df_sd["customer_wait_time_seconds_rolling_SD"],
        width,
        label=" Customer Wait time SD",
    )
    # Line Chart for monthly_expenses
    plt.plot(
        x,
        df_sd["monthly_expenses_rolling_SD"],
        marker="x",
        linewidth=3.5,
        color="#C33582",
        label="Expenses SD",
    )

    # Labels and Title
    plt.xlabel("Rolling WIndow Index")
    plt.ylabel("Standard Deviation")
    plt.title(
        "Rolling Standard Deviation for  Monthly Profit, Expense, Customer_wait_time"
    )

    plt.legend()
    plt.tight_layout()
    plt.show()


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
