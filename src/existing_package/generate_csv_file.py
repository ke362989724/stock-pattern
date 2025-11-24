import os
import psycopg
import pandas as pd
from pathlib import Path


CURRENT_FILE = Path(__file__).resolve()           # This file's full path
SRC_DIR = CURRENT_FILE.parent                     # .../stock_pattern/src
CSV_DIR = SRC_DIR / "csv"                         # .../stock_pattern/src/csv

# Create the csv folder if it doesn't exist
CSV_DIR.mkdir(parents=True, exist_ok=True)

print(f"Saving all CSV files to: {CSV_DIR}")


def create_csv_folder():
    ticker_list_query = """
        SELECT symbol
        FROM tickers
        WHERE is_actively_trading = TRUE
    """

    ticker_price_query = """
        SELECT 
            p.date   AS "Date",
            p.open   AS "Open",
            p.high   AS "High",
            p.low    AS "Low",
            p.close  AS "Close",
            p.volume AS "Volume"
        FROM prices AS p
        INNER JOIN tickers AS t ON p.ticker_id = t.id
        WHERE t.symbol = %(ticker)s
        AND p.date::date >= (CURRENT_DATE - INTERVAL '1 year')
        ORDER BY p.date ASC
    """

    conn_string = "postgresql://postgres:123456@localhost:5432/market_data"
    with psycopg.connect(conn_string) as conn:
        print("Fetching list of active tickers...")
        ticker_list_df = pd.read_sql(ticker_list_query, conn)

        print(f"Found {len(ticker_list_df)} active tickers. Starting export...")

        successful = 0
        failed = 0

        for idx, ticker in enumerate(ticker_list_df["symbol"], start=1):
            try:
                df = pd.read_sql(ticker_price_query, conn, params={"ticker": ticker})

                if df.empty:
                    print(f"  [{idx}/{len(ticker_list_df)}] {ticker}: No data in the last year")
                    continue

                csv_path = CSV_DIR / f"{ticker}.csv"
                df.to_csv(csv_path, index=False)
                print(f"  [{idx}/{len(ticker_list_df)}] Saved {ticker} → {csv_path.name} ({len(df)} rows)")

                successful += 1

            except Exception as e:
                print(f"  [Failed] {ticker}: {e}")
                failed += 1

        print("\nFinished!")
        print(f"   Successful: {successful}")
        print(f"   Failed:     {failed}")
        print(f"   CSV folder: {CSV_DIR}")
