import argparse
import logging
from pathlib import Path
from typing import Tuple

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def load_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        logger.error(f"File not found: {path}")
        raise
    except EmptyDataError:
        logger.error(f"File is empty: {path}")
        raise


def clean_customers(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    report: dict = {"rows_before": len(df)}

    for col in ["name", "region", "email"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    def parse_signup(val):
        try:
            return pd.to_datetime(val, errors="coerce")
        except Exception:
            return pd.NaT

    if "signup_date" in df.columns:
        df["signup_date_parsed"] = df["signup_date"].apply(parse_signup)
        unparseable = df["signup_date_parsed"].isna() & df["signup_date"].notna()
        if unparseable.any():
            logger.warning(
                "Unparseable signup_date values for %d rows", unparseable.sum()
            )
        df["signup_date"] = df["signup_date_parsed"]
        df = df.drop(columns=["signup_date_parsed"])

    if "customer_id" in df.columns and "signup_date" in df.columns:
        df = df.sort_values("signup_date").drop_duplicates(
            subset="customer_id", keep="last"
        )

    if "email" in df.columns:
        df["email"] = df["email"].str.lower()

        def is_valid_email(val: str) -> bool:
            if not isinstance(val, str):
                return False
            if val in ("", "nan", "none"):
                return False
            return "@" in val and "." in val

        df["is_valid_email"] = df["email"].apply(is_valid_email)

    if "region" in df.columns:
        df["region"] = df["region"].replace({"": np.nan})
        df["region"] = df["region"].fillna("Unknown")

    report["rows_after"] = len(df)
    report["duplicates_removed"] = report["rows_before"] - report["rows_after"]
    report["nulls_after"] = df.isna().sum().to_dict()
    return df, report


def parse_order_date(val):
    if pd.isna(val):
        return pd.NaT
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y"):
        try:
            return pd.to_datetime(val, format=fmt)
        except Exception:
            continue
    return pd.NaT


def clean_orders(df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    report: dict = {"rows_before": len(df)}

    df = df.dropna(subset=["customer_id", "order_id"], how="all")

    if "order_date" in df.columns:
        df["order_date"] = df["order_date"].apply(parse_order_date)

    if "amount" in df.columns:
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
        if "product" in df.columns:
            medians = df.groupby("product")["amount"].transform("median")
            df["amount"] = df["amount"].fillna(medians)
        global_median = df["amount"].median()
        df["amount"] = df["amount"].fillna(global_median)

    if "status" in df.columns:
        df["status"] = df["status"].astype(str).str.strip().str.lower()
        mapping = {
            "done": "completed",
            "complete": "completed",
            "completed": "completed",
            "pending": "pending",
            "in progress": "pending",
            "canceled": "cancelled",
            "cancelled": "cancelled",
            "refunded": "refunded",
            "refund": "refunded",
        }
        df["status"] = df["status"].map(lambda s: mapping.get(s, s))

    if "order_date" in df.columns:
        df["order_year_month"] = df["order_date"].dt.strftime("%Y-%m")

    report["rows_after"] = len(df)
    report["duplicates_removed"] = 0
    report["nulls_after"] = df.isna().sum().to_dict()
    return df, report


def print_report(
    customers_report: dict, orders_report: dict, customers_nulls_before, orders_nulls_before
) -> None:
    print("=== Cleaning Report ===")
    print("\nCustomers:")
    print(f"Rows before: {customers_report['rows_before']}")
    print(f"Rows after: {customers_report['rows_after']}")
    print(f"Duplicate rows removed: {customers_report['duplicates_removed']}")
    print("Null counts before:")
    for col, val in customers_nulls_before.items():
        print(f"  {col}: {val}")
    print("Null counts after:")
    for col, val in customers_report["nulls_after"].items():
        print(f"  {col}: {val}")

    print("\nOrders:")
    print(f"Rows before: {orders_report['rows_before']}")
    print(f"Rows after: {orders_report['rows_after']}")
    print(f"Duplicate rows removed: {orders_report['duplicates_removed']}")
    print("Null counts before:")
    for col, val in orders_nulls_before.items():
        print(f"  {col}: {val}")
    print("Null counts after:")
    for col, val in orders_report["nulls_after"].items():
        print(f"  {col}: {val}")


def main():
    parser = argparse.ArgumentParser(description="Clean raw customer and order data.")
    parser.add_argument(
        "--raw-dir",
        type=str,
        default=None,
        help="Directory containing raw CSVs (customers.csv, orders.csv)",
    )
    parser.add_argument(
        "--processed-dir",
        type=str,
        default=None,
        help="Directory to write cleaned CSVs",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).parent
    raw_dir = Path(args.raw_dir) if args.raw_dir else base_dir / "data" / "raw"
    processed_dir = (
        Path(args.processed_dir) if args.processed_dir else base_dir / "data" / "processed"
    )
    processed_dir.mkdir(parents=True, exist_ok=True)

    customers_path = raw_dir / "customers.csv"
    orders_path = raw_dir / "orders.csv"

    customers_df = load_csv(customers_path)
    orders_df = load_csv(orders_path)

    customers_nulls_before = customers_df.isna().sum().to_dict()
    orders_nulls_before = orders_df.isna().sum().to_dict()

    customers_clean, customers_report = clean_customers(customers_df)
    orders_clean, orders_report = clean_orders(orders_df)

    customers_out = processed_dir / "customers_clean.csv"
    orders_out = processed_dir / "orders_clean.csv"
    customers_clean.to_csv(customers_out, index=False)
    orders_clean.to_csv(orders_out, index=False)

    print_report(customers_report, orders_report, customers_nulls_before, orders_nulls_before)


if __name__ == "__main__":
    main()

