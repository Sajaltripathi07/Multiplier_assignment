import argparse
from pathlib import Path

import numpy as np
import pandas as pd
from pandas.errors import EmptyDataError


def load_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except FileNotFoundError:
        raise
    except EmptyDataError:
        raise


def main():
    parser = argparse.ArgumentParser(description="Merge cleaned data and run analyses.")
    parser.add_argument(
        "--processed-dir",
        type=str,
        default=None,
        help="Directory containing customers_clean.csv and orders_clean.csv",
    )
    parser.add_argument(
        "--raw-dir",
        type=str,
        default=None,
        help="Directory containing products.csv",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).parent
    processed_dir = (
        Path(args.processed_dir) if args.processed_dir else base_dir / "data" / "processed"
    )
    raw_dir = Path(args.raw_dir) if args.raw_dir else base_dir / "data" / "raw"

    customers = load_csv(processed_dir / "customers_clean.csv")
    orders = load_csv(processed_dir / "orders_clean.csv")
    products = load_csv(raw_dir / "products.csv")

    orders_with_customers = orders.merge(
        customers, on="customer_id", how="left", suffixes=("_order", "_customer")
    )
    full_data = orders_with_customers.merge(
        products,
        left_on="product",
        right_on="product_name",
        how="left",
    )

    no_customer = orders_with_customers["name"].isna().sum()
    no_product = full_data["product_id"].isna().sum()
    print(f"Orders without matching customer: {no_customer}")
    print(f"Orders without matching product: {no_product}")

    processed_dir.mkdir(parents=True, exist_ok=True)

    completed = full_data[full_data["status"] == "completed"].copy()
    monthly_revenue = (
        completed.groupby("order_year_month")["amount"].sum().reset_index()
    )
    monthly_revenue.rename(columns={"amount": "total_revenue"}, inplace=True)
    monthly_revenue.to_csv(processed_dir / "monthly_revenue.csv", index=False)

    top_customers = (
        completed.groupby(["customer_id", "name", "region"])["amount"]
        .sum()
        .reset_index()
        .rename(columns={"amount": "total_spend"})
        .sort_values("total_spend", ascending=False)
        .head(10)
    )

    completed_dates = completed.copy()
    completed_dates["order_date"] = pd.to_datetime(
        completed_dates["order_date"], errors="coerce"
    )

    if not completed_dates["order_date"].dropna().empty:
        latest_date = completed_dates["order_date"].max()
        cutoff = latest_date - pd.Timedelta(days=90)
        completed_recent = completed_dates[completed_dates["order_date"] >= cutoff]
        active_ids = set(completed_recent["customer_id"].dropna().unique())
        top_customers["churned"] = ~top_customers["customer_id"].isin(active_ids)
    else:
        top_customers["churned"] = True

    top_customers.to_csv(processed_dir / "top_customers.csv", index=False)

    category_perf = (
        completed.groupby("category")
        .agg(
            total_revenue=("amount", "sum"),
            average_order_value=("amount", "mean"),
            number_of_orders=("order_id", "count"),
        )
        .reset_index()
    )
    category_perf.to_csv(processed_dir / "category_performance.csv", index=False)

    customers_by_region = (
        customers.groupby("region")["customer_id"].nunique().reset_index()
    )
    customers_by_region.rename(
        columns={"customer_id": "number_of_customers"}, inplace=True
    )

    regional_orders = (
        completed.groupby("region")
        .agg(
            number_of_orders=("order_id", "count"),
            total_revenue=("amount", "sum"),
        )
        .reset_index()
    )

    regional = customers_by_region.merge(regional_orders, on="region", how="left")
    regional["number_of_orders"] = regional["number_of_orders"].fillna(0).astype(int)
    regional["total_revenue"] = regional["total_revenue"].fillna(0.0)
    regional["avg_revenue_per_customer"] = regional["total_revenue"] / regional[
        "number_of_customers"
    ].replace({0: np.nan})

    regional.to_csv(processed_dir / "regional_analysis.csv", index=False)


if __name__ == "__main__":
    main()

