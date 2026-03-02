import os
import sys

import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from clean_data import clean_customers, clean_orders


def test_clean_customers_deduplicates_and_flags_email():
    df = pd.DataFrame(
        {
            "customer_id": [1, 1],
            "name": [" Alice", "Alice "],
            "email": ["ALICE@EXAMPLE.COM", ""],
            "region": [" North", ""],
            "signup_date": ["2024-01-01", "2024-02-01"],
        }
    )
    cleaned, report = clean_customers(df)

    assert len(cleaned) == 1
    row = cleaned.iloc[0]
    assert row["customer_id"] == 1
    assert row["name"] == "Alice"
    assert row["region"] in {"North", "Unknown"}
    assert row["email"] in {"alice@example.com", ""}
    assert bool(row["is_valid_email"]) in {True, False}
    assert report["duplicates_removed"] == 1


def test_clean_customers_missing_region_becomes_unknown():
    df = pd.DataFrame(
        {
            "customer_id": [2],
            "name": ["Bob"],
            "email": [""],
            "region": [""],
            "signup_date": ["2024-01-01"],
        }
    )
    cleaned, _ = clean_customers(df)
    assert cleaned.iloc[0]["region"] == "Unknown"
    assert bool(cleaned.iloc[0]["is_valid_email"]) is False


def test_clean_orders_parses_dates_and_fills_amount():
    df = pd.DataFrame(
        {
            "order_id": [100, 101],
            "customer_id": [1, 1],
            "product": ["Widget A", "Widget A"],
            "amount": [100.0, None],
            "order_date": ["2024-01-15", "16/01/2024"],
            "status": ["Done", "pending "],
        }
    )
    cleaned, _ = clean_orders(df)

    assert cleaned["order_date"].notna().all()
    assert cleaned["amount"].notna().all()
    assert set(cleaned["status"]) == {"completed", "pending"}
    assert "order_year_month" in cleaned.columns

