# Technical Round Assignment - Data Pipeline & Dashboard

## Project Structure

- `clean_data.py` - cleans `customers.csv` and `orders.csv` from `data/raw` into `data/processed`.
- `analyze.py` - merges cleaned data with `products.csv` and produces analysis CSVs in `data/processed`.
- `backend/` - FastAPI app exposing analysis outputs as REST endpoints.
- `frontend/` - Single-page HTML dashboard that consumes the backend API.
- `data/raw/` - Input CSVs (`customers.csv`, `orders.csv`, `products.csv`).
- `data/processed/` - All generated cleaned and analysis CSVs.
- `tests/` - Pytest unit tests for data-cleaning functions.

## Prerequisites

- Python 3.9+ (tested with Python 3.11).

## Setup

From the project root:

```bash
python -m venv .venv
.venv\Scripts\activate  # on Windows
pip install -r backend/requirements.txt
pip install pytest
```

The repo already includes small sample CSVs in `data/raw` to exercise the pipeline.

## Running the Data Pipeline

From the project root:

```bash
python clean_data.py
python analyze.py
```

Outputs are written to `data/processed`:

- `customers_clean.csv`
- `orders_clean.csv`
- `monthly_revenue.csv`
- `top_customers.csv`
- `category_performance.csv`
- `regional_analysis.csv`

## Running the Backend API

From the project root:

```bash
uvicorn backend.main:app --reload
```

Key endpoints:

- `GET /health` - `{ "status": "ok" }`
- `GET /api/revenue` - monthly revenue (from `monthly_revenue.csv`)
- `GET /api/top-customers` - top customers with churn flag
- `GET /api/categories` - category performance
- `GET /api/regions` - regional analysis

## Running the Frontend Dashboard

Open `frontend/index.html` in a browser (or serve it with any static file server).

The dashboard includes:

- **Revenue Trend Chart** with a **date-range filter** (month range).
- **Top Customers Table** with a **search box** and sortable columns.
- **Category Breakdown** bar chart.
- **Region Summary** table of KPIs.

Ensure the backend is running at `http://localhost:8000` (default `uvicorn` host/port).

## Running Tests

```bash
pytest
```

This runs three unit tests in `tests/test_clean_data.py` that cover:

- Customer deduplication and basic normalization.
- Region defaulting and email validity flag.
- Order cleaning (date parsing, status normalization, and derived `order_year_month`).

