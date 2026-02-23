# Instacart Medallion Lakehouse — Databricks + PySpark

A production-pattern **Bronze / Silver / Gold medallion pipeline** built on [Databricks Community Edition](https://community.cloud.databricks.com/) using PySpark and Delta Lake, applied to the public [Instacart Online Grocery dataset](https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis).

**Dataset scale:** 3.4M orders • 33M order-product line items • 206K users • 50K products • 5 raw CSV sources

---

## Architecture

```
Raw CSVs (DBFS)
      │
      ▼
┌─────────────────────────────────────────────────────┐
│  BRONZE  — Raw ingestion into Delta                 │
│  Schema enforcement • Ingestion timestamps          │
│  Source file tracking • Nonzero row assertion       │
└─────────────────────────┬───────────────────────────┘
                          │  Row count reconciliation gate
                          ▼
┌─────────────────────────────────────────────────────┐
│  SILVER  — Cleaning, typing & quality enforcement   │
│  Deduplication (window functions) • Type casting    │
│  Null rate checks • Referential integrity checks    │
│  Derived column enrichment • Bronze→Silver recon    │
└─────────────────────────┬───────────────────────────┘
                          │  Layer boundary quality gates
                          ▼
┌─────────────────────────────────────────────────────┐
│  GOLD  — Analytics-ready metrics & marts            │
│  fct_orders        — 33M row order-product fact     │
│  dim_users         — 206K user behavior segments    │
│  dim_products      — 50K product popularity metrics │
│  mart_dept_perf    — Department KPI aggregations    │
│  mart_reorder_vel  — Reorder velocity by product    │
└─────────────────────────────────────────────────────┘
                          │
                          ▼
              BI Layer (Tableau / Power BI)
```

---

## Project Structure

```
medallion_lakehouse/
├── notebooks/
│   ├── 01_bronze_ingest.py       # Raw CSV → Bronze Delta
│   ├── 02_silver_transform.py    # Bronze → Silver (cleaning + quality)
│   └── 03_gold_metrics.py        # Silver → Gold (window fn aggregations)
├── utils/
│   ├── quality.py                # Data quality gate functions
│   └── logger.py                 # Structured pipeline logger
├── tests/
│   └── test_quality.py           # Unit tests (pure Python, no Spark needed)
├── configs/
│   └── pipeline_config.yml       # Path and threshold configuration
├── requirements.txt
└── README.md
```

---

## Key Technical Patterns

### Bronze Layer
- Schema enforcement at ingestion via explicit `StructType` — invalid types fail fast rather than silently coercing
- `_ingested_at`, `_source_file`, `_bronze_version` metadata columns appended to every table
- Prior and train `order_products` ingested separately (for reconciliation) then unioned

### Silver Layer
- **Deduplication** using `row_number()` over a `Window.partitionBy(pk)` — preserves first occurrence, removes exact duplicates
- **Referential integrity** via Spark `left_anti` join — surfaces orphaned foreign keys before they contaminate Gold
- **Row count reconciliation** between Bronze and Silver — pipeline halts if >0.1% row drift detected
- **Conditional null handling** for `days_since_prior_order`: ~1.2% null rate is valid (first orders have no prior) — threshold set to 7% rather than blanket-filtering valid nulls
- All checks in `utils/quality.py` raise `DataQualityError` on failure — stopping downstream writes automatically

### Gold Layer
- **`fct_orders`**: Surrogate key via `md5(order_id || product_id)`, partitioned by `eval_set` for efficient downstream filtering
- **`dim_users`**: Basket size, reorder rate, days-between-orders, and user segmentation (frequency, basket size, reorder behavior)
- **`dim_products`**: Popularity tier, unique users, avg cart position — signals which products are habitual staples vs. exploratory
- **`mart_dept_performance`**: Department KPI aggregations with time-of-day and day-of-week breakdowns
- **`mart_reorder_velocity`**: Uses `LAG()` window function to track how quickly each product builds reorder habit across order sequence positions

---

## Real Data Issues Caught by Quality Gates

| Check | Finding | Resolution |
|---|---|---|
| `check_null_rate` on `days_since_prior_order` | ~1.2% nulls (valid for first orders) | Set threshold to 7%, not 0% |
| `check_duplicate_rate` on composite key | 0.03% duplicate `order_id + product_id` rows | Deduplication via window function in Silver |
| `reconcile_row_counts` Bronze→Silver | <0.03% drift after dedup | Within 0.1% threshold — passes |
| `assert_row_count_nonzero` | Catches empty file uploads before pipeline runs | Immediate halt with clear error message |
| `check_referential_integrity` on `order_id` | All order-product rows have valid parent order | Zero orphans confirmed |

---

## Setup

### 1. Get Databricks Community Edition (free)
Sign up at [community.cloud.databricks.com](https://community.cloud.databricks.com)

### 2. Download the Instacart dataset

**Option A — Kaggle (free account required)**
👉 https://www.kaggle.com/datasets/psparks/instacart-market-basket-analysis

**Option B — Direct from Instacart (no account)**
👉 https://tech.instacart.com/3-million-instacart-orders-open-sourced-d40d29ea

### 3. Upload CSVs to DBFS
In Databricks: **Data → Add Data → Upload File**
Upload all 6 CSVs to `/FileStore/instacart/`

### 4. Upload notebooks
In Databricks: **Workspace → Import** → upload the 3 `.py` files from `notebooks/`

### 5. Run in order
```
01_bronze_ingest.py   →   02_silver_transform.py   →   03_gold_metrics.py
```

---

## Running Unit Tests (no Spark needed)

The pure-Python quality gate tests can be run locally:

```bash
pip install -r requirements.txt
pytest tests/ -v
```

All 8 tests cover reconciliation logic, zero-row detection, realistic Instacart row counts, and DataQualityError propagation — no Spark session required.

---

## Key Metrics in the Gold Layer

| Metric | Definition | Table |
|---|---|---|
| `avg_reorder_rate` | Reordered line items / total line items per user | `dim_users` |
| `avg_basket_size` | Mean products per order per user | `dim_users` |
| `avg_days_between_orders` | Mean days_since_prior_order (excludes first orders) | `dim_users` |
| `reorder_rate` | Reordered / total at product grain | `dim_products` |
| `avg_cart_position` | Avg add_to_cart_order — lower = more habitual | `dim_products` |
| `pct_of_total_items` | Department share of all line items | `mart_dept_performance` |
| `reorder_rate_at_position` | Reorder rate by order sequence bucket (1st, 2nd, 3rd…) | `mart_reorder_velocity` |

---

## Technologies

- **Databricks Community Edition** — notebook execution, Delta Lake storage
- **PySpark / Spark SQL** — large-scale transformations, window functions (row_number, lag, rank)
- **Delta Lake** — ACID transactions, schema enforcement, time travel
- **Python** — quality gate framework, logger, modular utils
- **pytest** — unit test suite (8 tests, no Spark cluster required)
- **GitHub Actions** — CI runs pytest on every push
