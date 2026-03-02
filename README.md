# HBC Data Warehouse - Misa CRM ETL

This project implements an ETL pipeline to extract data from Misa CRM and load it into a DuckDB data warehouse following a Star Schema design.

## Project Structure

- `etl_misa.py`: Main Python script for extraction and loading (using Parquet for staging).
- `transforms.sql`: SQL transformations for Star Schema modeling (reads from Parquet).
- `.env`: Configuration for API credentials and database path.
- `data/staging/`: Directory containing raw data in Parquet format.
- `hbc_data_warehouse.duckdb`: The DuckDB database file.

## Setup Instructions

### 1. Prerequisites

- Python 3.12+
- Misa CRM API credentials (provided in `.env`).

### 2. Installation

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # Mac/Linux
# venv\Scripts\activate  # Windows

# Install dependencies
pip install requests duckdb pandas python-dotenv
```

### 3. Running the ETL

To run the ETL manually:

```bash
source venv/bin/activate
python etl_misa.py
```

### 4. Scheduling (2x per day)

You can use `crontab` to schedule the script.

```bash
# Open crontab
crontab -e

# Add these lines (runs at 8 AM and 8 PM)
0 8,20 * * * cd "/Users/tombui/Desktop/HBC Data Warehouse" && ./venv/bin/python etl_misa.py >> etl.log 2>&1
```

## Data Warehouse Design (Star Schema)

This project follows a professional Star Schema architecture optimized for Power BI.

### Fact Layer

- **`fact_sale_orders`**: Header-level sales data. Includes `total_amount`, `discount`, `vat`, and `status`.
- **`fact_sale_order_items`**: Granular line-item details extracted from nested JSON. Allows for SKU-level analysis.

### Dimension Layer

- **`dim_customers`**: Enriched with `account_type`, `province` (for mapping), and `tax_code`. Joined via numeric `customer_id`.
- **`dim_employees`**: Heuristically derived from order metadata to track sales rep performance.
- **`dim_products`**: Product catalog with `unit_price`, `category`, and `properties`.
- **`dim_contacts`**: Contact person details linked to accounts.
- **`dim_date`**: Calendar dimension (2020-2030) for time-intelligence calculations.

### Reporting views

- **`v_sales_analysis`**: Pre-joined flat table for high-level order dashboards.
- **`v_item_analysis`**: Flattened view of line items and product categories for granular reporting.

## Power BI Integration (via ODBC)

### 1. Install DuckDB ODBC Driver

1. Download the latest DuckDB ODBC driver for macOS: [DuckDB ODBC Releases](https://github.com/duckdb/duckdb-odbc/releases)
2. Install the driver on your machine.

### 2. Configure ODBC Data Source (DSN)

1. Open **ODBC Data Source Administrator** (or use `iODBC` on Mac).
2. Create a new **System DSN**.
3. Select **DuckDB Driver**.
4. Set **Database** path to: `/Users/tombui/Desktop/HBC Data Warehouse/hbc_data_warehouse.duckdb`.

### 3. Connect in Power BI

1. In Power BI Desktop, go to **Get Data** -> **ODBC**.
2. Select the DSN you created.
3. Import the tables/views (e.g., `v_sales_analysis`, `dim_date`).

- **Environment Variables**: Sensitive credentials stored in `.env`.

## How Incremental Loading Works

The ETL script uses a **"High Watermark"** strategy to ensure efficiency and data integrity:

1. **Tracking**: The script maintains an `etl_metadata` table in DuckDB to store the `last_modified` timestamp for each entity (customers, orders, etc.).
2. **Smart Fetching**: On every run, the script asks the Misa CRM API for records sorted by `modified_date` (newest first).
3. **Automatic Stop**: It stops downloading as soon as it hits a record that is older than or equal to the timestamp recorded in the previous run.
4. **No Data Gaps**: If a run fails, the "watermark" isn't updated, so the next run will automatically pick up where it left off.
5. **Safe Merging**: In the Data Warehouse layer, we use `INSERT OR REPLACE` logic. If a record was modified (like an order status changing), the old version is replaced by the latest version, while brand new records are simply added.

This means the script is safe to run as often as you like—it will always result in a clean, deduplicated, and up-to-date Data Warehouse.

## How to Inspect Data Locally

### 1. Using DuckDB CLI (Recommended for terminal)

If you have Homebrew installed, you can install the CLI:

```bash
brew install duckdb
```

Then, open your database file:

```bash
duckdb hbc_data_warehouse.duckdb
```

Inside the CLI, you can run SQL:

```sql
SHOW TABLES;
SELECT * FROM v_sales_analysis LIMIT 10;
.quit
```

### 2. Using DBeaver (Best GUI experience)

1. Download and install [DBeaver](https://dbeaver.io/).
2. Create a new connection and select **DuckDB**.
3. Point the **Path** to `/Users/tombui/Desktop/HBC Data Warehouse/hbc_data_warehouse.duckdb`.

### 3. Using VS Code Extension

Install the **DuckDB** extension (by `duckdb`) or **SQLTools** with the DuckDB driver. You can then query the database directly within VS Code.

### 4. Quick Python Check

If you just want a quick peek without installing anything else:

```bash
./venv/bin/python -c "import duckdb; conn = duckdb.connect('hbc_data_warehouse.duckdb'); print(conn.execute('SHOW TABLES').fetchall()); conn.close()"
```
