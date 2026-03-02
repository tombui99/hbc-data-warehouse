-- 0. Staging Views (making Parquet files visible in DBeaver/DuckDB)
CREATE OR REPLACE VIEW stg_customers AS SELECT * FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_customers.parquet');
CREATE OR REPLACE VIEW stg_contacts AS SELECT * FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_contacts.parquet');
CREATE OR REPLACE VIEW stg_products AS SELECT * FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_products.parquet');
CREATE OR REPLACE VIEW stg_sale_orders AS SELECT * FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_sale_orders.parquet');
CREATE OR REPLACE VIEW stg_stocks AS SELECT * FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_stocks.parquet');

-- Clean up and create modeled tables

-- 1. Date Dimension
CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    day INTEGER,
    month INTEGER,
    year INTEGER,
    day_of_week INTEGER,
    iso_dw INTEGER,
    week_of_year INTEGER,
    quarter INTEGER
);

INSERT OR IGNORE INTO dim_date
SELECT
    datum AS date_key,
    day(datum) AS day,
    month(datum) AS month,
    year(datum) AS year,
    dayofweek(datum) AS day_of_week,
    isodow(datum) AS iso_dw,
    week(datum) AS week_of_year,
    quarter(datum) AS quarter
FROM (
    SELECT CAST(datum AS DATE) AS datum 
    FROM generate_series(DATE '2020-01-01', DATE '2030-12-31', INTERVAL 1 DAY) AS t(datum)
);

-- 2. Dimension Tables (SCD Type 1 - Upsert)

-- dim_customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id VARCHAR PRIMARY KEY,
    customer_code VARCHAR,
    customer_name VARCHAR,
    address VARCHAR,
    province VARCHAR,
    mobile VARCHAR,
    email VARCHAR,
    account_type VARCHAR,
    tax_code VARCHAR,
    owner_name VARCHAR,
    organization_unit VARCHAR,
    modified_date TIMESTAMP
);

-- Migration: Add missing columns if they don't exist
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS province VARCHAR;
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS account_type VARCHAR;
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS tax_code VARCHAR;
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS owner_name VARCHAR;
ALTER TABLE dim_customers ADD COLUMN IF NOT EXISTS organization_unit VARCHAR;

INSERT OR REPLACE INTO dim_customers (
    customer_id, customer_code, customer_name, address, province, 
    mobile, email, account_type, tax_code, owner_name, 
    organization_unit, modified_date
)
SELECT DISTINCT
    CAST(json_extract(data, '$.id') AS VARCHAR) AS customer_id,
    CAST(json_extract(data, '$.account_number') AS VARCHAR) AS customer_code,
    CAST(json_extract(data, '$.account_name') AS VARCHAR) AS customer_name,
    CAST(json_extract(data, '$.billing_address') AS VARCHAR) AS address,
    CAST(json_extract(data, '$.billing_province') AS VARCHAR) AS province,
    CAST(json_extract(data, '$.office_tel') AS VARCHAR) AS mobile,
    CAST(json_extract(data, '$.office_email') AS VARCHAR) AS email,
    CAST(json_extract(data, '$.account_type') AS VARCHAR) AS account_type,
    CAST(json_extract(data, '$.tax_code') AS VARCHAR) AS tax_code,
    CAST(json_extract(data, '$.owner_name') AS VARCHAR) AS owner_name,
    CAST(json_extract(data, '$.organization_unit_name') AS VARCHAR) AS organization_unit,
    CAST(json_extract(data, '$.modified_date') AS TIMESTAMP) AS modified_date
FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_customers.parquet')
WHERE CAST(json_extract(data, '$.id') AS VARCHAR) IS NOT NULL;

-- dim_contacts
CREATE TABLE IF NOT EXISTS dim_contacts (
    contact_id VARCHAR PRIMARY KEY,
    contact_name VARCHAR,
    mobile VARCHAR,
    email VARCHAR,
    job_title VARCHAR,
    account_code VARCHAR,
    owner_name VARCHAR,
    organization_unit VARCHAR,
    modified_date TIMESTAMP
);

-- Migration: Add missing columns
ALTER TABLE dim_contacts ADD COLUMN IF NOT EXISTS account_code VARCHAR;
ALTER TABLE dim_contacts ADD COLUMN IF NOT EXISTS owner_name VARCHAR;
ALTER TABLE dim_contacts ADD COLUMN IF NOT EXISTS organization_unit VARCHAR;

INSERT OR REPLACE INTO dim_contacts (
    contact_id, contact_name, mobile, email, job_title, 
    account_code, owner_name, organization_unit, modified_date
)
SELECT DISTINCT
    CAST(json_extract(data, '$.id') AS VARCHAR) AS contact_id,
    CAST(json_extract(data, '$.contact_name') AS VARCHAR) AS contact_name,
    CAST(json_extract(data, '$.mobile') AS VARCHAR) AS mobile,
    CAST(json_extract(data, '$.email') AS VARCHAR) AS email,
    CAST(json_extract(data, '$.title') AS VARCHAR) AS job_title,
    CAST(json_extract(data, '$.account_code') AS VARCHAR) AS account_code,
    CAST(json_extract(data, '$.owner_name') AS VARCHAR) AS owner_name,
    CAST(json_extract(data, '$.organization_unit_name') AS VARCHAR) AS organization_unit,
    CAST(json_extract(data, '$.modified_date') AS TIMESTAMP) AS modified_date
FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_contacts.parquet')
WHERE CAST(json_extract(data, '$.id') AS VARCHAR) IS NOT NULL;

-- dim_products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id VARCHAR PRIMARY KEY,
    product_code VARCHAR UNIQUE, -- Required for foreign key references
    product_name VARCHAR,
    category VARCHAR,
    unit VARCHAR,
    unit_price DOUBLE,
    product_properties VARCHAR,
    modified_date TIMESTAMP
);

-- Migration: Add missing columns
ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS unit_price DOUBLE;
ALTER TABLE dim_products ADD COLUMN IF NOT EXISTS product_properties VARCHAR;

INSERT INTO dim_products (
    product_id, product_code, product_name, category, unit, 
    unit_price, product_properties, modified_date
)
SELECT DISTINCT
    CAST(json_extract(data, '$.id') AS VARCHAR) AS product_id,
    CAST(json_extract(data, '$.product_code') AS VARCHAR) AS product_code,
    CAST(json_extract(data, '$.product_name') AS VARCHAR) AS product_name,
    CAST(json_extract(data, '$.product_category') AS VARCHAR) AS category,
    CAST(json_extract(data, '$.usage_unit') AS VARCHAR) AS unit,
    CAST(json_extract(data, '$.unit_price') AS DOUBLE) AS unit_price,
    CAST(json_extract(data, '$.product_properties') AS VARCHAR) AS product_properties,
    CAST(json_extract(data, '$.modified_date') AS TIMESTAMP) AS modified_date
FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_products.parquet')
WHERE CAST(json_extract(data, '$.id') AS VARCHAR) IS NOT NULL
ON CONFLICT (product_id) DO UPDATE SET
    product_code = excluded.product_code,
    product_name = excluded.product_name,
    category = excluded.category,
    unit = excluded.unit,
    unit_price = excluded.unit_price,
    product_properties = excluded.product_properties,
    modified_date = excluded.modified_date;

-- dim_stocks
CREATE TABLE IF NOT EXISTS dim_stocks (
    stock_id VARCHAR PRIMARY KEY,
    stock_code VARCHAR,
    stock_name VARCHAR,
    modified_date TIMESTAMP
);

INSERT OR REPLACE INTO dim_stocks
SELECT DISTINCT
    CAST(json_extract(data, '$.async_id') AS VARCHAR) AS stock_id,
    CAST(json_extract(data, '$.stock_code') AS VARCHAR) AS stock_code,
    CAST(json_extract(data, '$.stock_name') AS VARCHAR) AS stock_name,
    CAST(json_extract(data, '$.modified_date') AS TIMESTAMP) AS modified_date
FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_stocks.parquet')
WHERE CAST(json_extract(data, '$.async_id') AS VARCHAR) IS NOT NULL;

-- dim_employees (Heuristic: Derived from Sale Orders)
CREATE TABLE IF NOT EXISTS dim_employees (
    employee_code VARCHAR PRIMARY KEY,
    employee_name VARCHAR,
    modified_date TIMESTAMP
);

INSERT OR REPLACE INTO dim_employees
SELECT DISTINCT
    CAST(json_extract(data, '$.employee_code') AS VARCHAR) AS employee_code,
    CAST(json_extract(data, '$.recorded_sale_users_name') AS VARCHAR) AS employee_name,
    MAX(CAST(json_extract(data, '$.modified_date') AS TIMESTAMP)) AS modified_date
FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_sale_orders.parquet')
WHERE CAST(json_extract(data, '$.employee_code') AS VARCHAR) IS NOT NULL
GROUP BY 1, 2;

-- 3. Fact Tables (Upsert)

-- fact_sale_orders
CREATE TABLE IF NOT EXISTS fact_sale_orders (
    sale_order_id VARCHAR PRIMARY KEY,
    sale_order_no VARCHAR,
    order_date DATE REFERENCES dim_date(date_key),
    customer_code VARCHAR,
    contact_code VARCHAR,
    employee_code VARCHAR REFERENCES dim_employees(employee_code),
    status VARCHAR,
    total_amount DOUBLE,
    total_discount DOUBLE,
    total_vat DOUBLE,
    modified_date TIMESTAMP
);

-- Migration: Add missing columns
ALTER TABLE fact_sale_orders ADD COLUMN IF NOT EXISTS employee_code VARCHAR;
ALTER TABLE fact_sale_orders ADD COLUMN IF NOT EXISTS status VARCHAR;

INSERT OR REPLACE INTO fact_sale_orders (
    sale_order_id, sale_order_no, order_date, customer_code, 
    contact_code, employee_code, status, total_amount, 
    total_discount, total_vat, modified_date
)
SELECT
    CAST(json_extract(data, '$.id') AS VARCHAR) AS sale_order_id,
    CAST(json_extract(data, '$.sale_order_no') AS VARCHAR) AS sale_order_no,
    CAST(json_extract(data, '$.sale_order_date') AS DATE) AS order_date,
    CAST(json_extract(data, '$.account_code') AS VARCHAR) AS customer_code,
    CAST(json_extract(data, '$.contact_code') AS VARCHAR) AS contact_code,
    CAST(json_extract(data, '$.employee_code') AS VARCHAR) AS employee_code,
    CAST(json_extract(data, '$.approved_status') AS VARCHAR) AS status,
    CAST(json_extract(data, '$.total_summary') AS DOUBLE) AS total_amount,
    CAST(json_extract(data, '$.discount_summary') AS DOUBLE) AS total_discount,
    CAST(json_extract(data, '$.tax_summary') AS DOUBLE) AS total_vat,
    CAST(json_extract(data, '$.modified_date') AS TIMESTAMP) AS modified_date
FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_sale_orders.parquet')
WHERE CAST(json_extract(data, '$.id') AS VARCHAR) IS NOT NULL;

-- fact_sale_order_items (Granular line items)
CREATE TABLE IF NOT EXISTS fact_sale_order_items (
    order_item_id VARCHAR PRIMARY KEY,
    sale_order_id VARCHAR REFERENCES fact_sale_orders(sale_order_id),
    product_code VARCHAR REFERENCES dim_products(product_code),
    quantity DOUBLE,
    unit_price DOUBLE,
    discount_amount DOUBLE,
    tax_amount DOUBLE,
    total_amount DOUBLE,
    is_promotion BOOLEAN,
    modified_date TIMESTAMP
);

INSERT OR REPLACE INTO fact_sale_order_items
SELECT
    CAST(json_extract(item, '$.id') AS VARCHAR) AS order_item_id,
    CAST(json_extract(data, '$.id') AS VARCHAR) AS sale_order_id,
    CAST(json_extract(item, '$.product_code') AS VARCHAR) AS product_code,
    CAST(json_extract(item, '$.amount') AS DOUBLE) AS quantity,
    CAST(json_extract(item, '$.price') AS DOUBLE) AS unit_price,
    CAST(json_extract(item, '$.discount') AS DOUBLE) AS discount_amount,
    CAST(json_extract(item, '$.tax') AS DOUBLE) AS tax_amount,
    CAST(json_extract(item, '$.total') AS DOUBLE) AS total_amount,
    CAST(json_extract(item, '$.is_promotion') AS BOOLEAN) AS is_promotion,
    CAST(json_extract(data, '$.modified_date') AS TIMESTAMP) AS modified_date
FROM (
    SELECT 
        data,
        unnest(CAST(json_extract(data, '$.sale_order_product_mappings') AS JSON[])) AS item
    FROM read_parquet('{{PROJECT_ROOT}}/data/staging/stg_sale_orders.parquet')
)
WHERE CAST(json_extract(item, '$.id') AS VARCHAR) IS NOT NULL;

-- Helper views for Power BI
CREATE OR REPLACE VIEW v_sales_analysis AS
SELECT 
    f.*,
    c.customer_name,
    c.address AS customer_address,
    c.province AS customer_province,
    c.account_type,
    e.employee_name,
    d.year,
    d.month,
    d.quarter
FROM fact_sale_orders f
-- Join on business codes as requested
LEFT JOIN dim_customers c ON f.customer_code = c.customer_code
LEFT JOIN dim_employees e ON f.employee_code = e.employee_code
LEFT JOIN dim_date d ON f.order_date = d.date_key;

CREATE OR REPLACE VIEW v_item_analysis AS
SELECT 
    i.*,
    f.sale_order_no,
    f.order_date,
    p.product_name,
    p.category AS product_category,
    c.customer_name,
    c.province AS customer_province,
    e.employee_name,
    d.year,
    d.month,
    d.quarter
FROM fact_sale_order_items i
JOIN fact_sale_orders f ON i.sale_order_id = f.sale_order_id
LEFT JOIN dim_products p ON i.product_code = p.product_code
-- Join on business codes as requested
LEFT JOIN dim_customers c ON f.customer_code = c.customer_code
LEFT JOIN dim_employees e ON f.employee_code = e.employee_code
LEFT JOIN dim_date d ON f.order_date = d.date_key;
