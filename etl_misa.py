import os
import requests
import duckdb
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import time
import json
from pathlib import Path

load_dotenv()

# Configuration
BASE_URL = os.getenv("MISA_BASE_URL", "https://crmconnect.misa.vn")
CLIENT_ID = os.getenv("MISA_CLIENT_ID")
CLIENT_SECRET = os.getenv("MISA_CLIENT_SECRET")
DB_PATH = os.getenv("DUCKDB_PATH", "hbc_data_warehouse.duckdb")
STAGING_DIR = Path("data/staging")

# Ensure staging directory exists
STAGING_DIR.mkdir(parents=True, exist_ok=True)

def get_token():
    url = f"{BASE_URL}/api/v2/Account"
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    print(f"Authenticating with Misa CRM at {url}...")
    response = requests.post(url, json=payload)
    response.raise_for_status()
    data = response.json()
    if data.get("success"):
        return data.get("data")
    else:
        raise Exception(f"Failed to get token: {data.get('error_message')}")

def fetch_data(endpoint, token, last_modified=None):
    all_data = []
    page = 0
    page_size = 100
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Clientid": CLIENT_ID
    }
    
    while True:
        params = {
            "page": page,
            "pageSize": page_size,
            "orderBy": "modified_date",
            "isDescending": True
        }
        
        print(f"  Fetching {endpoint} page {page}...")
        
        # Retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(f"{BASE_URL}{endpoint}", headers=headers, params=params, timeout=30)
                response.raise_for_status()
                break
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if attempt == max_retries - 1:
                    raise
                print(f"    Connection error, retrying in 2s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(2)
        
        res_json = response.json()
        
        data = res_json.get("data")
        
        if not data or len(data) == 0:
            break
            
        new_records = []
        stop_fetching = False
        
        for record in data:
            modified_date = record.get("modified_date")
            if last_modified and modified_date:
                if modified_date <= last_modified:
                    stop_fetching = True
                    break
            new_records.append(record)
            
        all_data.extend(new_records)
        
        if stop_fetching or len(data) < page_size:
            break
            
        page += 1
        time.sleep(0.1)
        
    return all_data

def run_etl():
    conn = duckdb.connect(DB_PATH)
    token = get_token()
    
    endpoints = {
        "contacts": "/api/v2/Contacts",
        "customers": "/api/v2/Customers",
        "products": "/api/v2/Products",
        "sale_orders": "/api/v2/SaleOrders",
        "stocks": "/api/v2/Stocks"
    }
    
    for table_name, endpoint in endpoints.items():
        print(f"Processing {table_name}...")
        
        # Get last modified date from metadata
        conn.execute("CREATE TABLE IF NOT EXISTS etl_metadata (table_name VARCHAR PRIMARY KEY, last_modified TIMESTAMP)")
        res = conn.execute("SELECT last_modified FROM etl_metadata WHERE table_name = ?", (table_name,)).fetchone()
        last_modified = res[0].isoformat() if res and res[0] else None
        
        # The 'stocks' endpoint is a small static list that doesn't support 
        # pagination or incremental filtering, so we fetch it in one Go.
        if table_name == "stocks":
             headers = {"Authorization": f"Bearer {token}", "Clientid": CLIENT_ID}
             res = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
             res.raise_for_status()
             records = res.json().get("data", [])
        else:
            records = fetch_data(endpoint, token, last_modified)
        
        # Always save a "fresh" Parquet file for the current run's incremental slice.
        # This prevents transforms.sql from re-processing the previous run's batch if no new data is found.
        parquet_path = STAGING_DIR / f"stg_{table_name}.parquet"
        json_records = [json.dumps(r) for r in records]
        df = pd.DataFrame({"data": json_records, "extracted_at": [datetime.now()] * len(json_records)})
        df.to_parquet(parquet_path, index=False)
        
        if records:
            print(f"  Saved {len(records)} NEW records to {parquet_path}")
            # Update metadata only if we have new records with modification dates
            mod_dates = [r.get("modified_date") for r in records if r.get("modified_date")]
            if mod_dates:
                max_mod = max(mod_dates)
                conn.execute("""
                    INSERT INTO etl_metadata (table_name, last_modified) 
                    VALUES (?, ?) 
                    ON CONFLICT(table_name) DO UPDATE SET last_modified = excluded.last_modified
                """, (table_name, max_mod))
        else:
            print(f"  No new records for {table_name}. Staging batch is empty.")

    # Run transformations
    print("Running SQL transformations from Parquet...")
    if os.path.exists("transforms.sql"):
        # Use absolute path for {{PROJECT_ROOT}} so views work in DBeaver
        project_root = str(Path(__file__).parent.absolute())
        with open("transforms.sql", "r") as f:
            sql_script = f.read().replace("{{PROJECT_ROOT}}", project_root)
            
            statements = sql_script.split(";")
            for stmt in statements:
                if stmt.strip():
                    conn.execute(stmt)
        print("Transformations completed successfully.")
    else:
        print("Warning: transforms.sql not found.")

    conn.close()
    print("ETL Job Finished.")

if __name__ == "__main__":
    run_etl()
