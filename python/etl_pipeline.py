# ============================================================
# ETL PIPELINE — Revenue Leakage Intelligence
# Flow: AWS S3 (raw) → Clean & Validate → MySQL (warehouse)
# ============================================================

import boto3
import pandas as pd
import numpy as np
import mysql.connector
import io
import os
from datetime import datetime

# ── CONFIG ───────────────────────────────────────────────────
S3_BUCKET   = "revenue-leakage-raw-data"
S3_PREFIX   = "raw/"
MYSQL_HOST  = "localhost"
MYSQL_USER  = "root"
MYSQL_PASS  = "roott"   # ← change this
MYSQL_DB    = "revenue_leakage"

# ── CONNECT TO S3 ────────────────────────────────────────────
s3 = boto3.client("s3", region_name="ap-south-1")

def read_csv_from_s3(filename):
    """Read a CSV file directly from S3 into a DataFrame"""
    print(f"  Reading {filename} from S3...")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_PREFIX + filename)
    df  = pd.read_csv(io.BytesIO(obj["Body"].read()))
    print(f"  → {len(df)} rows loaded")
    return df

# ── CONNECT TO MYSQL ─────────────────────────────────────────
def get_mysql_connection():
    return mysql.connector.connect(
        host     = MYSQL_HOST,
        user     = MYSQL_USER,
        password = MYSQL_PASS,
        database = MYSQL_DB
    )

def load_to_mysql(df, table_name):
    """Load a cleaned DataFrame into MySQL table"""
    conn   = get_mysql_connection()
    cursor = conn.cursor()

    # Drop and recreate table each run (full refresh)
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

    # Build CREATE TABLE from DataFrame columns
    col_defs = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        if "int" in dtype:
            col_defs.append(f"`{col}` BIGINT")
        elif "float" in dtype:
            col_defs.append(f"`{col}` FLOAT")
        else:
            col_defs.append(f"`{col}` VARCHAR(255)")

    create_sql = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
    cursor.execute(create_sql)

    # Insert rows
    cols        = ", ".join([f"`{c}`" for c in df.columns])
    placeholders = ", ".join(["%s"] * len(df.columns))
    insert_sql  = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    for _, row in df.iterrows():
        values = [None if pd.isna(v) else v for v in row]
        cursor.execute(insert_sql, values)

    conn.commit()
    cursor.close()
    conn.close()
    print(f"  → Loaded {len(df)} rows into MySQL table: {table_name}")


# ════════════════════════════════════════════════════════════
# STEP 1 — CLEAN CUSTOMERS
# ════════════════════════════════════════════════════════════
def clean_customers():
    print("\n[1/5] Cleaning customers...")
    df = read_csv_from_s3("customers_raw.csv")

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["customer_id"])
    print(f"  Duplicates removed: {before - len(df)}")

    # Standardize segment to Title Case (smb → SMB, enterprise → Enterprise)
    df["segment"] = df["segment"].str.strip().str.upper()

    # Fill missing signup_date with a default
    df["signup_date"] = df["signup_date"].fillna("2023-01-01")

    # Add cohort month column
    df["cohort_month"] = pd.to_datetime(df["signup_date"]).dt.to_period("M").astype(str)

    print(f"  Final rows: {len(df)}")
    df.to_csv("data_clean/customers_clean.csv", index=False)
    load_to_mysql(df, "dim_customer")


# ════════════════════════════════════════════════════════════
# STEP 2 — CLEAN PRODUCTS
# ════════════════════════════════════════════════════════════
def clean_products():
    print("\n[2/5] Cleaning products...")
    df = read_csv_from_s3("products_raw.csv")

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["product_id"])
    print(f"  Duplicates removed: {before - len(df)}")

    # Standardize category to Title Case
    df["category"] = df["category"].str.strip().str.title()

    # Fill missing margin with median
    median_margin = df["margin_pct"].median()
    df["margin_pct"] = df["margin_pct"].fillna(median_margin)

    print(f"  Final rows: {len(df)}")
    df.to_csv("data_clean/products_clean.csv", index=False)
    load_to_mysql(df, "dim_product")


# ════════════════════════════════════════════════════════════
# STEP 3 — CLEAN ORDERS
# ════════════════════════════════════════════════════════════
def clean_orders():
    print("\n[3/5] Cleaning orders...")
    df = read_csv_from_s3("orders_raw.csv")

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["order_id"])
    print(f"  Duplicates removed: {before - len(df)}")

    # Drop rows where order_value is null (can't use for revenue)
    df = df.dropna(subset=["order_value"])

    # Remove outliers — order value > 50000 is unrealistic
    df = df[df["order_value"] <= 50000]

    # Fill missing discount with 0
    df["discount_pct"] = df["discount_pct"].fillna(0)

    # Fix date format
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.strftime("%Y-%m-%d")

    print(f"  Final rows: {len(df)}")
    df.to_csv("data_clean/orders_clean.csv", index=False)
    load_to_mysql(df, "stg_orders")


# ════════════════════════════════════════════════════════════
# STEP 4 — CLEAN WEB EVENTS
# ════════════════════════════════════════════════════════════
def clean_web_events():
    print("\n[4/5] Cleaning web events...")
    df = read_csv_from_s3("web_events_raw.csv")

    # Standardize event types
    df["event_type"] = df["event_type"].str.strip().str.lower()
    df["event_type"] = df["event_type"].replace({
        "visit"       : "Visit",
        "add_to_cart" : "Add_To_Cart",
        "purchase"    : "Purchase"
    })

    # Drop null counts
    df = df.dropna(subset=["count"])
    df["count"] = df["count"].astype(int)

    # Fix date format
    df["event_date"] = pd.to_datetime(df["event_date"]).dt.strftime("%Y-%m-%d")

    print(f"  Final rows: {len(df)}")
    df.to_csv("data_clean/web_events_clean.csv", index=False)
    load_to_mysql(df, "stg_web_events")


# ════════════════════════════════════════════════════════════
# STEP 5 — CLEAN TARGETS
# ════════════════════════════════════════════════════════════
def clean_targets():
    print("\n[5/5] Cleaning targets...")
    df = read_csv_from_s3("targets_raw.csv")

    # Fix date format (handles both 2024-01-01 and 2024/01/01)
    df["month"] = pd.to_datetime(df["month"], format="mixed").dt.strftime("%Y-%m-%d")

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates(subset=["month", "region"])
    print(f"  Duplicates removed: {before - len(df)}")

    print(f"  Final rows: {len(df)}")
    df.to_csv("data_clean/targets_clean.csv", index=False)
    load_to_mysql(df, "stg_targets")


# ════════════════════════════════════════════════════════════
# DATA QUALITY REPORT
# ════════════════════════════════════════════════════════════
def print_quality_report():
    print("\n" + "="*50)
    print("   DATA QUALITY REPORT")
    print("="*50)
    files = {
        "customers" : "data_clean/customers_clean.csv",
        "products"  : "data_clean/products_clean.csv",
        "orders"    : "data_clean/orders_clean.csv",
        "web_events": "data_clean/web_events_clean.csv",
        "targets"   : "data_clean/targets_clean.csv",
    }
    for name, path in files.items():
        df = pd.read_csv(path)
        nulls = df.isnull().sum().sum()
        print(f"  {name:12} → {len(df):4} rows | {nulls} nulls remaining")
    print("="*50)
    print("  ETL Pipeline completed successfully!")
    print("="*50)


# ════════════════════════════════════════════════════════════
# RUN ALL STEPS
# ════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("Starting ETL Pipeline...")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    clean_customers()
    clean_products()
    clean_orders()
    clean_web_events()
    clean_targets()
    print_quality_report()