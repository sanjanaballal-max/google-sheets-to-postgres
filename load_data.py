import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# --------------------------
# 1. Google Sheets Connection
# --------------------------
SCOPE = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CREDS_FILE = os.path.join(BASE_DIR, "service_account.json")  # Ensure file exists

CREDS = ServiceAccountCredentials.from_json_keyfile_name(CREDS_FILE, SCOPE)
client = gspread.authorize(CREDS)

SHEET_ID = "11HNNAgtUgPgJ8c_C2nuHQrj3dNQhw4d7uidmRMUibVA"
SHEET_NAMES = ["customers", "products", "orders", "payments", "delivery"]

# --------------------------
# 2. PostgreSQL Connection
# --------------------------
DB_CONFIG = {
    "host": "localhost",
    "database": "etl_project",
    "user": "postgres",
    "password": "example_password",
    "port": "5432"
}

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# --------------------------
# 3. Drop & Recreate Bronze Schema
# --------------------------
cur.execute("DROP SCHEMA IF EXISTS bronze CASCADE;")
cur.execute("CREATE SCHEMA bronze;")
conn.commit()

# --------------------------
# 4. Create Tables (Dates as VARCHAR, no type conversions)
# --------------------------
TABLE_QUERIES = {
    "customers": """
        CREATE TABLE bronze.customers (
            customer_id VARCHAR(50) PRIMARY KEY,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(150),
            city VARCHAR(100),
            signup_date VARCHAR(50),
            age VARCHAR(20),
            customer_satisfaction_score VARCHAR(20),
            loyalty_points VARCHAR(20)
        )
    """,
    "products": """
        CREATE TABLE bronze.products (
            product_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(150),
            category VARCHAR(100),
            price VARCHAR(50),
            stock VARCHAR(20),
            rating VARCHAR(20),
            discount_percent VARCHAR(20),
            return_rate VARCHAR(20),
            brand VARCHAR(100)
        )
    """,
    "orders": """
        CREATE TABLE bronze.orders (
            order_id VARCHAR(50) PRIMARY KEY,
            customer_id VARCHAR(50),
            order_date VARCHAR(50),
            total_amount VARCHAR(50),
            payment_type VARCHAR(50),
            order_status VARCHAR(50),
            repeat_customer VARCHAR(20),
            cancellation_flag VARCHAR(20)
        )
    """,
    "payments": """
        CREATE TABLE bronze.payments (
            payment_id VARCHAR(50) PRIMARY KEY,
            order_id VARCHAR(50),
            paymnt_date VARCHAR(50),
            payment_type VARCHAR(50),
            payment_status VARCHAR(50),
            refund_flag VARCHAR(20)
        )
    """,
    "delivery": """
        CREATE TABLE bronze.delivery (
            delivery_id VARCHAR(50) PRIMARY KEY,
            order_id VARCHAR(50),
            deliver_date VARCHAR(50),
            delivery_partner VARCHAR(100),
            delivery_status VARCHAR(50),
            customer_feedback TEXT
        )
    """
}

for table, query in TABLE_QUERIES.items():
    cur.execute(query)
conn.commit()

# --------------------------
# 5. Load Data from Google Sheets
# --------------------------
def get_data(sheet_name):
    sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
    data = sheet.get_all_records()
    return pd.DataFrame(data)

# --------------------------
# 6. Insert Data into PostgreSQL
# --------------------------
def insert_data(df, table):
    if df.empty:
        return

    columns = list(df.columns)
    values = [tuple(str(x) for x in row) for row in df.to_numpy()]  # Convert all to string

    insert_query = sql.SQL("INSERT INTO bronze.{} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

    for val in values:
        cur.execute(insert_query, val)
    conn.commit()

# --------------------------
# 7. Run Pipeline (Simple Insert, no ETL)
# --------------------------
for sheet in SHEET_NAMES:
    df = get_data(sheet)
    if not df.empty:
        insert_data(df, sheet)
        print(f"Data inserted into bronze.{sheet} successfully.")

# --------------------------
# 8. Close Connection
# --------------------------
cur.close()
conn.close()
print("All data loaded into PostgreSQL bronze schema successfully!")
