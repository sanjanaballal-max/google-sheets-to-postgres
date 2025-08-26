import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import gspread
import logging
from google.oauth2.service_account import Credentials

# --------------------------
# 0. Logging Configuration
# --------------------------
LOG_FILE = "etl_extraction_load.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.info("ETL process started: Extraction + Load steps")

try:
    # --------------------------
    # 1. Google Sheets Connection
    # --------------------------
    SCOPE = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    CREDS_FILE = os.path.join(BASE_DIR, "service_account.json")

    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPE)
    client = gspread.authorize(creds)
    SHEET_ID = "1503y3s8mtgPpPqEPeQ7SZbkgHt1OOQYfh2mrmNxbPBM"
    SHEET_NAMES = ["customers", "products", "orders", "payments", "delivery"]

    logging.info("Connected to Google Sheets successfully.")

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
    logging.info("Connected to PostgreSQL database.")

    # --------------------------
    # 3. Drop & Recreate Bronze Schema
    # --------------------------
    cur.execute("DROP SCHEMA IF EXISTS bronze CASCADE;")
    cur.execute("CREATE SCHEMA bronze;")
    conn.commit()
    logging.info("Bronze schema recreated successfully.")

    # --------------------------
    # 4. Create Tables (match Apps Script / Sheet)
    # --------------------------
    TABLE_QUERIES = {
        "customers": """
            CREATE TABLE bronze.customers (
                customer_id VARCHAR(10) PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                city VARCHAR(50),
                signup_date DATE,
                age INT,
                customer_satisfaction_score INT,
                loyalty_points INT
            )
        """,
        "products": """
            CREATE TABLE bronze.products (
                product_id VARCHAR(10) PRIMARY KEY,
                name VARCHAR(150),
                category VARCHAR(50),
                price NUMERIC,
                stock INT,
                rating NUMERIC(2,1),
                discount_percent NUMERIC(5,2),
                return_rate NUMERIC(5,2),
                brand VARCHAR(50)
            )
        """,
        "orders": """
            CREATE TABLE bronze.orders (
                order_id VARCHAR(10) PRIMARY KEY,
                customer_id VARCHAR(10),
                product_id VARCHAR(10),
                order_date DATE,
                total_amount NUMERIC,
                payment_type VARCHAR(50),
                order_status VARCHAR(50),
                repeat_customer BOOLEAN,
                cancellation_flag BOOLEAN
            )
        """,
        "payments": """
            CREATE TABLE bronze.payments (
                payment_id VARCHAR(10) PRIMARY KEY,
                order_id VARCHAR(10),
                paymnt_date DATE,
                payment_type VARCHAR(50),
                payment_status VARCHAR(50),
                refund_flag BOOLEAN
            )
        """,
        "delivery": """
            CREATE TABLE bronze.delivery (
                delivery_id VARCHAR(10) PRIMARY KEY,
                order_id VARCHAR(10),
                deliver_date DATE,
                delivery_partner VARCHAR(50),
                delivery_status VARCHAR(50),
                customer_feedback TEXT
            )
        """
    }

    for table, query in TABLE_QUERIES.items():
        cur.execute(query)
        logging.info(f"Table bronze.{table} created successfully.")
    conn.commit()

    # --------------------------
    # 5. Load Data from Google Sheets
    # --------------------------
    def get_data(sheet_name):
        logging.info(f"Extracting data from Google Sheet: {sheet_name}")
        sheet = client.open_by_key(SHEET_ID).worksheet(sheet_name)
        data = sheet.get_all_records()
        df = pd.DataFrame(data)
        if df.empty:
            logging.warning(f"No data found in sheet: {sheet_name}")
        else:
            # Normalize column names: lowercase, strip spaces
            df.columns = df.columns.str.strip().str.lower()
        return df

    # --------------------------
    # 6. Insert Data into PostgreSQL
    # --------------------------
    def insert_data(df, table):
        if df.empty:
            logging.warning(f"No data to insert for table: {table}")
            return

        # Type conversions
        if table == "customers":
            df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce').dt.date
            df['age'] = pd.to_numeric(df['age'], errors='coerce').astype('Int64')
            df['customer_satisfaction_score'] = pd.to_numeric(df['customer_satisfaction_score'], errors='coerce').astype('Int64')
            df['loyalty_points'] = pd.to_numeric(df['loyalty_points'], errors='coerce').astype('Int64')
        elif table == "products":
            df['price'] = pd.to_numeric(df['price'], errors='coerce')
            df['stock'] = pd.to_numeric(df['stock'], errors='coerce').astype('Int64')
            df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
            df['discount_percent'] = pd.to_numeric(df['discount_percent'], errors='coerce')
            df['return_rate'] = pd.to_numeric(df['return_rate'], errors='coerce')
        elif table == "orders":
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce').dt.date
            df['total_amount'] = pd.to_numeric(df['total_amount'], errors='coerce')
            df['repeat_customer'] = df['repeat_customer'].apply(lambda x: True if x in [1,'1',True] else False)
            df['cancellation_flag'] = df['cancellation_flag'].apply(lambda x: True if x in [1,'1',True] else False)
        elif table == "payments":
            df['paymnt_date'] = pd.to_datetime(df['paymnt_date'], errors='coerce').dt.date
            df['refund_flag'] = df['refund_flag'].apply(lambda x: True if x in [1,'1',True] else False)
        elif table == "delivery":
            df['deliver_date'] = pd.to_datetime(df['deliver_date'], errors='coerce').dt.date

        columns = list(df.columns)
        values = [tuple(None if pd.isna(x) else x for x in row) for row in df.to_numpy()]

        insert_query = sql.SQL("INSERT INTO bronze.{} ({}) VALUES ({})").format(
            sql.Identifier(table),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )

        for val in values:
            cur.execute(insert_query, val)
        conn.commit()
        logging.info(f"Data inserted into bronze.{table} successfully. Rows: {len(values)}")

    # --------------------------
    # 7. Run ETL Pipeline
    # --------------------------
    for sheet in SHEET_NAMES:
        df = get_data(sheet)
        insert_data(df, sheet)

    logging.info("All data loaded into PostgreSQL bronze schema successfully!")

except Exception as e:
    logging.error(f"Error in ETL process: {e}", exc_info=True)

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()
    logging.info("PostgreSQL connection closed.")
