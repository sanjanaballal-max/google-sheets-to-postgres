import os
import json
import logging
from typing import Dict, List
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import datetime

# =======================
# CONFIG
# =======================
DB_CONFIG = {
    "host": "localhost",
    "dbname": "etl_project",
    "user": "postgres",
    "password": "example_password",
    "port": 5432,
}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "etl_pipeline.log")
TABLES = ["customers", "products", "orders", "payments", "delivery"]

# =======================
# LOGGING
# =======================
def init_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    for h in list(logger.handlers):
        logger.removeHandler(h)
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(LOG_FILE)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)

# =======================
# DB HELPERS
# =======================
def db_uri() -> str:
    return f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"

def get_engine() -> Engine:
    return create_engine(db_uri(), future=True)

def run_sql(engine: Engine, sql: str):
    with engine.begin() as conn:
        conn.execute(text(sql))

def create_schemas(engine: Engine):
    run_sql(engine, "CREATE SCHEMA IF NOT EXISTS silver;")
    run_sql(engine, "CREATE SCHEMA IF NOT EXISTS audit;")
    run_sql(engine, """
        CREATE TABLE IF NOT EXISTS audit.rejected_rows (
            id BIGSERIAL PRIMARY KEY,
            stage TEXT NOT NULL,
            table_name TEXT NOT NULL,
            rule_name TEXT NOT NULL,
            reason TEXT NOT NULL,
            row_data JSONB NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

# =======================
# EXTRACT
# =======================
def read_bronze(engine: Engine) -> Dict[str, pd.DataFrame]:
    logging.info("Reading bronze tables ...")
    data: Dict[str, pd.DataFrame] = {}
    with engine.connect() as conn:
        for t in TABLES:
            df = pd.read_sql(text(f"SELECT * FROM bronze.{t}"), conn)
            data[t] = df
            logging.info(f"bronze.{t}: {len(df)} rows read")
    return data

# =======================
# UTILS
# =======================
def to_date(series: pd.Series) -> pd.Series:
    if series is None:
        return series
    series = series.astype(str).str.strip()
    return pd.to_datetime(series, errors="coerce").dt.date  # removed infer_datetime_format

def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def to_bool(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.lower()
    true_vals = {"y", "yes", "true", "1"}
    false_vals = {"n", "no", "false", "0"}
    return s.map(lambda x: True if x in true_vals else (False if x in false_vals else None))

def reject_append(rejections: List[dict], stage: str, table: str, rule: str, reason: str, df: pd.DataFrame):
    if df is None or df.empty:
        return
    rejections.append({"stage": stage, "table_name": table, "rule_name": rule, "reason": reason, "rows": df.copy()})
    logging.warning(f"[DQ] {table} | {rule} | {reason} | rejected_rows={len(df)}")

def safe_json(row):
    row_dict = row.to_dict()
    for k, v in row_dict.items():
        if pd.isna(v):
            row_dict[k] = None
        elif isinstance(v, (datetime.date, datetime.datetime)):
            row_dict[k] = v.isoformat()
    return row_dict

# =======================
# TRANSFORM
# =======================
def transform_to_silver(bronze: Dict[str, pd.DataFrame]) -> (Dict[str, pd.DataFrame], List[dict]):
    stage = "silver"
    rejections: List[dict] = []

    # --- Customers ---
    c = bronze["customers"].copy()
    for col in c.columns:
        c[col] = c[col].astype(str).str.strip()
    c["signup_date"] = to_date(c.get("signup_date"))
    c["age"] = to_numeric(c.get("age")).astype("Int64")

    missing_id = c[c["customer_id"].isna()]
    reject_append(rejections, stage, "customers", "MISSING_PK", "customer_id is null", missing_id)
    c = c[c["customer_id"].notna()]

    neg_age = c[c["age"] < 0]
    reject_append(rejections, stage, "customers", "AGE_RANGE", "age < 0", neg_age)
    c = c[(c["age"].isna()) | (c["age"] >= 0)]

    # --- Products ---
    p = bronze["products"].copy()
    for col in p.columns:
        p[col] = p[col].astype(str).str.strip()
    p["price"] = to_numeric(p.get("price"))
    p["stock"] = to_numeric(p.get("stock")).astype("Int64")

    bad_price = p[p["price"] <= 0]
    reject_append(rejections, stage, "products", "PRICE_RANGE", "price <= 0", bad_price)
    p = p[p["price"].isna() | (p["price"] > 0)]

    bad_stock = p[p["stock"] < 0]
    reject_append(rejections, stage, "products", "STOCK_NEG", "stock < 0", bad_stock)
    p = p[p["stock"] >= 0]

    # --- Orders ---
    o = bronze["orders"].copy()
    for col in o.columns:
        o[col] = o[col].astype(str).str.strip()
    o["order_date"] = to_date(o.get("order_date"))
    o["total_amount"] = to_numeric(o.get("total_amount"))
    o["repeat_customer"] = to_bool(o.get("repeat_customer"))
    o["cancellation_flag"] = to_bool(o.get("cancellation_flag"))

    bad_order_amt = o[o["total_amount"] <= 0]
    reject_append(rejections, stage, "orders", "TOTAL_AMT", "total_amount <= 0", bad_order_amt)
    o = o[o["total_amount"].isna() | (o["total_amount"] > 0)]

    missing_date = o[o["order_date"].isna()]
    reject_append(rejections, stage, "orders", "MISSING_DATE", "order_date is null", missing_date)
    o = o[o["order_date"].notna()]

    merged = o.merge(c[["customer_id", "signup_date"]], on="customer_id", how="left")
    bad_dates = merged[merged["order_date"] < merged["signup_date"]]
    reject_append(rejections, stage, "orders", "DATE_LOGIC", "order_date < signup_date", bad_dates)
    o = o[~o["order_id"].isin(bad_dates["order_id"])]

    # --- Payments ---
    pay = bronze["payments"].copy()
    for col in pay.columns:
        pay[col] = pay[col].astype(str).str.strip()
    pay["paymnt_date"] = to_date(pay.get("paymnt_date"))

    merged_pay = pay.merge(o[["order_id","order_date","cancellation_flag"]], on="order_id", how="left")
    bad_pay = merged_pay[
        (merged_pay["paymnt_date"] < merged_pay["order_date"]) |
        ((merged_pay["cancellation_flag"]==True) & (merged_pay["payment_status"].str.lower()=="success"))
    ]
    reject_append(rejections, stage, "payments", "PAYMENT_LOGIC", "payment before order or cancelled+success", bad_pay)
    pay = pay[~pay["payment_id"].isin(bad_pay["payment_id"])]

    # --- Delivery (Relaxed logic: Only check order_date) ---
    d = bronze["delivery"].copy()
    for col in d.columns:
        d[col] = d[col].astype(str).str.strip()
    d["deliver_date"] = to_date(d.get("deliver_date"))

    merged_del = d.merge(o[["order_id", "order_date"]], on="order_id", how="left")
    bad_del = merged_del[merged_del["deliver_date"] < merged_del["order_date"]]
    reject_append(rejections, stage, "delivery", "DELIVERY_LOGIC", "deliver_date < order_date", bad_del)
    d = d[~d["delivery_id"].isin(bad_del["delivery_id"])]

    silver_tables = {
        "customers": c,
        "products": p,
        "orders": o,
        "payments": pay,
        "delivery": d
    }
    return silver_tables, rejections

# =======================
# LOAD
# =======================
def load_silver(engine: Engine, silver_tables: Dict[str, pd.DataFrame]):
    logging.info("Loading silver tables ...")
    with engine.begin() as conn:
        for t, df in silver_tables.items():
            df.to_sql(t, conn, schema="silver", if_exists="replace", index=False)
            logging.info(f"silver.{t}: loaded {len(df)} rows")

def load_rejections(engine: Engine, rejections: List[dict]):
    logging.info("Loading rejected rows into audit.rejected_rows ...")
    with engine.begin() as conn:
        for r in rejections:
            df = r["rows"]
            for _, row in df.iterrows():
                row_json = json.dumps(safe_json(row))
                conn.execute(text("""
                    INSERT INTO audit.rejected_rows(stage,table_name,rule_name,reason,row_data)
                    VALUES(:stage,:table_name,:rule_name,:reason,:row_data)
                """), {
                    "stage": r["stage"],
                    "table_name": r["table_name"],
                    "rule_name": r["rule_name"],
                    "reason": r["reason"],
                    "row_data": row_json
                })

# =======================
# MAIN
# =======================
def main():
    init_logging()
    logging.info("Starting ETL pipeline ...")

    engine = get_engine()
    create_schemas(engine)

    bronze_data = read_bronze(engine)
    silver_tables, rejections = transform_to_silver(bronze_data)
    load_silver(engine, silver_tables)
    load_rejections(engine, rejections)

    logging.info("ETL pipeline completed successfully.")

if __name__ == "__main__":
    main()
