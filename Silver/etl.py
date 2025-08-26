import pandas as pd
import datetime
import json
import logging
from sqlalchemy import create_engine, text

# =======================
# CONFIG
# =======================
DB_URI = "postgresql+psycopg2://postgres:example_password@localhost:5432/etl_project"
engine = create_engine(DB_URI)

BRONZE_TABLES = ["customers", "products", "orders", "payments", "delivery"]
SILVER_SCHEMA = "silver"
AUDIT_SCHEMA = "audit"
LOG_FILE = "etl_bronze_to_silver_clean.log"

# =======================
# LOGGING SETUP
# =======================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# =======================
# UTILS
# =======================
def strip_all(df: pd.DataFrame) -> pd.DataFrame:
    """Strip leading/trailing spaces in all string columns."""
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).str.strip()
    return df

def to_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date

def to_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def safe_json(row):
    row_dict = row.to_dict()
    for k, v in row_dict.items():
        if pd.isna(v):
            row_dict[k] = None
        elif isinstance(v, (datetime.date, datetime.datetime)):
            row_dict[k] = v.isoformat()
    return row_dict

# =======================
# READ BRONZE
# =======================
def read_bronze():
    data = {}
    logging.info("Reading bronze tables...")
    for table in BRONZE_TABLES:
        df = pd.read_sql(text(f"SELECT * FROM bronze.{table}"), engine)
        df = strip_all(df)  # Strip spaces immediately after reading
        data[table] = df
        logging.info(f"Loaded bronze.{table} rows: {len(df)}")
    return data

# =======================
# TRANSFORM AND CLEAN
# =======================
def transform(bronze: dict):
    logging.info("Starting transformation and full cleaning...")
    rejections = []

    # --- CUSTOMERS ---
    customers = bronze['customers'].copy()
    customers['signup_date'] = to_date(customers.get('signup_date'))
    customers['age'] = to_numeric(customers.get('age')).astype('Int64')

    # Remove PK nulls
    bad_customers_pk = customers[customers['customer_id'].isna()]
    if not bad_customers_pk.empty:
        logging.warning(f"{len(bad_customers_pk)} customer rows rejected: NULL PK")
        rejections.append(('customers','NULL customer_id',bad_customers_pk))
    customers = customers[customers['customer_id'].notna()]

    # Remove age <0 or >100
    bad_customers_age = customers[(customers['age'] < 0) | (customers['age'] > 100)]
    if not bad_customers_age.empty:
        logging.warning(f"{len(bad_customers_age)} customer rows rejected: invalid age")
        rejections.append(('customers','Invalid age',bad_customers_age))
    customers = customers[(customers['age'].isna()) | ((customers['age'] >= 0) & (customers['age'] <= 100))]

    # Remove completely blank rows
    customers = customers.dropna(how='all')

    # --- PRODUCTS ---
    products = bronze['products'].copy()
    products['price'] = to_numeric(products.get('price'))
    products['stock'] = to_numeric(products.get('stock')).astype('Int64')

    bad_products_pk = products[products['product_id'].isna()]
    if not bad_products_pk.empty:
        logging.warning(f"{len(bad_products_pk)} product rows rejected: NULL PK")
        rejections.append(('products','NULL product_id',bad_products_pk))
    products = products[products['product_id'].notna()]

    bad_products_price = products[products['price'] <= 0]
    if not bad_products_price.empty:
        logging.warning(f"{len(bad_products_price)} product rows rejected: price <= 0")
        rejections.append(('products','Invalid price',bad_products_price))
    products = products[products['price'] > 0]

    bad_products_stock = products[products['stock'] < 0]
    if not bad_products_stock.empty:
        logging.warning(f"{len(bad_products_stock)} product rows rejected: stock < 0")
        rejections.append(('products','Invalid stock',bad_products_stock))
    products = products[products['stock'] >= 0]

    # --- ORDERS ---
    orders = bronze['orders'].copy()
    orders['order_date'] = to_date(orders.get('order_date'))
    orders['total_amount'] = to_numeric(orders.get('total_amount'))

    bad_orders_pk = orders[orders['order_id'].isna()]
    if not bad_orders_pk.empty:
        logging.warning(f"{len(bad_orders_pk)} order rows rejected: NULL PK")
        rejections.append(('orders','NULL order_id',bad_orders_pk))
    orders = orders[orders['order_id'].notna()]

    bad_orders_customer = orders[~orders['customer_id'].isin(customers['customer_id'])]
    if not bad_orders_customer.empty:
        logging.warning(f"{len(bad_orders_customer)} order rows rejected: customer_id FK mismatch")
        rejections.append(('orders','Invalid customer_id FK',bad_orders_customer))
    orders = orders[orders['customer_id'].isin(customers['customer_id'])]

    bad_orders_amount = orders[orders['total_amount'] <= 0]
    if not bad_orders_amount.empty:
        logging.warning(f"{len(bad_orders_amount)} order rows rejected: total_amount <= 0")
        rejections.append(('orders','Invalid total_amount',bad_orders_amount))
    orders = orders[orders['total_amount'] > 0]

    bad_orders_date = orders.merge(customers[['customer_id','signup_date']], on='customer_id')
    bad_orders_date = bad_orders_date[bad_orders_date['order_date'] < bad_orders_date['signup_date']]
    if not bad_orders_date.empty:
        logging.warning(f"{len(bad_orders_date)} order rows rejected: order_date < signup_date")
        rejections.append(('orders','order_date < signup_date',bad_orders_date))
    orders = orders[~orders['order_id'].isin(bad_orders_date['order_id'])]

    # --- PAYMENTS ---
    payments = bronze['payments'].copy()
    payments['paymnt_date'] = to_date(payments.get('paymnt_date'))

    bad_payments_pk = payments[payments['payment_id'].isna()]
    if not bad_payments_pk.empty:
        logging.warning(f"{len(bad_payments_pk)} payment rows rejected: NULL PK")
        rejections.append(('payments','NULL payment_id',bad_payments_pk))
    payments = payments[payments['payment_id'].notna()]

    bad_payments_order = payments[~payments['order_id'].isin(orders['order_id'])]
    if not bad_payments_order.empty:
        logging.warning(f"{len(bad_payments_order)} payment rows rejected: order_id FK mismatch")
        rejections.append(('payments','Invalid order_id FK',bad_payments_order))
    payments = payments[payments['order_id'].isin(orders['order_id'])]

    bad_payments_date = payments.merge(orders[['order_id','order_date']], on='order_id')
    bad_payments_date = bad_payments_date[bad_payments_date['paymnt_date'] < bad_payments_date['order_date']]
    if not bad_payments_date.empty:
        logging.warning(f"{len(bad_payments_date)} payment rows rejected: paymnt_date < order_date")
        rejections.append(('payments','paymnt_date < order_date',bad_payments_date))
    payments = payments[~payments['payment_id'].isin(bad_payments_date['payment_id'])]

    # --- DELIVERY ---
    delivery = bronze['delivery'].copy()
    delivery['deliver_date'] = to_date(delivery.get('deliver_date'))

    bad_delivery_pk = delivery[delivery['delivery_id'].isna()]
    if not bad_delivery_pk.empty:
        logging.warning(f"{len(bad_delivery_pk)} delivery rows rejected: NULL PK")
        rejections.append(('delivery','NULL delivery_id',bad_delivery_pk))
    delivery = delivery[delivery['delivery_id'].notna()]

    bad_delivery_order = delivery[~delivery['order_id'].isin(orders['order_id'])]
    if not bad_delivery_order.empty:
        logging.warning(f"{len(bad_delivery_order)} delivery rows rejected: order_id FK mismatch")
        rejections.append(('delivery','Invalid order_id FK',bad_delivery_order))
    delivery = delivery[delivery['order_id'].isin(orders['order_id'])]

    bad_delivery_date = delivery.merge(payments[['order_id','paymnt_date']], on='order_id')
    bad_delivery_date = bad_delivery_date[bad_delivery_date['deliver_date'] < bad_delivery_date['paymnt_date']]
    if not bad_delivery_date.empty:
        logging.warning(f"{len(bad_delivery_date)} delivery rows rejected: deliver_date < paymnt_date")
        rejections.append(('delivery','deliver_date < paymnt_date',bad_delivery_date))
    delivery = delivery[~delivery['delivery_id'].isin(bad_delivery_date['delivery_id'])]

    silver_tables = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "payments": payments,
        "delivery": delivery
    }

    return silver_tables, rejections

# =======================
# LOAD SILVER
# =======================
def load_silver(silver_tables):
    logging.info("Loading silver tables...")
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}"))
        for table, df in silver_tables.items():
            df.to_sql(table, conn, schema=SILVER_SCHEMA, if_exists='replace', index=False)
            logging.info(f"silver.{table}: loaded {len(df)} rows")

# =======================
# LOAD REJECTIONS
# =======================
def load_rejections(rejections):
    logging.info("Loading rejected rows into audit.rejected_rows...")
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {AUDIT_SCHEMA}"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {AUDIT_SCHEMA}.rejected_rows (
                id BIGSERIAL PRIMARY KEY,
                table_name TEXT NOT NULL,
                reason TEXT NOT NULL,
                row_data JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        for table, reason, df in rejections:
            if df.empty:
                continue
            for _, row in df.iterrows():
                row_json = json.dumps(safe_json(row))
                conn.execute(text(f"""
                    INSERT INTO {AUDIT_SCHEMA}.rejected_rows(table_name,reason,row_data)
                    VALUES(:table_name,:reason,:row_data)
                """), {"table_name": table, "reason": reason, "row_data": row_json})
            logging.info(f"Rejected rows logged: {table} | {reason} | {len(df)} rows")

# =======================
# MAIN
# =======================
def main():
    logging.info("ETL process started: Bronze → Silver (FULL CLEAN, strip spaces)")
    bronze_data = read_bronze()
    silver_tables, rejections = transform(bronze_data)
    load_silver(silver_tables)
    load_rejections(rejections)
    logging.info("ETL process completed successfully!")

if __name__ == "__main__":
    main()
