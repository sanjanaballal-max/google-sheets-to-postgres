import pandas as pd
import logging
from sqlalchemy import create_engine, text
import json

# =======================
# CONFIG
# =======================
DB_URI = "postgresql+psycopg2://postgres:example_password@localhost:5432/etl_project"
engine = create_engine(DB_URI)

SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
LOG_FILE = "etl_silver_to_gold.log"

# =======================
# LOGGING
# =======================
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

# =======================
# READ SILVER TABLES
# =======================
def read_silver_table(table_name):
    df = pd.read_sql(text(f"SELECT * FROM {SILVER_SCHEMA}.{table_name}"), engine)
    logging.info(f"Read {len(df)} rows from silver.{table_name}")
    return df

# =======================
# CREATE CUSTOMER AGGREGATE
# =======================
def create_customer_agg(customers, orders, payments, delivery):
    logging.info("Creating customer-centric aggregate table...")

    df = customers.copy()

    # Total orders and average order amount
    orders_agg = orders.groupby('customer_id', as_index=False).agg(
        total_orders=pd.NamedAgg(column='order_id', aggfunc='count'),
        avg_order_amount=pd.NamedAgg(column='total_amount', aggfunc='mean')
    )
    df = df.merge(orders_agg, on='customer_id', how='left')

    # Total payments per customer
    payments_agg = payments.merge(orders[['order_id', 'customer_id']], on='order_id', how='left')
    payments_agg = payments_agg.groupby('customer_id', as_index=False).agg(
        total_payments=pd.NamedAgg(column='payment_id', aggfunc='count')
    )
    df = df.merge(payments_agg, on='customer_id', how='left')

    # Total deliveries per customer
    delivery_agg = delivery.merge(orders[['order_id', 'customer_id']], on='order_id', how='left')
    delivery_agg = delivery_agg.groupby('customer_id', as_index=False).agg(
        total_deliveries=pd.NamedAgg(column='delivery_id', aggfunc='count')
    )
    df = df.merge(delivery_agg, on='customer_id', how='left')

    df.fillna(0, inplace=True)
    logging.info(f"Customer aggregate table created: {len(df)} rows")
    return df

# =======================
# CREATE PRODUCT AGGREGATE
# =======================
def create_product_agg(products, orders):
    logging.info("Creating product-centric aggregate table...")

    df = products.copy()

    # Total orders and total revenue per product
    orders_agg = orders.groupby('product_id', as_index=False).agg(
        total_sold=pd.NamedAgg(column='order_id', aggfunc='count'),
        total_revenue=pd.NamedAgg(column='total_amount', aggfunc='sum')
    )
    df = df.merge(orders_agg, on='product_id', how='left')

    # Fill NaNs with 0
    df[['total_sold','total_revenue']] = df[['total_sold','total_revenue']].fillna(0)

    logging.info(f"Product aggregate table created: {len(df)} rows")
    return df

# =======================
# LOAD GOLD TABLES
# =======================
def load_gold(df, table_name):
    logging.info(f"Loading {table_name} to gold schema...")
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}"))
        df.to_sql(table_name, conn, schema=GOLD_SCHEMA, if_exists='replace', index=False)
    logging.info(f"{table_name} loaded successfully to gold schema. Rows: {len(df)}")

# =======================
# MAIN
# =======================
def main():
    logging.info("Starting Silver → Gold aggregation ETL...")

    customers = read_silver_table('customers')
    products = read_silver_table('products')
    orders = read_silver_table('orders')
    payments = read_silver_table('payments')
    delivery = read_silver_table('delivery')

    # Aggregations
    customer_agg = create_customer_agg(customers, orders, payments, delivery)
    product_agg = create_product_agg(products, orders)

    # Load into gold schema
    load_gold(customer_agg, 'customer_agg')
    load_gold(product_agg, 'product_agg')

    logging.info("Silver → Gold ETL completed successfully!")

if __name__ == "__main__":
    main()
