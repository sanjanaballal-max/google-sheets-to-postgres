import subprocess
import psycopg2
import pandas as pd
import os

# ---------- CONFIGURATION ----------
BRONZE_SCRIPT = "Bronze/load_data.py"
SILVER_SCRIPT = "Silver/etl.py"
GOLD_SCRIPT = "Gold/aggregate.py"

DB_CONFIG = {
    "dbname": "etl_project",
    "user": "postgres",
    "password": "example_password",
    "host": "localhost",
    "port": 5432
}

OUTPUT_DIR = "gold_csv_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------- HELPER: RUN PYTHON SCRIPT ----------
def run_script(script_path):
    print(f"\n[INFO] Running {script_path}...")
    result = subprocess.run(["python3", script_path], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"[SUCCESS] {script_path} executed successfully.")
    else:
        print(f"[ERROR] {script_path} failed.\n{result.stderr}")
        exit(1)


# ---------- HELPER: EXPORT TABLE TO CSV ----------
def export_table_to_csv(table_name, output_path):
    print(f"[INFO] Exporting {table_name} to {output_path}...")
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, conn)
        df.to_csv(output_path, index=False)
        print(f"[SUCCESS] {table_name} exported to {output_path}")
    finally:
        conn.close()


# ---------- MAIN PIPELINE ----------
if __name__ == "__main__":
    # Step 1: Run Bronze → Silver → Gold scripts
    run_script(BRONZE_SCRIPT)
    run_script(SILVER_SCRIPT)
    run_script(GOLD_SCRIPT)

    # Step 2: Export gold tables as CSV
    export_table_to_csv("gold.customer_agg", os.path.join(OUTPUT_DIR, "customer_agg.csv"))
    export_table_to_csv("gold.product_agg", os.path.join(OUTPUT_DIR, "product_agg.csv"))

    print("\n[PIPELINE COMPLETED] All scripts executed and CSVs downloaded successfully.")
