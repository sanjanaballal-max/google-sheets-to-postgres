# Business Performance Insights – End-to-End Data Pipeline

This project implements a **Medallion Architecture (Bronze → Silver → Gold)** data pipeline starting from **Google Sheets** raw data, flowing through **Python ETL + SQL transformations**, and ending with an interactive **Looker Studio dashboard** for analyzing **business performance at customer, product, and brand levels**.

---

## Project Overview
The goal of this project was to design and automate a **scalable data pipeline** that integrates data from multiple sources, performs cleaning & transformation, and provides **real-time insights** for business decision-making.

- **Bronze Layer:** Raw data ingestion from Google Sheets into PostgreSQL.
- **Silver Layer:** Data cleaning, standardization, and data quality checks.
- **Gold Layer:** Aggregate tables for analytics and dashboards.
- **Dashboard:** Interactive business insights with multiple slicers and KPIs.

---

## Tech Stack
- **Data Sources:** Google Sheets
- **Database:** PostgreSQL
- **ETL & Automation:** Python (pandas, psycopg2/sqlalchemy, schedule)
- **Visualization:** Looker Studio
- **Version Control:** GitHub
- **Orchestration:** Python scheduling / CRON jobs

---

## Dashboard Description

### Filters (Slicers)
- **Age group** → Filter metrics and visuals by customer age group
- **City** → Analyze metrics for specific cities
- **Year** → Filter by year for time-based analysis
- **Brand** → Focus on specific product brands

---

### Key Metrics
- **Total customers** → Total unique customers in the dataset
- **Total orders** → Total orders placed across all customers
- **Active customers** → Customers actively ordering in the selected period
- **Repeat customers** → Customers placing multiple orders
- **Average order amount** → Average order value across all orders
- **Customer satisfaction score** → Average rating across products and orders

---

### Graphs & Visuals
1. **Number of new customers over time (Line Chart)**  
   Monthly trend of new customers added over the year.

2. **Order frequency by customers (Pie Chart)**  
   Breakdown of Inactive, Active, and Repeat customers.

3. **Total orders vs. Total deliveries (Bar Chart)**  
   Monthly comparison of orders vs. successful deliveries.

4. **Total revenue by category & product name (Stacked Bar Chart)**  
   Revenue split across categories and individual products.

5. **Category by rating (Donut/Pie Chart)**  
   Distribution of product ratings across different categories.

6. **Net Revenue (Bar Chart)**  
   Comparison of total revenue and prices across products/brands.

---

## How to Run the ETL Pipeline

1. Clone this repository:
   ```bash
   git clone https://github.com/username/project-name.git
   cd project-name
   ```

2. Create a virtual environment & install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. Setup environment variables in `.env`:
   ```
   DB_HOST=
   DB_USER=
   DB_PASSWORD=
   DB_NAME=
   ```

4. Run the complete pipeline:
   ```bash
   python etl.py all
   ```

---

## Repository Structure
```
/bronze_inputs/      # Raw CSV files from Google Sheets
/sql/                # SQL scripts for transformations
/src/                # Python ETL scripts
/eda/                # Exploratory Data Analysis scripts/notebooks
/docs/               # Data dictionary, lineage diagram, runbook
/logs/               # Pipeline run logs
/config/             # Config files, env templates
README.md
Runbook.md
```

---

## Deliverables
- **Automated ETL pipeline** with Bronze → Silver → Gold flow
- **Data Quality Checks** with logs and audit trail
- **EDA notebook** with insights and visualizations
- **Looker Studio dashboard** with interactive KPIs & slicers
- **Documentation** including data dictionary & lineage diagram

---
