from google.cloud import bigquery
import pandas as pd
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "careful-span-456811-f9-d8e96e6e391e.json"

# Set project and table info
project_id = "careful-span-456811-f9"
dataset_id = "reporting_db"
table_id = "rep_revenue_per_customer_and_period"

client = bigquery.Client(project=project_id)

# Custom query
query = """WITH film_filtered AS (
    SELECT film_id
    FROM `careful-span-456811-f9.staging_db.stg_film`
    WHERE film_title != 'GOODFELLAS SALUTE'
),
valid_payments AS (
    SELECT 
        p.payment_id,
        p.customer_id,
        c.customer_first_name,
        c.customer_last_name,
        p.payment_amount,
        p.payment_date
    FROM `careful-span-456811-f9.staging_db.stg_payment` p
    JOIN `careful-span-456811-f9.staging_db.stg_customer` c ON p.customer_id = c.customer_id
    JOIN `careful-span-456811-f9.staging_db.stg_rental` r ON p.rental_id = r.rental_id
    JOIN `careful-span-456811-f9.staging_db.stg_inventory` i ON r.inventory_id = i.inventory_id
    JOIN film_filtered f ON i.film_id = f.film_id
)
SELECT
    customer_id,
    customer_first_name,
    customer_last_name,
    DATE(payment_date) AS day,
    FORMAT_DATE('%B', DATE(payment_date)) AS month,
    EXTRACT(YEAR FROM payment_date) AS year,
    SUM(payment_amount) AS revenue
FROM valid_payments
GROUP BY customer_id, customer_first_name, customer_last_name, day, month, year
ORDER BY customer_id, day"""

df = client.query(query).to_dataframe()

# Define schema
schema = [
    bigquery.SchemaField("day", "DATE"),
    bigquery.SchemaField("month", "STRING"),
    bigquery.SchemaField("year", "INT64"),
    bigquery.SchemaField("revenue", "FLOAT"),
]

# Full table ID
full_table_id = f"careful-span-456811-f9.reporting_db.rep_revenue_per_customer_and_period"

# Check if table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Upload to BigQuery
if table_exists(client, full_table_id):
    df.to_gbq(f"reporting_db.rep_revenue_per_customer_and_period", project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} overwritten.")
else:
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    print(f"Table {full_table_id} created and loaded.")
