from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "careful-span-456811-f9-d8e96e6e391e.json"

# Set project and table info
project_id = "careful-span-456811-f9"
dataset_id = "staging_db"
table_id = "stg_city"

client = bigquery.Client(project=project_id)

# Query
query = """
WITH base AS (
    SELECT * FROM `careful-span-456811-f9.pagila_productionpublic.city`
),
final AS (
    SELECT 
        city_id,
        city as city_name,
        country_id,
        last_update as city_last_update
    
    FROM base
)
SELECT * FROM final
"""

df = client.query(query).to_dataframe()

# Define schema
schema = [
    bigquery.SchemaField("city_id", "INTEGER"),
    bigquery.SchemaField("city_name", "STRING"),
    bigquery.SchemaField("country_id", "INTEGER"),
    bigquery.SchemaField("city_last_update", "DATETIME")
]

# Full table id
full_table_id = f"careful-span-456811-f9.staging_db.stg_city"

# Check if table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Upload to BigQuery
if table_exists(client, full_table_id):
    to_gbq(df, f"staging_db.stg_city", project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} overwritten.")
else:
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    print(f"Table {full_table_id} created and loaded.")
