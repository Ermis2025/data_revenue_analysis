from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "careful-span-456811-f9-d8e96e6e391e.json"

# Set project and table info
project_id = "careful-span-456811-f9"
dataset_id = "staging_db"
table_id = "stg_category"

client = bigquery.Client(project=project_id)

# Query
query = """
WITH base AS (
    SELECT * FROM `careful-span-456811-f9.pagila_productionpublic.category`
),
final AS (
    SELECT 
        category_id,
        name as category_name,
        last_update as category_last_update
    
    FROM base
)
SELECT * FROM final
"""

df = client.query(query).to_dataframe()

# Define schema
schema = [
    bigquery.SchemaField("category_id", "INTEGER"),
    bigquery.SchemaField("category_name", "STRING"),
    bigquery.SchemaField("category_last_update", "DATETIME")
]

# Full table id
full_table_id = f"careful-span-456811-f9.staging_db.stg_category"

# Check if table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Upload to BigQuery
if table_exists(client, full_table_id):
    to_gbq(df, f"staging_db.stg_category", project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} overwritten.")
else:
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    print(f"Table {full_table_id} created and loaded.")
