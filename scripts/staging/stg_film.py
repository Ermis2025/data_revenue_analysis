from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "careful-span-456811-f9-d8e96e6e391e.json"

# Set project and table info
project_id = "careful-span-456811-f9"
dataset_id = "staging_db"
table_id = "stg_film"

client = bigquery.Client(project=project_id)

# Query
query = """
WITH base AS (
    SELECT * FROM `careful-span-456811-f9.pagila_productionpublic.film`
),
final AS (
    SELECT 
        film_id,
        title as film_title,
        description as film_description,
        language_id as film_language_id,
        original_language_id as film_original_language_id,
        rental_duration as film_rental_duration,
        rental_rate as film_rental_rate,
        length as film_length,
        replacement_cost as film_replacement_cost,
        rating as film_rating,
        last_update as film_last_update,
        special_features as film_special_features,
        fulltext as film_fulltext
    
    FROM base
)
SELECT * FROM final
"""

df = client.query(query).to_dataframe()
# Ensure correct data types
df["film_rental_rate"] = df["film_rental_rate"].astype(float)
df["film_replacement_cost"] = df["film_replacement_cost"].astype(float)


# Define schema
schema = [
    bigquery.SchemaField("film_id", "INTEGER"),
    bigquery.SchemaField("film_title", "STRING"),
    bigquery.SchemaField("film_description", "STRING"),
    bigquery.SchemaField("film_language_id", "INTEGER"),
    bigquery.SchemaField("film_original_language_id", "INTEGER"),
    bigquery.SchemaField("film_rental_duration", "INTEGER"),
    bigquery.SchemaField("film_rental_rate", "FLOAT"),
    bigquery.SchemaField("film_length", "INTEGER"),
    bigquery.SchemaField("film_replacement_cost", "FLOAT"),
    bigquery.SchemaField("film_rating", "STRING"),
    bigquery.SchemaField("film_last_update", "DATETIME"),
    bigquery.SchemaField("film_special_features", "STRING"),
    bigquery.SchemaField("film_fulltext", "STRING")
]

# Full table id
full_table_id = f"careful-span-456811-f9.staging_db.stg_film"

# Check if table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Upload to BigQuery
if table_exists(client, full_table_id):
    to_gbq(df, f"staging_db.stg_film", project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} overwritten.")
else:
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()
    print(f"Table {full_table_id} created and loaded.")
