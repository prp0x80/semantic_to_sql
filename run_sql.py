"""
File: run_sql.py
Description: A script to run a SQL snippet on BigQuery

Author: Liam Garrison <liam.garrison@fluenthq.com>
Source: https://github.com/channelhq/tech-test
"""

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from tabulate import tabulate
import json
from dotenv import load_dotenv
import os

load_dotenv()

# Dummy service account JSON - Replace this with your actual service account JSON path
SERVICE_ACCOUNT_JSON_PATH = os.getenv("SERVICE_ACCOUNT_JSON_PATH")

# BigQuery Default Dataset - Replace this with your actual dataset name
DEFAULT_DATASET = os.getenv("DEFAULT_DATASET")

# Limit the number of rows to fetch
MAX_RESULTS = os.getenv("MAX_RESULTS") or 10

if SERVICE_ACCOUNT_JSON_PATH is None:
    raise ValueError(
        "SERVICE_ACCOUNT_JSON_PATH is not set in the environment variables.")

if DEFAULT_DATASET is None:
    raise ValueError(
        "DEFAULT_DATASET is not set in the environment variables.")


def query_bigquery(sql_query: str):
    """
    Sends a SQL query to BigQuery, fetches the first 100 rows and total row count,
    and prints them in a formatted table.

    Args:
        sql_query (str): The SQL query string to execute.
    """
    try:

        with open(SERVICE_ACCOUNT_JSON_PATH) as f:
            service_account_json = f.read()

        # Create credentials from the embedded JSON string
        credentials = Credentials.from_service_account_info(
            json.loads(service_account_json)
        )

        # Initialize BigQuery client
        client = bigquery.Client(
            credentials=credentials, project=credentials.project_id
        )

        # Configure the job with the default dataset
        job_config = bigquery.QueryJobConfig(default_dataset=DEFAULT_DATASET)

        # Execute the query
        query_job = client.query(sql_query, job_config=job_config)
        result = query_job.result(
            max_results=int(MAX_RESULTS)
        )  # Waits for job to complete

        # Get total rows
        total_rows = result.total_rows

        rows = list(result)

        # Print results
        if rows:
            print(f"Total Rows: {total_rows}")
            print(f"First {MAX_RESULTS} Rows:")
            headers = rows[0].keys()  # Get column headers
            data = [
                dict(row).values() for row in rows
            ]  # Convert rows to list of values
            return tabulate(data, headers=headers, tablefmt="grid")
        else:
            print("Query returned no results.")

    except Exception as e:
        print(f"An error occurred: {e}")


# Example Usage
if __name__ == "__main__":
    sample_query = """
    SELECT * 
    FROM orders
    """
    query_bigquery(sample_query)
