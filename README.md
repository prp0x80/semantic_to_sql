# Semantic layer to SQL

Python program to convert query and semantic layer input into a SQL query

## `query_builder.py`
Use this function to build and execute the query on collection of query-semantic layer pairs defined in `data.py`.

### Setup
You will need to add a `.env` file with:
* `DEFAULT_DATASET` - the BigQuery dataset you want to run queries on
* `SERVICE_ACCOUNT_JSON_PATH` - path to the JSON key file for the BigQuery service account that has permissions to run jobs
* `MAX_RESULTS` - limit the number of rows to fetch

Install dependencies with poetry:
1. Run `poetry install --no-root`

### Usage

Either run with `poetry run python query_builder.py`, or import into your code

