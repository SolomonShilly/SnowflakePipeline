# Snowflake Data Uploader

This Python script automates the process of creating a schema, stage, and uploading multiple datasets (CSV, Excel, and Parquet files) into Snowflake tables.

## Features

- Automatically creates a new schema and stage in Snowflake.
- Detects file type and converts Excel to CSV.
- Automatically infers data types from pandas DataFrames.
- Creates corresponding tables in Snowflake.
- Uploads data to Snowflake using the `PUT` and `COPY INTO` commands.

## Requirements

- Python 3.7+
- Snowflake Python Connector
- pandas

Install dependencies using:

```bash
pip install snowflake-connector-python pandas openpyxl pyarrow

- Change connection to your snowflake destination
connection = snowflake.connector.connect(
    user="YOUR USERNAME",
    password="YOUR PASSWORD",
    account="YOUR ACCOUNT",
    warehouse="YOUR WAREHOUSE",
    database="YOUR DATABASE"
)

- Add Files you would like to add, along with the name you would like it to have in Snowflake
data = [["filename.csv", "TargetTableName"], ...]
