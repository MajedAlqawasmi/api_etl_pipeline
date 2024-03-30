import pandas as pd
import requests
import json
from datetime import datetime
import datetime
from google.cloud import bigquery


SERVICE_ACCOUNT_USERNAME = 'service_account_username'
SERVICE_ACCOUNT_SECRET = 'service_account_secret'
DATASET_ID = 'dataset_id'
TABLE_ID = 'table_id'

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Checking if dataframe is empty
    if df.empty:
        print("No cohort created. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['id']).is_unique:
        pass
    else:
        raise ValueError("Primary Key check is violated")

    # Checking for nulls
    if df.isnull().values.any():
        raise ValueError("Null values found")

    # Checking that all created are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    created = df["created_at"].tolist()
    for created_at in created:
        if datetime.datetime.strptime(created_at, '%Y-%m-%d') != yesterday:
            raise ValueError("At least one of the returned cohorts does not have a yesterday's timestamp")

    return True

def run_mixpanel_etl():
    # Extracting part of the ETL process
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Basic {SERVICE_ACCOUNT_USERNAME}:{SERVICE_ACCOUNT_SECRET}"
    }
    
    # Converting time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Downloading all records created in the last 24 hours      
    r = requests.get("https://eu.mixpanel.com/api/query/cohorts/list".format(time=yesterday_unix_timestamp), headers = headers)
    if r.status_code == 200:
        data = r.json()

        # Extracting relevant data from the API response
        transformed_data_list = []
        for item in data:
            transformed_data = {
                "count": int(item["count"]),
                "is_visible": bool(item["is_visible"]),
                "description": str(item["description"]),
                "created_at": datetime.utcfromtimestamp(item["created"]),
                "project_id": int(item["project_id"]),
                "id": int(item["id"]),
                "name": str(item["name"])
            }
            transformed_data_list.append(transformed_data)

        # Creating DataFrame from the list of transformed data
        cohort_df = pd.DataFrame(transformed_data_list)
    else:
        print(f"Error: API request failed with status code {r.status_code}.")
    
    # Validating
    if check_if_valid_data(cohort_df):
        print("Data valid, proceed to Load stage")

    # Loading

    client = bigquery.Client()

    # Getting table reference
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    table = client.get_table(table_ref)

    # Preparing rows to insert
    rows_to_insert = cohort_df.to_records(index=False)

    # Inserting or updating data into BigQuery table
    errors = client.insert_rows(table, rows_to_insert)
    if errors:
        raise ValueError(f"Error inserting rows into BigQuery: {errors}")