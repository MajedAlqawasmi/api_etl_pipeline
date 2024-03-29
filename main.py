import pandas as pd 
import pandas as pd
import requests
import json
from datetime import datetime
import datetime
from google.cloud import bigquery
from retrying import retry


def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Checking if dataframe is empty
    if df.empty:
        print("No cohort created. Finishing execution")
        return False 

    # Primary Key Check
    if pd.Series(df['id']).is_unique:
        pass
    else:
        raise Exception("Primary Key check is violated")

    # Checking for nulls
    if df.isnull().values.any():
        raise Exception("Null values found")

    # Checking that all created are of yesterday's date
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # created = df["timestamp"].tolist()
    # for timestamp in created:
        # if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            # raise Exception("At least one of the returned cohorts does not have a yesterday's timestamp")

    # return True

def run_mixpanel_etl():
    service_account_username = 'service_account_username'
    service_account_secret = 'service_account_secret'

    # Extracting part of the ETL process
 
    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : f"Basic {service_account_username}:{service_account_secret}"
    }
    
    # Converting time to Unix timestamp in miliseconds      
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Downloading all records created in the last 24 hours      
    r = requests.get("https://eu.mixpanel.com/api/query/cohorts/list".format(time=yesterday_unix_timestamp), headers = headers)

    data = r.json()

    name = []
    id = []
    created = []
    count = []
    is_visible = []
    description = []
    project_id = []
        
    # Preparing a dictionary in order to turn it into a pandas dataframe below       
    cohort_dict = {
        "cohort_name" : name,
        "id" : id,
        "created_at": created,
        "count" : count,
        "is_visible" : is_visible,
        "description" : description,
        "project_id" : project_id
    }

    cohort_df = pd.DataFrame(cohort_dict, columns = ["cohort_name", "id", "created_at", "count", "is_visible", "description", "project_id"])
    
    # Validating
    if check_if_valid_data(cohort_df):
        print("Data valid, proceed to Load stage")

    # Loading

    # Initializing BigQuery client
    client = bigquery.Client()

    # Defining dataset and table IDs
    dataset_id = 'dataset_id'
    table_id = 'table_id'

    # Getting table reference
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    # Preparing rows to insert
    rows_to_insert = cohort_df.to_records(index=False)

    # Inserting or updating data into BigQuery table
    errors = client.insert_rows(table, rows_to_insert)
    if errors:
        raise Exception(f"Error inserting rows into BigQuery: {errors}")

    


    # Job scheduling 
    
    # For the scheduling in Airflow, refer to files in the dag file