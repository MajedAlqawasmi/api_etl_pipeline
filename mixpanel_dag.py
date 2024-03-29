from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from main import run_mixpanel_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2024, 4, 1),
    'email': ['majed.alqawasmi@up42.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'mixpanel_dag',
    default_args=default_args,
    description='API extracting from Mixpanel and loading to Bigquery',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=run_mixpanel_etl,
    dag=dag,
)

run_etl