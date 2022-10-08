import os
import logging


from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

dataset_file = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

#function to connect to s3 bucket
def ingest_to_s3(bucket_name:str, key:str, filename:str):
    hook = S3Hook('s3_conn')
    hook.load_file(bucket_name=bucket_name, key=key, filename=filename)


#initializing the ingestion workflow DAG
ingestion_workflow = DAG(
    "data_ingestion_s3_dag",  
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021,1,1)
)
#downloading the data 
with ingestion_workflow:
    download_task = BashOperator(
        task_id = "download_data",
        bash_command = f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

#ingesting into s3 bucket
    ingest_task = PythonOperator(
        task_id = 'ingest_to_s3',
        python_callable = ingest_to_s3,
        op_kwargs = {
            "bucket_name": "hbfirst",
            "key": f"fhv_data/{dataset_file}",
            "filename": f"{OUTPUT_FILE_TEMPLATE}"
        }
    )

    download_task >> ingest_task
