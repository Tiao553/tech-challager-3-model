from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.http.hooks.http import HttpHook
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import polars as pl
import tempfile

S3_RAW_PATH = "s3://tech-challanger-3-prd-raw-zone-593793061865/whether-minute/"
S3_CONN_ID = "aws-con-id"
API_CONN_ID = "API_CONN_ID"

with DAG(
    dag_id="raw_data_ingestion",
    schedule="* * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    @task(task_id="extract_data")
    def extract_data(execution_date: datetime):
        """Extrai dados da API e salva em formato parquet"""
        http_hook = HttpHook(method="GET", http_conn_id=API_CONN_ID)
        
        params = {
            "q": "São Paulo",
            "dt": execution_date.strftime("%Y-%m-%d")
        }
        
        response = http_hook.run(endpoint="/v1/history.json", data=params)
        response.raise_for_status()
        
        df = pl.from_dicts([response.json()])
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            df.write_parquet(tmp.name)
            return tmp.name

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        aws_conn_id=S3_CONN_ID,
        filename="{{ ti.xcom_pull(task_ids='extract_data') }}",
        dest_key=f"{S3_RAW_PATH}{{ ts_nodash }}-data.parquet",
        replace=True,
        outlets=[Dataset(S3_RAW_PATH)]
    )

    extract_data() >> upload_to_s3