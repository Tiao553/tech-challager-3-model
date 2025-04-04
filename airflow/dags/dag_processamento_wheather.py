from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.datasets import Dataset
from datetime import datetime, timedelta
from include.ml_utils import clean_data_polars, transform_features_polars
import polars as pl
import joblib

S3_PROCESSED_PATH = "s3://tech-challanger-3-prd-processing-zone-593793061865/predict-next-hour-whether/"
S3_RAW_PATH = "s3://tech-challanger-3-prd-raw-zone-593793061865/whether-minute/"
S3_MODEL_PATH = "s3://tech-challanger-3-prd-models-zone-593793061865/models/"
S3_CONN_ID = "aws-con-id"

with DAG(
    dag_id="hourly_processing",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:
    
    @task(task_id="process_hourly_data")
    def process_data(execution_date: datetime):
        """Processa dados da última hora"""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        # Listar arquivos da última hora
        files = s3_hook.list_keys(
            bucket_name="your-tech-challanger-3-prd-raw-zone-593793061865",
            prefix=f"whether-minute/{execution_date.strftime('%Y%m%dT%H')}-data.parquet"
        )
        
        # Ler e combinar dados
        df = pl.concat([
            pl.read_parquet(s3_hook.download_file(key))
            for key in files
        ])
        
        # Limpeza e transformação
        clean_df = clean_data_polars(df)
        transformed, _, _ = transform_features_polars(clean_df)
        
        # Carregar modelo mais recente
        latest_model = s3_hook.download_file(
            key=s3_hook.get_latest_mod_time(S3_MODEL_PATH).key
        )
        model = joblib.load(latest_model)
        
        # Gerar previsões
        predictions = model.predict(transformed.to_numpy())
        
        # Salvar resultados
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            transformed.with_columns(pl.Series(predictions).alias("previsoes")).write_parquet(tmp.name)
            s3_hook.load_file(
                filename=tmp.name,
                key=f"{S3_PROCESSED_PATH}{{ ts_nodash }}-results.parquet",
                replace=True
            )

    process_data()