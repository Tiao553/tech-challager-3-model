from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.datasets import Dataset
from datetime import datetime
from include.ml_utils import clean_data_polars, transform_features_polars
from sklearn.svm import SVC
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    classification_report,
    log_loss
)
import polars as pl
import joblib
import pandas as pd
import tempfile
import json

S3_TRAINING_DATA = "s3://tech-challanger-3-prd-processing-zone-593793061865/predict-next-hour-whether/"
S3_MODEL_PATH = "s3://tech-challanger-3-prd-models-zone-593793061865//models/"
S3_METRICS_PATH = "s3://tech-challanger-3-prd-models-zone-593793061865//metrics/"
S3_CONN_ID = "aws-con-id"

def save_metrics_to_s3(s3_hook, metrics, model_version):
    """Salva métricas no S3 em formato Parquet e JSON"""
    try:
        # Carregar histórico existente
        metrics_df = pl.DataFrame()
        if s3_hook.check_for_key("metrics/history.parquet", "your-bucket"):
            metrics_file = s3_hook.download_file("metrics/history.parquet")
            metrics_df = pl.read_parquet(metrics_file)

        # Adicionar novas métricas
        new_metrics = pl.DataFrame({k: [v] for k, v in metrics.items()})
        new_metrics = new_metrics.with_columns(
            pl.lit(model_version).alias("model_version"),
            pl.lit(datetime.now()).alias("timestamp")
        )
        
        updated_metrics = pl.concat([metrics_df, new_metrics])
        
        # Salvar versão completa
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            updated_metrics.write_parquet(tmp.name)
            s3_hook.load_file(
                filename=tmp.name,
                key="metrics/{{ ts_nodash }}history.parquet",
                replace=True
            )

        # Salvar última execução como JSON
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json") as tmp:
            json.dump(metrics, tmp)
            tmp.flush()
            s3_hook.load_file(
                filename=tmp.name,
                key=f"metrics/latest/{model_version}.json",
                replace=True
            )
            
    except Exception as e:
        print(f"Erro ao salvar métricas: {str(e)}")
        raise

with DAG(
    dag_id="model_retraining",
    schedule="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    
    @task(task_id="retrain_model")
    def retrain_model():
        """Retreina o modelo e armazena métricas"""
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        model_version = datetime.now().isoformat()

        # 1. Carregar e preparar dados
        files = s3_hook.list_keys(bucket_name="your-bucket", prefix="processed/")
        df = pl.concat([
            pl.read_parquet(s3_hook.download_file(key)) 
            for key in files
        ])
        
        clean_df = clean_data_polars(df)
        transformed, scaler, encoder = transform_features_polars(clean_df)
        
        # 2. Split dos dados
        X = transformed.to_numpy()
        y = clean_df['vai_chover'].to_numpy()
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        # 3. Treinar modelo
        model = SVC(probability=True)
        model.fit(X_train, y_train)

        # 4. Calcular métricas
        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:,1]
        
        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "f1": f1_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred),
            "recall": recall_score(y_test, y_pred),
            "log_loss": log_loss(y_test, y_proba),
            "classification_report": classification_report(
                y_test, y_pred, output_dict=True
            ),
            "model_version": model_version,
            "training_samples": len(X_train),
            "test_samples": len(X_test)
        }

        # 5. Salvar modelo e métricas
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Modelo
            model_path = f"{tmp_dir}/model_{model_version}.joblib"
            joblib.dump({
                'model': model,
                'scaler': scaler,
                'encoder': encoder,
                'metrics': metrics
            }, model_path)
            
            s3_hook.load_file(
                filename=model_path,
                key=f"models/{model_version}.joblib",
                replace=True
            )

            # Métricas
            save_metrics_to_s3(s3_hook, metrics, model_version)

        return metrics

    retrain_model()