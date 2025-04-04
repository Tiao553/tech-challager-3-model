import polars as pl
import numpy as np
from sklearn.preprocessing import MinMaxScaler
from category_encoders import CatBoostEncoder
from typing import Tuple

def clean_data_polars(df: pl.DataFrame) -> pl.DataFrame:
    """Limpeza de dados usando polars"""
    return df.drop([
        "vai_nevar", "chance_neve", "neve_cm", "pais", "localizacao",
        "regiao", "fuso_horario", "nascer_do_sol", "por_do_sol",
        "nascer_da_lua", "por_da_lua", "chance_chuva", "condicao", "data"
    ]).with_columns(
        pl.col("hora").str.strptime(pl.Datetime, "%H:%M").dt.hour().alias("hora"),
        pl.col("hora").map_elements(
            lambda x: np.sin(2 * np.pi * x / 24), 
            return_dtype=pl.Float64).alias("hora_seno"),
        pl.col("hora").map_elements(
            lambda x: np.cos(2 * np.pi * x / 24), 
            return_dtype=pl.Float64).alias("hora_cosseno")
    )

def transform_features_polars(df: pl.DataFrame) -> Tuple[pl.DataFrame, MinMaxScaler, CatBoostEncoder]:
    """Transformação de features com suporte a polars"""
    encoder = CatBoostEncoder(cols=['direcao_vento', 'fase_da_lua'], random_state=7)
    scaler = MinMaxScaler()

    # Converter para pandas para transformação
    pandas_df = df.to_pandas()
    y = pandas_df.pop('vai_chover')
    
    # Aplicar transformações
    encoded = encoder.fit_transform(pandas_df, y)
    scaled = scaler.fit_transform(encoded)
    
    return pl.from_pandas(scaled), scaler, encoder

def prepare_metrics_report(metrics: dict) -> pl.DataFrame:
    """Prepara relatório de métricas para armazenamento"""
    report = {
        'timestamp': [datetime.now().isoformat()],
        'model_version': [metrics['model_version']],
        'accuracy': [metrics['accuracy']],
        'f1_score': [metrics['f1']],
        'precision': [metrics['precision']],
        'recall': [metrics['recall']],
        'log_loss': [metrics['log_loss']],
        'training_samples': [metrics['training_samples']],
        'test_samples': [metrics['test_samples']]
    }
    
    # Adicionar métricas do classification report
    cls_report = metrics['classification_report']
    for label in cls_report:
        if label.isdigit():
            for metric, value in cls_report[label].items():
                report[f"class_{label}_{metric}"] = [value]
    
    return pl.DataFrame(report)