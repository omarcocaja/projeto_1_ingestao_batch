from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from delta import *
from pathlib import Path
import os
import pandas as pd
import pyspark
import pyspark.sql.functions as F

builder = pyspark.sql.SparkSession.builder.appName("Projeto_1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

DATALAKE_PATH = '/home/omarcocaja/Área de trabalho/portfolio/datalake'

with DAG(
    dag_id="INGEST_TRANSACTION_DATA",
    start_date=datetime(2025, 4, 10),
    schedule="0 3 * * *",
    catchup=True
) as dag:
    
    def ingest_source_to_bronze(data_processamento:str) -> None:
        SOURCE_PATH = f"{DATALAKE_PATH}/source/transaction_system/transaction_data_{data_processamento.replace('-', '_')}.csv"
        BRONZE_PATH = f"{DATALAKE_PATH}/bronze/transaction_data"
        df_source = (
            spark
            .read
            .option('header', 'true')
            .option('inferSchema', 'true')
            .csv(SOURCE_PATH)
        )

        try:
            df_bronze = DeltaTable.forPath(spark, BRONZE_PATH)
            df_bronze.toDF().limit(1)

            df_bronze.delete(f"transaction_date = '{data_processamento}'")

            (
                df_source
                .write
                .format('delta')
                .mode('append')
                .option('mergeSchema', 'true')
                .save(BRONZE_PATH)
            )
        
        except Exception as e:
            if 'DELTA_MISSING_DELTA_TABLE' in str(e):
                (
                    df_source
                    .write
                    .format('delta')
                    .option('mergeSchema', 'true')
                    .mode('overwrite')
                    .save(BRONZE_PATH)
                )
            else:
                raise e

        return BRONZE_PATH

    def ingest_bronze_to_silver() -> str:
        BRONZE_PATH = f"{DATALAKE_PATH}/bronze/transaction_data"
        SILVER_PATH = f"{DATALAKE_PATH}/silver/transaction_data"

        df_bronze = DeltaTable.forPath(spark, BRONZE_PATH)

        try:
            df_silver = DeltaTable.forPath(spark, SILVER_PATH)
            df_silver.toDF().limit(1)

            MAIOR_DATA_SILVER = (
                df_silver
                .toDF()
                .agg(
                    F.max('transaction_date')
                )
                .collect()[0][0]
            )

            df_bronze = df_bronze.toDF().filter(f"transaction_date > '{MAIOR_DATA_SILVER}'")
            
            (
                df_silver.alias('old_data')
                .merge(
                    df_bronze.alias('new_data'),
                    "old_data.transaction_id = new_data.transaction_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

        
        except Exception as e:
            if 'DELTA_MISSING_DELTA_TABLE' in str(e):
                (
                    df_bronze
                    .toDF()
                    .write
                    .format('delta')
                    .option('mergeSchema', 'true')
                    .mode('overwrite')
                    .save(SILVER_PATH)
                )
            else:
                raise e

    operator_source_to_silver = PythonOperator(
        task_id = "INGEST_DATA_FROM_SOURCE_TO_BRONZE",
        python_callable=ingest_source_to_bronze,
        provide_context=True,
        op_kwargs = {
            'data_processamento': "{{ ds }}"
        }
    )

    operator_bronze_to_silver = PythonOperator(
        task_id = "INGEST_DATA_FROM_BRONZE_TO_SILVER",
        python_callable=ingest_bronze_to_silver,
        provide_context=True,
    )

operator_source_to_silver >> operator_bronze_to_silver