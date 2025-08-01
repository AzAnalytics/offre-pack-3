"""
bourse/script_finance.py
DAG unique :
1) Extraction → GCS (full la 1ʳᵉ fois, incrément ensuite)
2) Load CSV → BigQuery RAW
3) MERGE vers table SILVER
4) Vérification que la veille est présente
"""

# --------------------- imports & config --------------------- #
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)

# modules internes
from bourse.config import (
    PROJECT_ID,
    BUCKET_NAME,
    DATASET,
    RAW_TABLE,
    SILVER_TABLE,
    LOCATION,
)
from bourse.etl_utils import update_all_tickers          # logique FULL / INCR auto

# --------------------- DAG --------------------- #
with DAG(
    dag_id="bourse_daily_update",
    start_date=datetime(2025, 7, 31),
    schedule_interval="0 6 * * 2-6",      # 06 h UTC = 08 h CEST
    catchup=False,
    tags=["bourse", "yfinance", "v2_autoinit"],
) as dag:

    # 1. Extraction CSV vers GCS
    extract_csv = PythonOperator(
        task_id="update_tickers_task",
        python_callable=update_all_tickers,      # gère full / incrément
    )

    # 2. Chargement CSV → table RAW (écrasement)
    load_raw = GCSToBigQueryOperator(
        task_id="load_raw_to_bq",
        bucket=BUCKET_NAME,
        location=LOCATION,
        source_objects=["ticker=*/data.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.{RAW_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    # 3. MERGE incrémental RAW → SILVER
    merge_silver = BigQueryInsertJobOperator(
        task_id="merge_silver",
        location=LOCATION,
        configuration={
            "query": {
                "query": f"""
                MERGE `{PROJECT_ID}.{DATASET}.{SILVER_TABLE}` AS tgt
                USING (
                  SELECT *
                  FROM `{PROJECT_ID}.{DATASET}.{RAW_TABLE}`
                  WHERE Date >
                        COALESCE((SELECT MAX(Date)
                                  FROM `{PROJECT_ID}.{DATASET}.{SILVER_TABLE}`),
                                 DATE '1900-01-01')
                ) src
                ON  tgt.Date   = src.Date
                AND tgt.Ticker = src.Ticker
                WHEN NOT MATCHED THEN
                  INSERT ROW;
                """,
                "useLegacySql": False,
            }
        },
    )

    # 4. Contrôle : la veille est bien présente
    check_yesterday = BigQueryCheckOperator(
        task_id="check_yesterday",
        location=LOCATION,
        sql=f"""
          SELECT 1
          FROM `{PROJECT_ID}.{DATASET}.{SILVER_TABLE}`
          WHERE Date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
          LIMIT 1
        """,
        use_legacy_sql=False,
    )

    # Dépendances
    extract_csv >> load_raw >> merge_silver >> check_yesterday
