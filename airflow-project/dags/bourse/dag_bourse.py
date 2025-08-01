"""
bourse/dag_bourse.py
DAG orchestrant :
1) Extraction CSV depuis yfinance → GCS
2) Chargement GCS → BigQuery (table RAW)
3) MERGE incrémental vers la table SILVER
4) Contrôle que la valeur de J-1 existe
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
)


from bourse.etl_utils import update_all_tickers
from bourse.config   import (
    PROJECT_ID,
    BUCKET_NAME,
    DATASET,
    RAW_TABLE,
    SILVER_TABLE,
    LOCATION,
)


# ------------------------------------------------------------------ #
#   DAG
# ------------------------------------------------------------------ #
with DAG(
    dag_id="bourse_daily_update",
    start_date=datetime(2025, 7, 31),
    schedule_interval="0 6 * * 2-6",      # 06 h UTC = 08 h CEST
    catchup=False,
    tags=["bourse", "yfinance", "v2_modulaire"],
) as dag:

    # 1) Extraction / (full ou incr) vers GCS
    extract_csv = PythonOperator(
        task_id="update_tickers",
        python_callable=update_all_tickers,
    )

    # 2) GCS → table RAW (écrasée chaque jour)
    load_raw = GCSToBigQueryOperator(
        task_id="load_raw",
        bucket=BUCKET_NAME,
        location=LOCATION,
        source_objects=["ticker=*/data.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.{RAW_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )

    # 2 bis) **Bootstrap** : crée la table SILVER si elle n'existe pas
    create_silver_if_absent = BigQueryInsertJobOperator(
        task_id="create_silver_if_absent",
        location=LOCATION,  # EU
        configuration={
            "query": {
                "query": f"""
                    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET}.{SILVER_TABLE}` (
                        Date        DATE,
                        Open        FLOAT64,
                        High        FLOAT64,
                        Low         FLOAT64,
                        Close       FLOAT64,
                        Volume      INT64,
                        Ticker      STRING,
                        CompanyName STRING
                    )
                    CLUSTER BY Ticker
                    OPTIONS (description = 'Table Silver non partitionnée, cluster sur Ticker');
                """,
                "useLegacySql": False,
            }
        },
    )

    # 3) MERGE incrémental RAW → SILVER
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
                        WHERE Date > (
                            SELECT IFNULL(MAX(Date), DATE '1900-01-01')
                            FROM `{PROJECT_ID}.{DATASET}.{SILVER_TABLE}`
                        )
                    ) AS src
                    ON  tgt.Date   = src.Date
                    AND tgt.Ticker = src.Ticker
                    WHEN NOT MATCHED THEN INSERT ROW;
                """,
                "useLegacySql": False,
            },
        },
    )

    # 4) Contrôle que J-1 est bien présent
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
    extract_csv >> load_raw >> create_silver_if_absent >> merge_silver >> check_yesterday
