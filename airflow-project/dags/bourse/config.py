"""
bourse/config.py
Constantes partagées par le pipeline bourse (DAG, fonctions ETL, etc.)
"""

import os

# ------------------------------------------------------------------
#  Infos GCP / BigQuery
# ------------------------------------------------------------------
PROJECT_ID = "azanalytics-data-lake-raw"      # ID de ton projet
LOCATION   = "EU"                             # Région BigQuery / Composer

# ------------------------------------------------------------------
#  Stockage GCS
# ------------------------------------------------------------------
# Bucket où les CSV sont partitionnés par ticker (ticker=*/data.csv)
BUCKET_NAME = os.getenv(
    "BUCKET_NAME",
    "datalake-yfinance-alexis-20250723",
)

# ------------------------------------------------------------------
#  Dataset & tables BigQuery  (zone unique : yfinance_data)
# ------------------------------------------------------------------
DATASET     = "yfinance_data"   # dataset unique (Raw + Silver)
RAW_TABLE   = "stocks_raw"      # table brute chargée depuis GCS
SILVER_TABLE = "stocks_silver"  # table nettoyée / MERGE incrémental

# ------------------------------------------------------------------
#  Divers
# ------------------------------------------------------------------
HISTORY_LOOKBACK_DAYS = 5       # nb de jours téléchargés via yfinance
