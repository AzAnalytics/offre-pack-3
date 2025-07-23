import yfinance as yf
import pandas as pd
from datetime import datetime

# --- Configuration ---
# Le nom de votre bucket GCS.
BUCKET_NAME = "datalake-yfinance-alexis-20250723"

# La liste des symboles (tickers) que vous voulez récupérer.
# Vous pouvez modifier cette liste à votre guise.
TICKERS = ["AAPL", "MSFT", "GOOGL", "TSLA", "NVDA", "BTC-USD"]

# La période pour les données historiques.
START_DATE = "2020-01-01"
END_DATE = datetime.now().strftime("%Y-%m-%d") # Jusqu'à aujourd'hui

# --- Logique du script ---
print("Démarrage de la récupération des données pour GCS...")

for ticker in TICKERS:
    print(f"Traitement du ticker : {ticker}")
    try:
        # 1. Télécharger les données depuis yfinance
        stock_data = yf.download(ticker, start=START_DATE, end=END_DATE)

        if stock_data.empty:
            print(f"  -> Aucune donnée trouvée pour {ticker}.")
            continue

        # 2. Construire le chemin de destination dans GCS
        # Le préfixe "gs://" est géré automatiquement par la bibliothèque gcsfs.
        file_path = f"gs://{BUCKET_NAME}/stocks_data/ticker={ticker}/data.parquet"

        # 3. Sauvegarder le DataFrame directement dans GCS au format Parquet
        stock_data.to_parquet(file_path, engine='pyarrow')

        print(f"  -> Données sauvegardées avec succès dans : {file_path}")

    except Exception as e:
        print(f"  -> Une erreur est survenue pour {ticker}: {e}")

print("Script terminé.")