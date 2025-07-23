import yfinance as yf
import pandas as pd
import logging
import os

# --- Configuration du Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration Globale ---
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake-yfinance-alexis-20250723")
GCS_PREFIX = os.getenv("GCS_PREFIX", "stocks_data")

TICKERS = {
    "LVMH Moët Hennessy - Louis Vuitton, Société Européenne": "MC.PA",
    "Hermès International Société": "RMS.PA",
    "L'Oréal S.A.": "OR.PA",
    "Christian Dior SE": "CDI.PA",
    "TotalEnergies SE": "TTE.PA",
    "Airbus SE": "AIR.PA",
    "Schneider Electric S.E.": "SU.PA",
    "Sanofi": "SAN.PA",
    "L'Air Liquide S.A.": "AI.PA",
    "EssilorLuxottica Société anonyme": "EL.PA",
    "Safran SA": "SAF.PA",
    "AXA SA": "CS.PA",
    "Vinci SA": "DG.PA",
    "BNP Paribas SA": "BNP.PA",
    "Dassault Systèmes SE": "DSY.PA",
    "Kering SA": "KER.PA",
    "Danone S.A.": "BN.PA"
}

def process_ticker(ticker_symbol, company_name):
    """Télécharge, nettoie et sauvegarde les données pour un ticker."""
    logging.info(f"Traitement de : {company_name} ({ticker_symbol})")
    try:
        stock_data = yf.download(
            ticker_symbol,
            period="max",
            progress=False,
            auto_adjust=True
        )

        # Nettoyage
        stock_data.dropna(inplace=True)

        # Aplatir les colonnes multi-index si besoin
        if isinstance(stock_data.columns, pd.MultiIndex):
            stock_data.columns = stock_data.columns.get_level_values(-1)

        # Supprimer lignes parasites (comme ,AI.PA,AI.PA,AI.PA...)
        stock_data = stock_data[
            ~stock_data.apply(lambda row: row.astype(str).str.fullmatch(ticker_symbol).all(), axis=1)]

        if stock_data.empty:
            logging.warning(f"Aucune donnée trouvée ou valide pour {ticker_symbol}.")
            return

        stock_data.reset_index(inplace=True)
        stock_data['Ticker'] = ticker_symbol
        stock_data['CompanyName'] = company_name

        # Définir le chemin du fichier CSV
        file_path = f"gs://{BUCKET_NAME}/{GCS_PREFIX}/ticker={ticker_symbol}/{ticker_symbol}.csv"

        # Vérifie si le fichier existe déjà
        file_exists = os.path.exists(file_path)

        # Sauvegarde sans répéter l'entête si append
        stock_data.to_csv(file_path, index=False, header=not file_exists)

        logging.info(f"Succès pour {ticker_symbol}. {len(stock_data)} lignes sauvegardées.")

    except Exception as e:
        logging.error(f"ERREUR pour {ticker_symbol}: {e}")

def main(event, context):
    """Point d'entrée de la Cloud Function, orchestre le traitement."""
    logging.info("Démarrage du traitement séquentiel...")

    for company_name, ticker_symbol in TICKERS.items():
        process_ticker(ticker_symbol, company_name)

    logging.info("Fonction terminée.")
    return "OK"

# Permet de tester le script localement
if __name__ == "__main__":
    main(None, None)