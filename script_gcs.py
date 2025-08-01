import yfinance as yf
import logging
import os

# --- Configuration du Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration Globale ---
BUCKET_NAME = os.getenv("BUCKET_NAME", "datalake-yfinance-alexis-20250723")

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
    """Télécharge, formate et sauvegarde les données pour un ticker donné."""
    logging.info(f"Traitement de : {company_name} ({ticker_symbol})")
    try:
        # Utilise la méthode yf.Ticker().history() pour un format de données simple et propre
        action = yf.Ticker(ticker_symbol)
        stock_data = action.history(period="max")

        if stock_data.empty:
            logging.warning(f"Aucune donnée valide trouvée pour {ticker_symbol}.")
            return

        # Supprime les colonnes inutiles que .history() peut retourner
        if 'Dividends' in stock_data.columns:
            stock_data = stock_data.drop(columns=['Dividends'])
        if 'Stock Splits' in stock_data.columns:
            stock_data = stock_data.drop(columns=['Stock Splits'])

        # Prépare le DataFrame pour un CSV propre
        stock_data.reset_index(inplace=True)
        stock_data['Date'] = stock_data['Date'].dt.date  # Convertit en date simple
        stock_data['Ticker'] = ticker_symbol
        stock_data['CompanyName'] = company_name

        # Force les types de données pour les colonnes de texte
        stock_data['Ticker'] = stock_data['Ticker'].astype('string')
        stock_data['CompanyName'] = stock_data['CompanyName'].astype('string')

        # Construit le chemin de destination partitionné par ticker
        file_path = f"gs://{BUCKET_NAME}/ticker={ticker_symbol}/data.csv"

        # Sauvegarde le DataFrame en écrasant le fichier précédent
        stock_data.to_csv(file_path, index=False)

        logging.info(f"Succès pour {ticker_symbol}. {len(stock_data)} lignes sauvegardées dans {file_path}")

    except Exception as e:
        logging.error(f"ERREUR pour {ticker_symbol}: {e}")


def main(event, context):
    """Point d'entrée de la Cloud Function, orchestre le traitement."""
    logging.info("Démarrage du traitement séquentiel par ticker...")

    for company_name, ticker_symbol in TICKERS.items():
        process_ticker(ticker_symbol, company_name)

    logging.info("Fonction terminée.")
    return "OK"


# Permet de tester le script localement
if __name__ == "__main__":
    main(None, None)
