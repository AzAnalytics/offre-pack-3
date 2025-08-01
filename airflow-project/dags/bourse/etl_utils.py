from __future__ import annotations

import logging
from typing import Dict

import pandas as pd
import yfinance as yf
from google.cloud.exceptions import GoogleCloudError
import gcsfs                                         # pour tester l’existence du CSV

from bourse.config import BUCKET_NAME, HISTORY_LOOKBACK_DAYS
from bourse.tickers import TICKERS                          # dict {CompanyName: Ticker}

fs = gcsfs.GCSFileSystem()                           # Composer sait l’utiliser

# ------------------------------------------------------------------ #
#  LOGGING GLOBAL
# ------------------------------------------------------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s",
)

# ------------------------------------------------------------------ #
#  FONCTIONS UTILITAIRES
# ------------------------------------------------------------------ #
def _download_history(ticker: str, period: str) -> pd.DataFrame:
    """Télécharge l’historique via yfinance pour la période demandée."""
    df = yf.Ticker(ticker).history(period=period)
    if df.empty:
        raise ValueError(f"Aucune donnée renvoyée pour {ticker} sur la période {period}")
    # on enlève les colonnes inutiles si elles existent
    cols_to_drop = [c for c in ("Dividends", "Stock Splits") if c in df]
    return df.drop(columns=cols_to_drop)



def update_ticker_data(ticker_symbol: str, company_name: str) -> int:
    """
    - Si le CSV n’existe pas encore → full load (period='max')
    - Sinon → incrémental (period='<HISTORY_LOOKBACK_DAYS>d')
    Renvoie le nombre de lignes ajoutées/écrites.
    """
    # Chemin dans GCS
    gcs_path = f"gs://{BUCKET_NAME}/ticker={ticker_symbol}/data.csv"

    # --- 1. essaie de lire le CSV existant ---------------------------
    try:
        hist = pd.read_csv(gcs_path, parse_dates=["Date"])
        full_mode = False                        # fichier trouvé → incrémental
    except FileNotFoundError:
        hist = pd.DataFrame()
        full_mode = True                         # 1er run → full load

    period = "max" if full_mode else f"{HISTORY_LOOKBACK_DAYS}d"

    # --- 2. téléchargement yfinance -----------------------------------
    try:
        new_df = _download_history(ticker_symbol, period)
    except Exception as err:
        logging.warning("⚠️  %s : %s", ticker_symbol, err)
        return 0

    # --- 3. préparation des données -----------------------------------
    new_df.reset_index(inplace=True)
    new_df["Date"]        = pd.to_datetime(new_df["Date"].dt.date)
    new_df["Ticker"]      = ticker_symbol
    new_df["CompanyName"] = company_name

    # --- 4. fusion / calcul du delta -----------------------------------
    if full_mode:
        merged = new_df.sort_values("Date")
        added  = len(merged)
        logging.info("FULL  %s → %s lignes écrites", ticker_symbol, added)
    else:
        before = len(hist)
        merged = (
            pd.concat([hist, new_df], ignore_index=True)
              .drop_duplicates(subset=["Date"])
              .sort_values("Date")
        )
        added = len(merged) - before
        logging.info("INCR  %s → +%s lignes (total %s)", ticker_symbol, added, len(merged))

    # --- 5. sauvegarde vers GCS ----------------------------------------
    try:
        merged.to_csv(gcs_path, index=False)
    except GoogleCloudError as gcs_err:
        logging.error("❌  Écriture GCS échouée pour %s : %s", ticker_symbol, gcs_err)
        return 0

    return added



def update_all_tickers(tickers: Dict[str, str] | None = None) -> int:
    """
    Parcourt le dictionnaire TICKERS (ou celui fourni) et renvoie
    le total de lignes ajoutées lors de ce run.
    """
    tickers = tickers or TICKERS
    logging.info("=== Début mise à jour CSV (auto full → incr) ===")
    total = sum(update_ticker_data(sym, name) for name, sym in tickers.items())
    logging.info("=== Fin mise à jour — %s lignes ajoutées ===", total)
    return total

