# Message pour savoir que le script démarre
echo "Préparation de l'environnement et lancement du script Python..."

# 1. Si tu veux utiliser une clé GCP, assure-toi que le fichier est accessible sur le worker.
# Sinon, dans Cloud Composer, on préfère gérer l'auth via le service account du worker.
# Supprimer la ligne suivante si tu es dans Composer avec un SA configuré :
# export GOOGLE_APPLICATION_CREDENTIALS="/home/airflow/gcs/data/azanalytics-data-lake-raw-742f244fd203.json"

# 2. Lancer le script Python avec l'interpréteur Python par défaut du container Airflow
python /home/airflow/gcs/dags/script_yfinance.py

echo "Script terminé."