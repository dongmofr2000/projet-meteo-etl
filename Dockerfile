# Utilise une image Python légère comme base
FROM python:3.11-slim

# Définit le répertoire de travail dans le conteneur
WORKDIR /app

# Copie le fichier de dépendances et les installe
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ÉTAPE CRUCIALE AJOUTÉE : Copie le dossier 'data'
# Assurez-vous que le dossier 'data' existe bien dans le même répertoire que ce Dockerfile.
COPY data/ ./data/ 

# Copie le reste du code de l'application (incluant etl_meteo.py)
COPY . .

# Commande par défaut (sera remplacée par la commande 'command' dans docker-compose.yml)
CMD ["python", "etl_meteo.py"]
