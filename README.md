Projet Météo E-T-L (Extraction, Transformation, Chargement)
Ce projet Python implémente un pipeline E-T-L pour unifier et normaliser des données météorologiques provenant de sources hétérogènes (Weather Underground en CSV et Infoclimat en JSON) avant de les charger dans une base de données MongoDB pour l'analyse.

🎯 Objectif du Pipeline
L'objectif principal est de résoudre les défis d'hétérogénéité des données en :

Unifiant les Schémas : Harmoniser les colonnes de chaque source dans un seul schéma cible.

Standardisant les Unités : Convertir les données de la source Weather Underground (unités Impériales) en unités Métrique (Celsius, hPa, m/s, mm).

🛠️ Logique de Transformation des Données (E-T-L)
Le script principal (etl_meteo.py) exécute les phases suivantes :

Phase 1: Extraction et Transformation des Fichiers CSV (Weather Underground)
Les fichiers CSV sont extraits, mais nécessitent une normalisation importante :

Colonne Source (Impériale)	Transformation appliquée	Colonne Cible (Métrique)
Temperature (°F)	(F−32)× 
9
5
​
 	temperature_c
Pressure (inHg)	×33.8638	pression_hpa
Speed (mph)	×0.44704	vent_vitesse_ms
Precip. Accum. (in)	×25.4	pluie_accum_mm
Date + Time	Concaténation	date_heure_utc

Exporter vers Sheets
Phase 2: Extraction et Transformation du Fichier JSON (Infoclimat)
Le fichier JSON est en unités Métriques, la transformation est donc plus légère et se concentre sur l'harmonisation du schéma cible :

Champ Source (JSON)	Transformation appliquée	Colonne Cible
vent_moyen (km/h)	÷3.6	vent_vitesse_ms
pluie_1h ou pluie_3h	Sélection de la valeur	pluie_accum_mm
dh_utc	Formatage de date	date_heure_utc

Exporter vers Sheets
Phase 3: Chargement et Audit (MongoDB)
Après l'unification, tous les enregistrements sont insérés dans la collection donnees_horaires de la base de données cible.

Schéma Cible Unifié :

date_heure_utc (String)

temperature_c (Float)

humidite_pct (Int)

pression_hpa (Float)

vent_vitesse_ms (Float)

pluie_accum_mm (Float)

id_station (String)

source_donnees (String: 'Weather Underground' ou 'Infoclimat')

🚀 Comment Exécuter le Script et les Tests
1. Prérequis
Python 3.x

MongoDB (doit être en cours d'exécution sur mongodb://localhost:27017/)

2. Configuration de l'Environnement
Le projet utilise un environnement virtuel pour garantir la reproductibilité.

Créer et Activer l'environnement virtuel :

PowerShell

python -m venv venv
# Activer pour la session PowerShell (nécessite l'autorisation temporaire)
Set-ExecutionPolicy RemoteSigned -Scope Process
.\venv\Scripts\activate
Installer les dépendances :

PowerShell

pip install -r requirements.txt
3. Exécuter le Pipeline E-T-L
Pour exécuter le pipeline et charger les données dans la base de données configurée (meteo_projet), lancez :

PowerShell

python etl_meteo.py
4. Automatisation des Tests (Validation d'Intégrité)
Le script de test (test_etl_meteo.py) automatise la vérification de l'intégrité après la migration.

Il crée une base de données de test temporaire (meteo_projet_TEST).

Il exécute le pipeline E-T-L complet sur cette base de données.

Il lance 4 tests critiques (nombre de documents, valeurs nulles, types de données, plage de dates).

Il supprime la base de données de test à la fin.

Pour lancer la suite de tests automatisés :

PowerShell

python -m unittest test_etl_meteo.py
Résultat attendu en cas de succès : Ran 4 tests in X.XXs - O