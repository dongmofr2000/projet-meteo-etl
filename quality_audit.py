from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

# --- Configuration MongoDB ---
MONGO_URI = "mongodb://localhost:27017/" 
MONGO_DATABASE = "meteo_projet"          # Base de données à auditer (changez si vous voulez auditer la DB de test)
MONGO_COLLECTION = "donnees_horaires"

# --- Contraintes de Qualité ---
# Définition des plages de valeurs physiques pour calculer le taux d'anomalies
QUALITY_CONSTRAINTS = {
    "temperature_c": {"min": -50.0, "max": 50.0},
    "humidite_pct": {"min": 0, "max": 100},
    "pression_hpa": {"min": 800.0, "max": 1100.0},
    "vent_vitesse_ms": {"min": 0.0, "max": 50.0}
}

def calculate_error_rate():
    """Se connecte à MongoDB et calcule le taux d'anomalies des données."""
    
    print(f"--- 📊 CALCUL DU TAUX D'ERREUR DE QUALITÉ dans '{MONGO_DATABASE}' ---")

    try:
        # 1. Connexion à MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        
        total_documents = collection.count_documents({})
        if total_documents == 0:
            print("❌ ERREUR: La collection est vide. Impossible de calculer le taux d'erreur.")
            client.close()
            return

        anomalies_found = 0
        
        print(f"-> Total des enregistrements vérifiés : {total_documents}")
        print("\n-> Vérification des plages de valeurs (Contrôles de Validité) :")
        
        # 2. Itération sur les contraintes et Agrégation
        for field, constraint in QUALITY_CONSTRAINTS.items():
            
            # La requête trouve les documents où le champ est en dehors de [min, max]
            query = {
                "$or": [
                    {field: {"$lt": constraint["min"]}}, # Inférieur au minimum
                    {field: {"$gt": constraint["max"]}}  # Supérieur au maximum
                ]
            }
            
            anomalous_count = collection.count_documents(query)
            anomalies_found += anomalous_count

            if anomalous_count > 0:
                print(f"   ❌ {field.upper()} : {anomalous_count} anomalies trouvées (hors de [{constraint['min']}, {constraint['max']}]).")
            else:
                print(f"   ✅ {field.upper()} : 0 violation. (Plage [{constraint['min']}, {constraint['max']}])")


        # 3. Calcul du Taux d'Erreur Final
        if total_documents > 0:
            error_rate = (anomalies_found / total_documents) * 100
        else:
            error_rate = 0.0

        print("\n------------------------------------------------------")
        print(f"-> Nombre total d'anomalies de plage trouvées : {anomalies_found}")
        print(f"-> TAUX D'ERREUR DE QUALITÉ (Anomalies de Plage) : {error_rate:.2f}%")
        print("------------------------------------------------------")
        
        client.close()

    except ConnectionFailure:
        print("\n❌ ERREUR DE CONNEXION: Assurez-vous que votre serveur MongoDB est démarré.")
    except Exception as e:
        print(f"\n❌ ERREUR LORS DU CALCUL : {e}")

if __name__ == "__main__":
    calculate_error_rate()