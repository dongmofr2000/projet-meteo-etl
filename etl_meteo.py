import pandas as pd
import json
from datetime import datetime
import os 
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure

# --- 0. CONFIGURATION MONGO DB ET FICHIERS ---

# URI de votre instance MongoDB Compass locale
MONGO_URI = "mongodb://localhost:27017/" 
MONGO_DATABASE = "meteo_projet"            # Base de données cible
MONGO_COLLECTION = "donnees_horaires"      # Collection cible

# --- DÉFINITIONS DES CHEMINS DE FICHIERS LOCAUX (DOIVENT CORRESPONDRE AUX NOMS DE VOS FICHIERS) ---
csv_files_la_madeleine = {
    '2024-10-07': "Weather+Underground+-+La+Madeleine,+FR.xlsx - 071024.csv",
    '2024-10-06': "Weather+Underground+-+La+Madeleine,+FR.xlsx - 061024.csv",
    '2024-10-05': "Weather+Underground+-+La+Madeleine,+FR.xlsx - 051024.csv",
    '2024-10-04': "Weather+Underground+-+La+Madeleine,+FR.xlsx - 041024.csv",
    '2024-10-03': "Weather+Underground+-+La+Madeleine,+FR.xlsx - 031024.csv",
    '2024-10-02': "Weather+Underground+-+La+Madeleine,+FR.xlsx - 021024.csv",
    '2024-10-01': "Weather+Underground+-+La+Madeleine,+FR.xlsx - 011024.csv"
}
csv_files_ichtegem = {
    '2024-10-07': "Weather+Underground+-+Ichtegem,+BE.xlsx - 071024.csv",
    '2024-10-06': "Weather+Underground+-+Ichtegem,+BE.xlsx - 061024.csv",
    '2024-10-05': "Weather+Underground+-+Ichtegem,+BE.xlsx - 051024.csv",
    '2024-10-04': "Weather+Underground+-+Ichtegem,+BE.xlsx - 041024.csv",
    '2024-10-03': "Weather+Underground+-+Ichtegem,+BE.xlsx - 031024.csv",
    '2024-10-02': "Weather+Underground+-+Ichtegem,+BE.xlsx - 021024.csv",
    '2024-10-01': "Weather+Underground+-+Ichtegem,+BE.xlsx - 011024.csv"
}
JSON_FILE_PATH = "Data_Source1_011024-071024.json"

# Définition des champs numériques à vérifier dans MongoDB
NUMERIC_FIELDS = ['temperature_c', 'humidite_pct', 'pression_hpa', 'vent_vitesse_ms', 'pluie_accum_mm']

# --- 1. FONCTIONS DE TRANSFORMATION ET DE VÉRIFICATION ---

def clean_value(value):
    """Nettoie une valeur potentiellement string/NaN pour la convertir en float."""
    if pd.isna(value): return None
    value = str(value).replace(',', '.').replace(' ', '').replace('°F', '').replace('mph', '').replace('in', '').replace('w/m²', '').replace('%', '')
    try: return float(value)
    except ValueError: return None

def clean_and_convert_csv_df(df, station_id, source):
    """Nettoie le DataFrame CSV et effectue les conversions d'unités (Impérial -> Métrique), retourne le DataFrame."""
    df_clean = pd.DataFrame()
    
    # Conversions (Noms de colonnes confirmés : Temperature, Humidity, Speed, Precip. Accum., Pressure, Time)
    df_clean['temperature_c'] = df['Temperature'].apply(clean_value).apply(lambda f: (f - 32) * 5/9 if f is not None else None)
    df_clean['humidite_pct'] = df['Humidity'].apply(clean_value)
    df_clean['pression_hpa'] = df['Pressure'].apply(clean_value).apply(lambda i: i * 33.8638 if i is not None else None) # Conversion inHg -> hPa
    df_clean['vent_vitesse_ms'] = df['Speed'].apply(clean_value).apply(lambda m: m * 0.44704 if m is not None else None) # Conversion mph -> m/s
    df_clean['pluie_accum_mm'] = df['Precip. Accum.'].apply(clean_value).apply(lambda i: i * 25.4 if i is not None else None) # Conversion in -> mm

    df_clean['date_heure_utc'] = df['Time'].apply(lambda t: t if isinstance(t, str) else None).apply(lambda t: f"{df.name} {t}" if t else None)

    df_clean['id_station'] = station_id
    df_clean['source_donnees'] = source
    
    df_clean = df_clean.dropna(subset=['date_heure_utc'])
    
    final_cols = ['date_heure_utc', 'temperature_c', 'humidite_pct', 'pression_hpa', 'vent_vitesse_ms', 'id_station', 'source_donnees', 'pluie_accum_mm']
    return df_clean[final_cols]

def check_initial_integrity(df_clean, station_id, date_str):
    """Vérifie l'intégrité des données CSV après le nettoyage."""
    doublons = df_clean.duplicated(subset=['date_heure_utc']).sum()
    manquants_critiques = df_clean['temperature_c'].isnull().sum()
    
    if doublons > 0 or manquants_critiques > 0:
        print(f"   -> ❌ INTÉGRITÉ ({station_id} - {date_str}) : Doublons: {doublons}, Température manquante: {manquants_critiques}")
    else:
        print(f"   -> ✅ INTÉGRITÉ ({station_id} - {date_str}) : Passée.")

def clean_and_convert_json(json_hourly_data):
    """Nettoie et convertit les données JSON (Infoclimat)."""
    all_json_records = []
    
    for station_id, records in json_hourly_data.items():
        if not isinstance(records, list):
            print(f"   -> ⚠️ AVERTISSEMENT JSON: Données inattendues pour la station {station_id}. Ignoré.")
            continue
            
        for record in records:
            if not isinstance(record, dict):
                continue

            vent_moyen_ms = float(record.get('vent_moyen', 0) or 0) / 3.6
            pluie_mm = float(record.get('pluie_1h', record.get('pluie_3h', 0) or 0) or 0)
            
            # Format compatible MongoDB (dictionnaire)
            new_record = {
                'date_heure_utc': record.get('dh_utc'),
                'temperature_c': float(record.get('temperature')) if record.get('temperature') else None,
                'humidite_pct': int(record.get('humidite')) if record.get('humidite') else None,
                'pression_hpa': float(record.get('pression')) if record.get('pression') else None,
                'vent_vitesse_ms': vent_moyen_ms,
                'id_station': record.get('id_station'),
                'source_donnees': 'Infoclimat', 
                'pluie_accum_mm': pluie_mm
            }
            all_json_records.append(new_record)
            
    return all_json_records

def check_final_integrity(data_list):
    """Vérifie l'intégrité de la liste complète de documents unifiés avant l'insertion MongoDB."""
    if not data_list: return False
        
    df_final = pd.DataFrame(data_list)
    print("\n--- ✅ Rapport d'Intégrité Unifiée (Source Totale) ---")
    
    # Doublons globaux (même horodatage ET même station)
    duplicates = df_final.duplicated(subset=['date_heure_utc', 'id_station']).sum()
    print(f"-> Total des enregistrements : {len(df_final)}")
    print(f"-> Doublons trouvés (date/station) : {duplicates}")

    # Résumé des valeurs manquantes
    print("\n-> Résumé des valeurs manquantes par colonne :")
    print(df_final.isnull().sum())
    
    # Plage de Données
    df_final['date_heure_utc'] = pd.to_datetime(df_final['date_heure_utc'], errors='coerce')
    start_date = df_final['date_heure_utc'].min()
    end_date = df_final['date_heure_utc'].max()
    print(f"\n-> Période couverte : Du {start_date} au {end_date}")

    if duplicates > 0:
        print("❌ AVERTISSEMENT: Des doublons ont été trouvés. MongoDB les insérera, sauf si vous ajoutez un index unique.")
    print("-----------------------------------------------------")
    return True


# --- 2. FONCTIONS D'EXTRACTION ---

def read_full_json_file(file_path):
    """Lit et charge le contenu JSON complet depuis un fichier local."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ ERREUR CRITIQUE: Fichier JSON non trouvé: {file_path}. Vérifiez le nom.")
        
    with open(file_path, 'r', encoding='utf-8') as f:
        full_content = json.load(f)
    return full_content.get('hourly', {})

def extract_and_process_csv_from_disk(file_paths_dict, station_id, source):
    """Lit les fichiers CSV, les nettoie, les convertit et vérifie leur intégrité."""
    all_records = []
    
    for date_str, file_name in file_paths_dict.items():
        try:
            # Correction: Encodage 'latin-1' ET délimiteur 'sep=;'
            df_csv = pd.read_csv(file_name, header=0, skiprows=[2], encoding='latin-1', sep=';') 
            
            # Nettoyage des noms de colonnes pour éviter les espaces
            df_csv.columns = [col.strip() for col in df_csv.columns]
            
            df_csv.name = date_str 
            df_csv.insert(0, 'Date', date_str) 
            
            # Transformation (T)
            df_clean = clean_and_convert_csv_df(df_csv, station_id=station_id, source=source)
            
            # Vérification initiale
            check_initial_integrity(df_clean, station_id, date_str)
            
            records = df_clean.to_dict('records')
            all_records.extend(records)
            print(f"   -> Traité {source} ({date_str} - {file_name}): {len(records)} lignes.")
        except FileNotFoundError:
            print(f"   -> ❌ ERREUR: Fichier CSV non trouvé: {file_name}.")
        except Exception as e:
            print(f"   -> ❌ ERREUR lors du traitement de {file_name}: {e}")
            
    return all_records

# --- 3. FONCTIONS DE CHARGEMENT VERS MONGODB ---

def load_data_to_mongodb(data_list):
    """Connecte à MongoDB, insère la liste complète des documents, et vérifie la collection cible."""
        
    try:
        # 1. Connexion
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        print(f"✅ Connexion MongoDB réussie à l'URI : {MONGO_URI}")
        
        # 2. Insertion
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        
        collection.delete_many({}) # Purgement de la collection avant l'insertion
        print(f"   -> Collection '{MONGO_COLLECTION}' purgée. Insertion de {len(data_list)} documents...")
        
        result = collection.insert_many(data_list)
        
        # 3. VÉRIFICATION FINALE DANS MONGODB (Compte)
        target_count = collection.count_documents({})

        print("\n--- ✅ Rapport d'Intégrité Cible (MongoDB - Compte) ---")
        if target_count == len(data_list):
             print(f"-> ✅ SUCCÈS: Nombre d'enregistrements (Source: {len(data_list)}, Cible: {target_count}) correspond.")
        else:
             print(f"-> ❌ ÉCHEC: La source ({len(data_list)}) ne correspond pas à la cible ({target_count}).")

        print("-------------------------------------------------------")
        client.close()
        return True
    
    except ConnectionFailure:
        print("\n❌ ERREUR DE CONNEXION: Assurez-vous que votre serveur MongoDB local est démarré (via Compass) et que l'URI est correct.")
        return False
    except Exception as e:
        print(f"\n❌ ERREUR LORS DU CHARGEMENT MONGODB : {e}")
        return False


# --- 4. NOUVELLE FONCTION D'AUDIT POST-MIGRATION ---

def audit_mongodb_data():
    """Vérifie les types et les valeurs nulles des données après la migration dans MongoDB."""
    
    print("\n--- 🔬 AUDIT POST-MIGRATION MONGO DB (Qualité des Données) ---")
    
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DATABASE]
        collection = db[MONGO_COLLECTION]
        
        audit_success = True
        
        # 1. Vérification des types et des nulles (MongoDB Aggregation)
        pipeline = []
        for field in NUMERIC_FIELDS:
            # Compte le nombre de documents où le champ est manquant (null)
            pipeline.append({
                "$group": {
                    "_id": None,
                    f"nulls_{field}": {"$sum": {"$cond": [{"$eq": [f"${field}", None]}, 1, 0]}}
                }
            })
            
        # Simplification de l'agrégation en un seul appel
        audit_results = list(collection.aggregate(pipeline))

        if audit_results:
            results = audit_results[0]
            del results['_id']
            
            print("\n-> Vérification des valeurs manquantes (Nulles):")
            for key, count in results.items():
                field_name = key.replace('nulls_', '')
                if count > 0:
                    print(f"   ❌ {field_name.upper()} : {count} documents contiennent la valeur null.")
                    audit_success = False
                else:
                    print(f"   ✅ {field_name.upper()} : 0 valeur nulle (Passée).")

        # 2. Vérification des plages de valeurs (optionnel mais bon)
        # On peut exécuter un simple find pour s'assurer qu'aucune chaîne n'a été insérée
        
        # Assurez-vous qu'aucun type String n'est présent dans les champs numériques
        print("\n-> Vérification des types (chaînes de caractères dans champs numériques):")
        for field in NUMERIC_FIELDS:
            # Cherche si le champ existe et est de type String (BSON type 2)
            string_count = collection.count_documents({field: {"$type": "string"}})
            if string_count > 0:
                print(f"   ❌ {field.upper()} : {string_count} documents contiennent une chaîne de caractères au lieu d'un nombre.")
                audit_success = False
            else:
                print(f"   ✅ {field.upper()} : Type numérique correct (Passée).")


        if audit_success:
            print("\n🎉 AUDIT RÉUSSI: La qualité des données est maintenue après la migration.")
        else:
             print("\n⚠️ AVERTISSEMENT: L'audit a détecté des problèmes de qualité (nulles/types).")

        print("==========================================================================")
        client.close()

    except ConnectionFailure:
        print("\n❌ ERREUR DE CONNEXION: Impossible d'auditer. Le serveur MongoDB est-il toujours démarré ?")
    except Exception as e:
        print(f"\n❌ ERREUR LORS DE L'AUDIT MONGODB : {e}")

# --- 5. ORCHESTRATION DU PIPELINE E-T-L FINAL ---

def run_full_etl():
    """Exécute l'intégralité du pipeline E-T-L pour MongoDB, suivi de l'audit."""
    
    all_processed_records = []
    
    print("--- ⏳ PHASE 1: Traitement des 14 fichiers CSV complets (Weather Underground) ---")
    madeleine_records = extract_and_process_csv_from_disk(csv_files_la_madeleine, station_id="1001", source="Weather Underground")
    all_processed_records.extend(madeleine_records)

    ichtegem_records = extract_and_process_csv_from_disk(csv_files_ichtegem, station_id="1002", source="Weather Underground")
    all_processed_records.extend(ichtegem_records)

    print("\n--- ⏳ PHASE 2: Traitement du fichier JSON complet (Infoclimat) ---")
    
    try:
        json_raw_content = read_full_json_file(JSON_FILE_PATH)
        print(f"   -> JSON Infoclimat complet chargé.")
        
        json_records = clean_and_convert_json(json_raw_content)
        all_processed_records.extend(json_records)
        print(f"   -> Traité Infoclimat: {len(json_records)} lignes.")
    except FileNotFoundError:
        pass 
    except Exception as e:
        print(f"   -> ❌ ERREUR critique lors du traitement du fichier JSON: {e}")
    
    final_count = len(all_processed_records)
    print(f"\n--- ✅ PHASE 3: Données unifiées : {final_count} enregistrements totaux ---")
    
    if final_count > 0:
        # Vérification finale avant chargement (Source)
        check_final_integrity(all_processed_records) 
        
        print("--- 🚀 PHASE 4: Chargement des données complètes vers MongoDB ---")
        load_success = load_data_to_mongodb(all_processed_records)
        
        # NOUVELLE ÉTAPE : AUDIT POST-MIGRATION
        if load_success:
            audit_mongodb_data()
    else:
        print("⚠️ Le pipeline s'arrête car aucune donnée n'a pu être traitée.")


if __name__ == "__main__":
    run_full_etl()