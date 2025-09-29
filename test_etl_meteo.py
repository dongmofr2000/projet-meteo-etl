import unittest
# CORRECTION D'IMPORTATION : Importe le module entier pour modifier ses variables internes
import etl_meteo 
from pymongo import MongoClient

# --- Configuration de test ---
# Utilisez une base de données de test séparée pour ne pas interférer avec la base de données de production
TEST_MONGO_DATABASE = "meteo_projet_TEST" 

# Utilisation des constantes via le namespace du module
MONGO_URI = etl_meteo.MONGO_URI
MONGO_COLLECTION = etl_meteo.MONGO_COLLECTION

class TestETLPipeline(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Méthode exécutée une seule fois avant tous les tests. Lance le pipeline E-T-L sur la DB de test."""
        print("\n--- ⏳ DÉBUT DES TESTS E-T-L AUTOMATISÉS ---")
        
        # 1. Sauvegarde et modification de la constante de la base de données dans le module etl_meteo
        # Sauvegarde du nom de la DB de production
        cls.original_db = etl_meteo.MONGO_DATABASE 
        # Modification pour pointer vers la DB de test (impacte le run_full_etl() suivant)
        etl_meteo.MONGO_DATABASE = TEST_MONGO_DATABASE 
        
        # 2. Exécutez le pipeline E-T-L complet pour charger les données dans la DB de test
        etl_meteo.run_full_etl()

        # 3. Initialisation de la connexion MongoDB pour les tests
        cls.client = MongoClient(MONGO_URI)
        cls.db = cls.client[TEST_MONGO_DATABASE]
        cls.collection = cls.db[MONGO_COLLECTION]
        
    @classmethod
    def tearDownClass(cls):
        """Méthode exécutée une seule fois après tous les tests. Nettoie la base de données de test."""
        # 1. Nettoyage: Suppression de la base de données de test
        cls.client.drop_database(TEST_MONGO_DATABASE)
        
        # 2. Rétablit le nom de la base de données d'origine
        etl_meteo.MONGO_DATABASE = cls.original_db
        cls.client.close()
        print("--- ✅ NETTOYAGE TERMINÉ ET FIN DES TESTS ---")
        
    # --- TESTS D'INTÉGRITÉ ET DE QUALITÉ DES DONNÉES ---

    def test_01_total_document_count(self):
        """Vérifie que le nombre de documents insérés correspond au total attendu (4936)."""
        count = self.collection.count_documents({})
        self.assertEqual(count, 4936, "Le nombre de documents insérés ne correspond pas à la source.")

    def test_02_no_missing_critical_values(self):
        """Vérifie qu'il n'y a aucune valeur nulle ('null') dans la colonne de température."""
        null_temp_count = self.collection.count_documents({"temperature_c": None})
        self.assertEqual(null_temp_count, 0, "Des valeurs de température manquantes ont été trouvées après migration.")

    def test_03_numeric_fields_are_numbers(self):
        """Vérifie qu'aucun champ numérique n'est stocké comme chaîne de caractères."""
        numeric_fields = ['temperature_c', 'humidite_pct', 'pression_hpa', 'vent_vitesse_ms', 'pluie_accum_mm']
        
        for field in numeric_fields:
            # Recherche des documents où le type BSON du champ est String (type 2)
            string_count = self.collection.count_documents({field: {"$type": "string"}})
            with self.subTest(field=field):
                 self.assertEqual(string_count, 0, f"Le champ {field} contient des chaînes de caractères.")

    def test_04_data_range_is_correct(self):
        """Vérifie que la plage de dates couverte correspond à la période du 2024-10-01 au 2024-10-07."""
        
        date_range = self.collection.aggregate([
            {"$group": {
                "_id": None,
                "min_date": {"$min": "$date_heure_utc"},
                "max_date": {"$max": "$date_heure_utc"}
            }}
        ])
        
        # Vérification qu'on a bien un résultat
        results = list(date_range)
        self.assertTrue(results, "L'agrégation des dates n'a retourné aucun résultat.")
        
        result = results[0]
        min_date_str = result['min_date']
        max_date_str = result['max_date']

        # Vérifie que les dates extrêmes se situent bien dans la période 01/10 au 07/10
        self.assertIn('2024-10-01', min_date_str, "La date minimale ne correspond pas au début de la période attendue.")
        self.assertIn('2024-10-07', max_date_str, "La date maximale ne correspond pas à la fin de la période attendue.")

if __name__ == '__main__':
    unittest.main()