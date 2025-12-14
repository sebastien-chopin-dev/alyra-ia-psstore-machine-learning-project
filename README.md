# Projet IA - Prédiction sur les marketplaces des consoles digitales

## Alyra Bloc 03 - Machine learning

Scope : Dataset des jeux PS5 playstation store europe.

Les prédictions visées :

- Un jeu va-t-il baisser de prix durant la première année (50%)
- Plusieurs variantes avec classes multiples et régressions
- Dans un second temps, Classification popularité

## Structure du projet

### data

#### processed/games_data.csv

Données de jeux ps5 nettoyé pour exploration EDA.

Pour en générer un nouveau :
- Récupérer les données brut collectées JSON et les installer ici:  <data/raw/psstore_all_games.json>
- Modifier au besoin et Lancer le script python suivant : `python -m src.run_clean_and_convert_raw_data`

#### processed/featured_games_dataset_final.csv

Généré par le notebook 2_features_engeniering.ipynb

Données de jeux ps5 avec features et target pour la modélisation ML.

### notebooks

#### 1_dataset_exploration.ipynb

Statistiques descriptives.
- Vérifier la qualité des données
- Vérifier la cohérence des données
- Quantifier les données manquantes
- Voir les corrélations

#### 2_features_engeniering.ipynb

- Dans ce notebook, nous allons créer les features de baisse de prix à prédire.
- Nous allons également créer des features supplémentaire pour améliorer les performances des modèles ML.

#### 3_price_discount_classification_ml_model.ipynb

Dans ce notebook, nous allons tester différents modèles de machine learning pour la prédiction des seuils de promotion.

Expériementation en plusieurs phases:
- Phase 1: Réaliser un premier entrainement de base:
- Phase 2: Tester et comparer avec d'autres combinaisons de features
- Phase 3:  Tester et comparer avec différentes techniques de preprocess
- Phase 4: Entrainement et comparaison avec d'autres modèles
- Phase 5: Expérimenter sur d'autres target de prédiction

## Installation des dépendances

- `pip install python-dotenv`
- `pip install requests`
- `pip install aiohttp`
- `pip install aiodns`
- `pip install pandas`
- `pip install matplotlib`
- `pip install numpy`
- `pip install seaborn`
- `pip install dython`
- `pip install scikit-learn`

