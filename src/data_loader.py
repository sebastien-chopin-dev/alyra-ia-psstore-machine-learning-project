from pathlib import Path
import os
import pandas as pd

# import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns

# Fichier stocké temporairement sur un de mes S3
# url = "https://d3aok2axxchxf9.cloudfront.net/ia/games_data.csv"


def load_processed_file():
    df = None
    try:
        url = os.path.join(Path.cwd(), "data/processed/games_data.csv")

        # Charger les données CSV, Les colonnes Boolean sont au format Int64 (0,1,NaN)
        df = pd.read_csv(
            url,
            dtype={
                "trophies_count": "Int64",
                "local_multiplayer_max_players": "Int64",
                "online_multiplayer_max_players": "Int64",
                "difficulty": "Int64",
                "download_size_ps4": "Int64",
                "download_size_ps5": "Int64",
                "hours_main_story": "Int64",
                "hours_completionist": "Int64",
                "metacritic_critic_score": "Int64",
                "metacritic_critic_userscore": "Int64",
                "pegi_rating": "Int64",
                "days_to_first_price_record": "Int64",
                "days_to_10_percent_discount": "Int64",
                "days_to_25_percent_discount": "Int64",
                "days_to_50_percent_discount": "Int64",
                "days_to_75_percent_discount": "Int64",
            },
        )
    except Exception as e:
        print(f"Error when loading processed data {e}")

    return df
