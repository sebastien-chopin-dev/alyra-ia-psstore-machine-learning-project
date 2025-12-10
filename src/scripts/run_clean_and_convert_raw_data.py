from datetime import datetime
from pathlib import Path
import os

from src.clean.clean_raw_data import (
    filter_and_process_raw_json_file,
    create_csv,
    remove_game_name_duplicate_keep_min_nan_optimized,
    remove_id_duplicate_keep_min_nan_optimized,
)

from src.data_loader import load_processed_file


def clean_and_convert_raw_data():
    target_path_processed_data = os.path.join(
        Path.cwd(), "data/raw/psstore_all_games.json"
    )
    # On filtre les données pour cibler la playstation 5 - 2 jours
    df = filter_and_process_raw_json_file(
        target_path_processed_data,
        released_date_filter=datetime(2020, 11, 10),
        min_price_ps5=1.0,  # Que des jeux payants
        min_price_ps4=-1.0,  # On ne cible que la console ps5 (on veut prédire sur des jeux récents)
        min_price_dlc=-1.0,  # On ne cible que des jeux complet, pas d'extensions
    )

    # Remove duplicate id_store
    df = remove_id_duplicate_keep_min_nan_optimized(df)

    # Remove duplicate game_name
    df = remove_game_name_duplicate_keep_min_nan_optimized(df)

    print(f"DataFrame créé avec {len(df)} lignes et {len(df.columns)} colonnes")
    # Afficher un aperçu
    print("\nAperçu des données:")
    print(df.head())
    print("\nInfo du DataFrame:")
    print(df.info())

    # Sauvegarder selon le format choisi
    create_csv(df, output_format="csv")


# Load processed file
def main(clean_and_convert=False):
    if clean_and_convert:
        clean_and_convert_raw_data()

    df = load_processed_file()

    if df is None:
        print("Erreur lors du chargement du fichier raw traité.")
        return None
    else:
        print(
            f"DataFrame raw traité chargé avec {len(df)} lignes et {len(df.columns)} colonnes"
        )

    return True


if __name__ == "__main__":
    main(True)
