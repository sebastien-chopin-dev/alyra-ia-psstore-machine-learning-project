from datetime import datetime
from pathlib import Path
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from src.data_cleaner import (
    filter_and_process_raw_json_file,
    create_csv,
    remove_game_name_duplicate_keep_min_nan_optimized,
    remove_id_duplicate_keep_min_nan_optimized,
)

from src.eda import column_summary, create_eda_report
from src.data_loader import load_processed_file


def clean_and_convert_raw_data():
    target_path_processed_data = os.path.join(
        Path.cwd(), "data/raw/psstore_all_games.json"
    )
    # On filtre les données pour avoir des jeux récents
    # On enlève également les petits jeux < 4.90 euros (bruits et baisse de prix non representative)
    df = filter_and_process_raw_json_file(
        target_path_processed_data,
        released_date_filter=datetime(2020, 10, 10),
        min_price_ps5=4.9,
        min_price_ps4=18.9,
        min_price_dlc=14.9,
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
def pipeline_ml_price_prediction(clean_and_convert=False):
    if clean_and_convert:
        clean_and_convert_raw_data()

    df = load_processed_file()

    if df is None:
        return None

    # column_summary(df)

    create_eda_report(df)

    return True


if __name__ == "__main__":
    pipeline_ml_price_prediction(True)
