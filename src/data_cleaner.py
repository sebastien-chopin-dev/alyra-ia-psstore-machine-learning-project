# Import et chargement

from datetime import datetime
from pathlib import Path
import os
import json
import pandas as pd
from src.clean.clean_raw_data_helper import (
    check_released_date_is_futur,
    days_until_first_discount,
    days_until_first_sales_record,
    get_additionnal_features_tags,
    get_base_price,
    get_developer,
    get_difficulty,
    get_dlcs_count,
    get_genres_list,
    get_have_micro_transaction,
    get_how_long,
    get_id_store,
    get_is_indie,
    get_is_ps4,
    get_is_ps5_exclusive,
    get_is_ps5_pro,
    get_is_remaster,
    get_is_vr,
    get_local_multi_player_count,
    get_max_price_from_ggsales_history_complete,
    get_metacritic,
    get_min_price_from_sales_history,
    get_online_multi_player_count,
    get_pack_deluxe_count,
    get_product_name,
    get_psstore_start_rating_average,
    get_psstore_start_rating_total_count,
    get_publisher,
    get_rating_pegi_esrb,
    get_sales_history,
    get_release_date,
    get_serie_count,
    get_size,
    get_trophys_count,
    get_voice_subtitle_list,
)


def remove_id_duplicate_keep_min_nan_optimized(df_to_opti: pd.DataFrame):
    # Calculer le nombre de NaN pour chaque ligne
    df_copy = df_to_opti.copy()
    df_copy["nan_count"] = df_copy.isnull().sum(axis=1)

    # Trier par id_store puis par nombre de NaN
    df_sorted = df_copy.sort_values(["id_store", "nan_count"])

    # Garder la première ligne de chaque groupe (celle avec le moins de NaN)
    result = df_sorted.groupby("id_store").first().reset_index()

    # Supprimer la colonne temporaire
    result = result.drop("nan_count", axis=1)

    return result


def remove_game_name_duplicate_keep_min_nan_optimized(df_to_opti: pd.DataFrame):
    # Calculer le nombre de NaN pour chaque ligne
    df_copy = df_to_opti.copy()
    df_copy["nan_count"] = df_copy.isnull().sum(axis=1)

    # Trier par game_name puis par nombre de NaN
    df_sorted = df_copy.sort_values(["game_name", "nan_count"])

    # Garder la première ligne de chaque groupe (celle avec le moins de NaN)
    result = df_sorted.groupby("game_name").first().reset_index()

    # Supprimer la colonne temporaire
    result = result.drop("nan_count", axis=1)

    return result


def filter_and_process_raw_json_file_get_ps5_games():
    # On filtre les données pour cibler la playstation 5
    target_path_processed_data = os.path.join(
        Path.cwd(), "data/raw/psstore_all_games.json"
    )

    df = filter_and_process_raw_json_file(
        target_path_processed_data,
        released_date_filter=datetime(2020, 10, 10),
        min_price_ps5=1.0,
        min_price_ps4=200.0,
        min_price_dlc=200.0,
    )

    return df


def filter_and_process_raw_json_file(
    file_path,
    released_date_filter: datetime,
    min_price_ps5: float,
    min_price_ps4: float,
    min_price_dlc: float,
):
    # Liste pour stocker toutes les données
    data_list = []

    if min_price_dlc < 0:
        min_price_dlc = 1000

    if min_price_ps4 < 0:
        min_price_ps4 = 1000

    with open(file_path, "r", encoding="utf-8") as fp:
        try:
            try:
                data_all = json.load(fp)
            except Exception as e:
                print(f"Load json {e}")
                return None

            for curr_game in data_all:

                for game_key, data in curr_game.items():
                    name = game_key

                    # On ne prend que les jeux déjà sortie avant extraction
                    is_futur_game = check_released_date_is_futur(data)
                    if is_futur_game:
                        continue

                    short_url_name = name
                    id_store = get_id_store(data)
                    game_name = get_product_name(data)
                    publisher = get_publisher(data)
                    developer = get_developer(data)

                    is_ps5_li = False
                    is_ps4_li = False
                    is_dlc_li = False

                    if data["Request"] == "games_ps5":
                        is_ps5_li = True

                    if data["Request"] == "dlcs_ps5":
                        is_dlc_li = True

                    if data["Request"] == "games_ps4":
                        is_ps4_li = True

                    if is_ps5_li or is_dlc_li:
                        is_ps5 = 1
                    else:
                        is_ps5 = 0

                    if is_ps4_li:
                        is_ps4 = 1
                    else:
                        is_ps4 = 0

                    if is_ps5_li or is_dlc_li:
                        is_ps4 = get_is_ps4(data)

                    release_date = get_release_date(data)
                    base_price = get_base_price(data)

                    if base_price > 90:
                        # Verify price twice
                        ggprice = get_max_price_from_ggsales_history_complete(data)
                        if ggprice > 0:
                            base_price = ggprice

                        # print(short_url_name)

                    # On ne garde que les jeux récents
                    if is_ps4_li or is_ps5_li or is_dlc_li:
                        if release_date is None:
                            continue
                        # Date de référence :  sortie ps5
                        if release_date < released_date_filter:
                            continue

                    # On ne garde que les prix exploitable
                    if is_ps5_li:
                        if base_price < min_price_ps5:
                            continue

                    if is_ps4_li:
                        if base_price < min_price_ps4:
                            continue

                    if is_dlc_li:
                        if base_price < min_price_dlc:
                            continue

                    pssstore_star_rating = get_psstore_start_rating_average(data)
                    pssstore_star_rating_count = get_psstore_start_rating_total_count(
                        data
                    )
                    genres_list = get_genres_list(data)
                    series_count = get_serie_count(data)
                    pack_deluxe_count = get_pack_deluxe_count(data)
                    has_micro_transactions = get_have_micro_transaction(data)
                    dlcs_count = get_dlcs_count(data)
                    trophy_count = get_trophys_count(data)
                    is_indie = get_is_indie(data)
                    isps5pro = get_is_ps5_pro(data)
                    ps_exclusive = get_is_ps5_exclusive(data)
                    is_vr = get_is_vr(data)
                    is_remaster = get_is_remaster(id_store, game_name)

                    local_multi_available, local_multi_nbplayers = (
                        get_local_multi_player_count(data)
                    )
                    if local_multi_nbplayers is not None:
                        local_multi_nbplayers = int(local_multi_nbplayers)

                    online_multi_available, online_multi_nbplayers, online_only = (
                        get_online_multi_player_count(data)
                    )

                    difficulty = get_difficulty(data)
                    ps4size, ps5size = get_size(data)

                    # if is_ps4 == 0:
                    #     if ps4size is None:
                    #         ps4size = 0

                    # if is_ps5 == 0:
                    #     if ps5size is None:
                    #         ps5size = 0

                    low_hour, high_hour = get_how_long(data)

                    metacritic_critic_score, metacritic_critic_userscore = (
                        get_metacritic(data)
                    )

                    pegi_rating, esrb_rating, rating_desc = get_rating_pegi_esrb(data)
                    voices_lang, subs_lang = get_voice_subtitle_list(data)

                    if len(rating_desc) == 0:
                        rating_desc = None

                    if esrb_rating is None and pegi_rating is None:
                        continue

                    sales_history = get_sales_history(data)

                    days_until_first_10 = days_until_first_discount(
                        sales_history, base_price, release_date, 10
                    )

                    days_until_first_25 = days_until_first_discount(
                        sales_history, base_price, release_date, 25
                    )

                    days_until_first_50 = days_until_first_discount(
                        sales_history, base_price, release_date, 50
                    )

                    days_until_first_75 = days_until_first_discount(
                        sales_history, base_price, release_date, 75
                    )

                    days_to_first_price_record = days_until_first_sales_record(
                        sales_history, release_date
                    )

                    lowest_price = get_min_price_from_sales_history(sales_history)

                    if lowest_price is not None and lowest_price < 0:
                        lowest_price = 0

                    if lowest_price is None:
                        continue

                    if (
                        days_to_first_price_record is not None
                        and days_to_first_price_record < 0
                    ):
                        days_to_first_price_record = 0

                    is_dlc = int(is_dlc_li)

                    # print(days_from_first_record)

                    additional_features_tags = get_additionnal_features_tags(data)

                    # Créer un dictionnaire avec toutes les données
                    row_data = {
                        "short_url_name": short_url_name,
                        "id_store": id_store,
                        "game_name": game_name,
                        "publisher": publisher,
                        "developer": developer,
                        "release_date": release_date,
                        "pssstore_stars_rating": pssstore_star_rating,
                        "pssstore_stars_rating_count": pssstore_star_rating_count,
                        "metacritic_critic_score": metacritic_critic_score,
                        "metacritic_critic_userscore": metacritic_critic_userscore,
                        "genres": ",".join(genres_list) if genres_list else "",
                        "is_ps4": is_ps4,
                        "is_ps5": is_ps5,
                        "is_indie": is_indie,
                        "is_dlc": is_dlc,
                        "is_vr": is_vr,
                        "is_opti_ps5_pro": isps5pro,
                        "is_remaster": is_remaster,
                        "is_ps_exclusive": ps_exclusive,
                        "series_count": series_count,
                        "packs_deluxe_count": pack_deluxe_count,
                        "has_microtransactions": has_micro_transactions,
                        "dlcs_count": dlcs_count,
                        "trophies_count": trophy_count,
                        "has_local_multiplayer": local_multi_available,
                        "local_multiplayer_max_players": local_multi_nbplayers,
                        "has_online_multiplayer": online_multi_available,
                        "online_multiplayer_max_players": online_multi_nbplayers,
                        "is_online_only": online_only,
                        "difficulty": difficulty,
                        # "download_size_ps4": ps4size,
                        "download_size": ps5size,
                        "hours_main_story": low_hour,
                        "hours_completionist": high_hour,
                        "pegi_rating": pegi_rating,
                        "esrb_rating": esrb_rating,
                        "rating_descriptions": (
                            ",".join(rating_desc) if rating_desc else ""
                        ),
                        "voice_languages": ",".join(voices_lang) if voices_lang else "",
                        "subtitle_languages": ",".join(subs_lang) if subs_lang else "",
                        # "additional_features_tags": (
                        #     ",".join(additional_features_tags)
                        #     if additional_features_tags
                        #     else ""
                        # ),
                        "base_price": base_price,
                        "lowest_price": lowest_price,
                        # "days_to_first_price_record": days_to_first_price_record,
                        # "days_to_10_percent_discount": days_until_first_10,
                        # "days_to_25_percent_discount": days_until_first_25,
                        # "days_to_50_percent_discount": days_until_first_50,
                        # "days_to_75_percent_discount": days_until_first_75,
                        "price_history": (
                            json.dumps(sales_history) if sales_history else None
                        ),
                    }

                    data_list.append(row_data)

        except Exception as e:
            print(e)

        # Créer le DataFrame
    data_frame_games = pd.DataFrame(data_list)
    # Le problème est que pandas convertit automatiquement en float quand il y a un mélange de None et d'entiers dans une colonne

    col_to_int_nullable = [
        "trophies_count",
        "local_multiplayer_max_players",
        "online_multiplayer_max_players",
        "difficulty",
        "download_size",
        # "download_size_ps5",
        "hours_main_story",
        "hours_completionist",
        "metacritic_critic_score",
        "metacritic_critic_userscore",
        "pegi_rating",
        # "days_to_first_price_record",
        # "days_to_first_discount",
        # "days_to_10_percent_discount",
        # "days_to_25_percent_discount",
        # "days_to_50_percent_discount",
        # "days_to_75_percent_discount",
    ]

    for col in col_to_int_nullable:
        data_frame_games[col] = data_frame_games[col].astype("Int64")

    return data_frame_games


def create_csv(df: pd.DataFrame, output_format="csv"):
    # Sauvegarder selon le format choisi

    if output_format.lower() == "csv":
        output_file = os.path.join(Path.cwd(), "data/processed/games_data.csv")
        df.to_csv(output_file, index=False, encoding="utf-8")
        print(f"Fichier CSV créé: {output_file}")
    elif output_format.lower() == "parquet":
        output_file = os.path.join(Path.cwd(), "data/processed/games_data.parquet")
        df.to_parquet(output_file, index=False, engine="pyarrow")
        print(f"Fichier Parquet créé: {output_file}")
    else:
        raise ValueError("Format non supporté. Utilisez 'csv' ou 'parquet'")

    return df


def clean_and_convert():
    target_path_processed_data = os.path.join(
        Path.cwd(), "data/raw/psstore_all_games.json"
    )
    df = filter_and_process_raw_json_file(
        target_path_processed_data,
        released_date_filter=datetime(2020, 10, 10),
        min_price_ps5=4.9,
        min_price_ps4=18.9,
        min_price_dlc=14.9,
    )

    df = remove_id_duplicate_keep_min_nan_optimized(df)
    df = remove_game_name_duplicate_keep_min_nan_optimized(df)

    print(f"DataFrame créé avec {len(df)} lignes et {len(df.columns)} colonnes")
    # Afficher un aperçu
    print("\nAperçu des données:")
    print(df.head())
    print("\nInfo du DataFrame:")
    print(df.info())

    # Sauvegarder selon le format choisi
    create_csv(df, output_format="csv")

    return df


if __name__ == "__main__":

    # Créer CSV
    dataf = clean_and_convert()

    # Ou créer Parquet
    # df = create_csv(folder_path, output_format="parquet")
