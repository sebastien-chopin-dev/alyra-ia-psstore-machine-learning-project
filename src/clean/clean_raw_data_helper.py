from datetime import datetime
import json
import re
from difflib import SequenceMatcher
import pandas as pd

from src.constants.constants import EXTRACT_DATE


def is_valid_date(date_str):
    """Vérifie si une date est valide"""
    try:
        entry_date = datetime.strptime(date_str, "%Y-%m-%d")
        if entry_date > EXTRACT_DATE:
            return False
        return True
    except ValueError:
        return False


def merge_and_clean_sales_histories(history1, history2):
    """
    Fusionne deux historiques, supprime les dates invalides et prix = -1,
    puis trie par date.
    """
    merged = {}

    def process_entry(entry):
        date = entry.get("x", "")
        price = entry.get("y", -1)

        # Convertir le prix en float
        if isinstance(price, str):
            try:
                price = float(price)
            except ValueError:
                return None

        # Vérifier validité
        if is_valid_date(date) and price > 0:
            return date, price
        return None

    # Traiter les deux historiques
    for entry in history1 + history2:
        result = process_entry(entry)
        if result:
            date, price = result
            merged[date] = price  # Évite les doublons automatiquement

    # Convertir en liste et trier
    result = [{"x": date, "y": price} for date, price in merged.items()]
    result.sort(key=lambda item: datetime.strptime(item["x"], "%Y-%m-%d"))

    return result


def days_until_first_sales_record(sales_history, release_date):
    if release_date is None:
        return -1

    if len(sales_history) == 0:
        return -1

    # Convertir release_date en datetime si c'est une string
    if isinstance(release_date, str):
        release_date = datetime.strptime(release_date, "%Y-%m-%d")

    # Parcourir l'historique pour trouver le premier record
    for entry in sales_history:
        date_str = entry["x"]
        date = datetime.strptime(date_str, "%Y-%m-%d")

        # Vérifier le nombre de jours qui sépare la date de sortie de la première date enregistrée
        days_diff = (date - release_date).days
        return days_diff

    # Aucune baisse trouvée
    return None


def days_until_first_discount(
    sales_history, base_price, release_date, discount_threshold=0
):
    if release_date is None:
        return None

    if len(sales_history) == 0:
        return None

    if not isinstance(base_price, (int, float)):
        return None

    # Convertir release_date en datetime si c'est une string
    if isinstance(release_date, str):
        release_date = datetime.strptime(release_date, "%Y-%m-%d")

    # Calculer le prix cible selon le seuil de réduction
    target_price = base_price * (1 - discount_threshold / 100)

    # Parcourir l'historique pour trouver la première baisse atteignant le seuil
    for entry in sales_history:
        price = entry["y"]
        date_str = entry["x"]
        date = datetime.strptime(date_str, "%Y-%m-%d")

        # Ignorer les prix négatifs
        if price < 0.5:
            continue

        # Vérifier si la réduction atteint le seuil et après la date de sortie
        if price <= target_price and date >= release_date:
            days_diff = (date - release_date).days
            return days_diff

    # Aucune baisse au seuil voulu trouvée
    return None


def get_max_price_from_sales_histroy(sales_history):
    # Parcourir l'historique pour trouver la première baisse
    max_price = 0
    for entry in sales_history:
        price = entry["y"]
        if max_price < price:
            max_price = price

    return max_price


def get_min_price_from_sales_history(sales_history):
    # Parcourir l'historique pour trouver la première baisse
    min_price = 5000.0
    if len(sales_history) == 0:
        return None

    for entry in sales_history:
        try:
            price = float(entry["y"])
            if price > 0.5 and min_price > price:
                min_price = price
        except Exception:
            pass

    if min_price is not None and min_price > 4000:
        min_price = None

    return min_price


def get_product_name(data):
    result = None

    try:
        result = data["PSStore"]["Name"]
        return result
    except Exception:
        result = None

    try:
        result = data["GGDeals"]["GameName"]
        return result
    except Exception:
        result = None

    try:
        result = data["PlatPrices"]["GameName"]
        return result
    except Exception:
        result = None

    return result


def get_publisher(data):
    result = None

    try:
        result = data["PSStore"]["Publisher"]
        return result
    except Exception:
        result = None

    try:
        result = data["PlatPrices"]["Publisher"]
        return result
    except Exception:
        result = None

    try:
        result = data["GGDeals"]["Publisher"]
        return result
    except Exception:
        result = None

    return result


def get_id_store(data):
    result = None

    try:
        result = data["PSStore"]["ID"]
        return result
    except Exception:
        result = None

    try:
        result = data["PSStore"]["Sku"]
        return result
    except Exception:
        result = None

    try:
        result = data["PlatPrices"]["PSNID"]
        return result
    except Exception:
        result = None

    return result


def get_serie_count(data):
    result = None
    # SeriesCount
    try:
        result = data["GGDeals"]["SeriesCount"]
        result = int(result)
        return result
    except Exception:
        result = 0

    return result


def get_pack_deluxe_count(data):
    result = None
    # SeriesCount
    try:
        result = data["GGDeals"]["EditionPackCount"]
        result = int(result)
        return result
    except Exception:
        result = 0

    return result


def get_dlcs_count(data):
    result = None
    # SeriesCount
    try:
        result = data["GGDeals"]["DLCsCount"]
        result = int(result)
        return result
    except Exception:
        result = 0

    return result


def get_trophys_count(data):

    cpt = 0

    try:
        bronze = data["PlatPrices"]["Bronze"]
        bronze = int(bronze)
        if bronze >= 0:
            cpt += bronze
    except Exception:
        bronze = -1
    try:
        silver = data["PlatPrices"]["Silver"]
        silver = int(silver)
        if silver >= 0:
            cpt += silver
    except Exception:
        silver = -1
    try:
        gold = data["PlatPrices"]["Gold"]
        gold = int(gold)
        if gold >= 0:
            cpt += gold
    except Exception:
        gold = -1
    try:
        plat = data["PlatPrices"]["Platinum"]
        plat = int(plat)
        if plat >= 0:
            cpt += plat
    except Exception:
        plat = -1

    if bronze < 0 and silver < 0 and gold < 0 and plat < 0:
        return None

    return cpt


def get_is_indie(data):
    result = 0
    try:
        result = data["GGDeals"]["IsIndie"]
        result = parse_int_0_1(result)
        return result
    except Exception:
        result = 0

    return result


def get_is_ps5_pro(data):
    try:
        result = data["PSStore"]["Notices"][0]
        if "Optimisé pour la PS5 Pro" in result:
            return 1

    except Exception:
        pass

    try:
        result = data["GGDeals"]["InfosVR"]
        if "PS5 Pro Enhanced" in result:
            return 1

    except Exception:
        pass

    return 0


def get_is_ps5_exclusive(data):
    try:
        result = data["GGDeals"]["Tags"]
        if "PlayStation exclusive" in result:
            return 1

    except Exception:
        pass

    return 0


def get_local_multi_player_count(data) -> tuple[int, int | None]:
    available = int(0)
    nb_players: int | None = None
    try:
        notices = data["PSStore"]["Notices"][0]

        for notice in notices:
            # Recherche le pattern "De 1 à X joueurs"
            match = re.search(r"De 1 à (\d+) joueurs?", notice)
            if match:
                available = int(1)
                return available, int(match.group(1))
    except Exception:
        pass

    # PLatPrices
    try:
        nb_players = data["PlatPrices"]["OfflinePlayers"]
        nb_players = int(nb_players)
        if nb_players > 1:
            available = int(1)
            return available, int(nb_players)
    except Exception:
        pass

    # GGDeals "Features": "Single-player,Local multiplayer",
    try:
        infos = data["GGDeals"]["Features"]

        keywords = [
            "Local multiplayer",
        ]
        # Vérifie si au moins une notice contient un mot-clé
        for keyword in keywords:
            if keyword in infos:
                available = int(1)
    except Exception:
        pass

    if available == 1:
        available = int(1)
        return available, None
    else:
        return int(0), int(0)


def get_online_multi_player_count(data) -> tuple[int, int | None, int]:
    available = int(0)
    online_only = int(0)
    nb_players: int | None = None
    # Get online only
    try:
        notices = data["PSStore"]["Notices"][0]
        if "Jeu en ligne requis" in notices:
            online_only = 1
    except Exception:
        online_only = 0

    #  get nombre de joueur en ligne ps store infos
    try:
        notices = data["PSStore"]["Notices"][0]

        for notice in notices:
            # Pattern 1: "X joueurs en ligne" ou "X joueur en ligne"
            match = re.search(r"^(\d+) joueurs? en ligne", notice)
            if match:
                available = 1
                return int(available), int(match.group(1)), int(online_only)

            # Pattern 2: "Prend en charge jusqu'à X joueurs en ligne avec PS Plus"
            match = re.search(r"jusqu\'à (\d+) joueurs? en ligne avec PS Plus", notice)
            if match:
                available = 1
                return int(available), int(match.group(1)), int(online_only)

            # Pattern 3: "Prend en charge 1 joueur en ligne avec PS Plus" (cas particulier)
            match = re.search(
                r"Prend en charge (\d+) joueurs? en ligne avec PS Plus", notice
            )
            if match:
                available = 1
                return int(available), int(match.group(1)), int(online_only)
    except Exception:
        pass

    try:
        nb_players = data["PlatPrices"]["OnlinePlayers"]
        nb_players = int(nb_players)

        online_play = data["PlatPrices"]["OnlinePlay"]
        online_play = int(online_play)

        if nb_players > 0 or online_play == 1:
            available = 1
        else:
            available = 0

    except Exception:
        online_only = 0

    if available == 1 and nb_players < 0:
        nb_players = None
    elif available == 1 and nb_players == 0:
        nb_players = 0
    elif available == 0:
        nb_players = 0

    return available, nb_players, online_only


def get_how_long(data):

    main_story = None
    main_plus_extra = None
    completionist = None
    all_style = None

    try:
        howlongdata = data["GGDeals"]["HowLong"]
    except Exception:
        howlongdata = None

    if howlongdata is not None:
        try:
            main_story = howlongdata["main_story"]
            main_story = int(main_story)
        except Exception:
            main_story = None

        try:
            main_plus_extra = howlongdata["main_plus_extra"]
            main_plus_extra = int(main_plus_extra)
        except Exception:
            main_plus_extra = None

        try:
            completionist = howlongdata["completionist"]
            completionist = int(completionist)
        except Exception:
            completionist = None

        try:
            all_style = howlongdata["all_styles"]
            all_style = int(all_style)
        except Exception:
            all_style = None

    try:
        pprices_h_low = data["PlatPrices"]["HoursLow"]
        pprices_h_low = int(pprices_h_low)
    except Exception:
        pprices_h_low = -1

    try:
        pprices_h_high = data["PlatPrices"]["HoursHigh"]
        pprices_h_high = int(pprices_h_high)
    except Exception:
        pprices_h_high = -1

    result_h_low = -1
    result_h_high = -1

    # On privilégie le resultat de how long to beat

    # Low hour
    if main_story is not None:
        result_h_low = main_story
    elif all_style is not None:
        result_h_low = all_style
    elif pprices_h_low != -1:
        result_h_low = pprices_h_low
    else:
        result_h_low = -1

    # High hour
    if main_plus_extra is not None:
        result_h_high = main_story
    elif pprices_h_high != -1:
        result_h_high = pprices_h_high
    elif completionist is not None:
        result_h_high = completionist
    else:
        result_h_high = -1

    if result_h_low is not None and result_h_low < 0:
        result_h_low = None

    if result_h_high is not None and result_h_high < 0:
        result_h_high = None

    return result_h_low, result_h_high


def get_difficulty(data) -> int | None:
    # PLatPrices
    try:
        difficulty = data["PlatPrices"]["Difficulty"]
        old_difficulty = data["PlatPrices"]["OldDifficulty"]

        difficulty = int(difficulty)
        old_difficulty = int(old_difficulty)

        if difficulty > 0:
            return difficulty

        if old_difficulty > 0:
            return old_difficulty
    except Exception:
        pass

    # GGDeals "Features",
    try:
        infos = data["GGDeals"]["Tags"]

        keywords = ["Difficult"]
        # Vérifie si au moins une notice contient un mot-clé
        for keyword in keywords:
            if keyword in infos:
                return 8
    except Exception:
        pass

    return None


def get_size(data):
    # PLatPrices
    ps4size: int | None = None
    ps5size: int | None = None

    try:
        ps4size = data["PlatPrices"]["PS4Size"]
        ps5size = data["PlatPrices"]["PS5Size"]

        ps4size = int(ps4size)
        ps5size = int(ps5size)

        if ps4size <= 0:
            ps4size = None

        if ps5size <= 0:
            ps5size = None

        return ps4size, ps5size

    except Exception:
        pass

    return None, None


def get_metacritic(data):
    # PLatPrices
    critic_score: int | None = None
    user_score: int | None = None

    try:
        critic_score = data["GGDeals"]["MetacriticScore"]["score"]
        critic_score = int(critic_score)
        if critic_score < 0:
            critic_score = None

    except Exception:
        pass

    try:
        user_score = data["GGDeals"]["MetacriticScore"]["user_score"]
        user_score = int(user_score)
        if user_score < 0:
            user_score = None

    except Exception:
        pass

    if user_score is not None:
        user_score *= 10

    return critic_score, user_score


def get_is_vr(data):
    try:
        notices = data["PSStore"]["Notices"][0]

        vr_keywords = [
            "Casque PS VR requis",
            "PlayStation VR2 facultatif",
            "Style de jeu VR",
            "Vibration du casque PlayStation VR2",
            "Casque PS VR activé",
        ]

        # Vérifie si au moins une notice contient un mot-clé VR
        for notice in notices:
            for keyword in vr_keywords:
                if keyword in notice:
                    return 1
    except Exception:
        pass

    try:
        infos = data["GGDeals"]["InfosVR"]

        vr_keywords = [
            "VR play style",
            "PlayStation VR2 required",
            "PS VR2 Sense controller trigger effect",
            "PlayStation VR2 optional",
            "PS VR2 Sense controllers optional",
        ]

        # Vérifie si au moins une notice contient un mot-clé VR
        for keyword in vr_keywords:
            if keyword in infos:
                return 1
    except Exception:
        pass

    try:
        isvr = data["PlatPrices"]["IsVR"]

        if int(isvr) == 1:
            return 1

    except Exception:
        pass

    return 0


def get_have_micro_transaction(data) -> int:
    result = None
    # PSStore Achats intra-jeu facultatifs
    #   "Notices": [
    #    [ ]]
    # GGDeals
    #   InGameCurrencyCount
    #   "RatingPEGIDesc": [
    #     "In-Game Purchases"
    #   ],
    #   "RatingESRBDesc": [
    #     "In-Game Purchases",
    #   ],

    try:
        result = data["PSStore"]["Notices"][0]
        if "Achats intra-jeu" in result:
            return 1
    except Exception:
        result = None

    try:
        result = data["GGDeals"]["InGameCurrencyCount"]
        result = int(result)
        if result > 0:
            return 1
    except Exception:
        result = None

    try:
        result = data["GGDeals"]["RatingPEGIDesc"]
        if "In-Game Purchases" in result:
            return 1
    except Exception:
        result = None

    try:
        result = data["GGDeals"]["RatingESRBDesc"]
        if "In-Game Purchases" in result:
            return 1
    except Exception:
        result = None

    return 0


def parse_int_0_1(value: str) -> int | None:
    if value != "":
        value = int(value)
        if value == 0 or value == 1:
            return value
        else:
            return None
    else:
        return None


def get_is_ps4(data):
    result = None

    try:
        result = data["PSStore"]["IsPS4"]
        result = parse_int_0_1(result)
        if result is not None:
            return result
    except Exception:
        result = None

    try:
        result = data["PlatPrices"]["IsPS4"]
        result = parse_int_0_1(result)
        if result is not None:
            return result
    except Exception:
        result = None

    try:
        result = data["GGDeals"]["IsPS4"]
        result = parse_int_0_1(result)
        if result is not None:
            return result
    except Exception:
        result = None


def get_psstore_start_rating_average(data):
    result = None

    try:
        result = data["PSStore"]["StarRatingAverage"]
        result = float(result)
        result = round(result, 2)

        if result < 0:
            return None

        return result
    except Exception:
        result = None

    return result


def get_psstore_start_rating_total_count(data):
    result = None

    try:
        result = data["PSStore"]["StarRatingTotalCount"]
        result = int(result)

        if result < 0:
            return None

        return result
    except Exception:
        result = None

    return result


def get_developer(data):
    result = None

    try:
        result = data["GGDeals"]["Developer"]
        return result
    except Exception:
        result = None

    try:
        result = data["PlatPrices"]["Developer"]
        return result
    except Exception:
        result = None

    try:
        result = data["PSStore"]["Developer"]
        return result
    except Exception:
        result = None

    return result


def get_release_date(data):
    try:
        result = data["PSStore"]["ReleaseDate"]
    except Exception:
        result = None

    if result is None:
        try:
            result = data["PlatPrices"]["ReleaseDate"]
        except Exception:
            result = None

    if result is not None:
        # Si c'est une string, la convertir en datetime
        try:
            if isinstance(result, str):
                result = datetime.strptime(result, "%Y-%m-%d")
                return result
        except Exception:
            result = None

    return result


def get_sales_history(data):
    is_futur_game = check_released_date_is_futur(data)
    if is_futur_game is True:
        return []

    try:
        ggsales = data["GGDeals"]["SalesHistory"]
    except Exception:
        ggsales = []

    try:
        ppsales = data["PlatPrices"]["SalesHistory"]
    except Exception:
        ppsales = []

    try:
        result = merge_and_clean_sales_histories(ggsales, ppsales)
    except Exception as e:
        print(e)
        result = []

    return result


def get_max_price_from_ggsales_history_complete(data):
    try:
        ggsales = data["GGDeals"]["SalesHistory"]
    except Exception:
        ggsales = []

    # Parcourir l'historique pour trouver la première baisse
    max_price = 0
    for entry in ggsales:
        price = entry["y"]
        if max_price < price:
            max_price = price

    return max_price


def get_base_price(data) -> float | None:
    try:
        ggsales = data["GGDeals"]["SalesHistory"]
    except Exception:
        ggsales = []

    try:
        error = data["PlatPrices"]["error"]
        if error == 3:
            # try get base price from ggdeals or psstore
            if ggsales is not None:
                # get max price
                max_price = get_max_price_from_sales_histroy(ggsales)
                try:
                    max_price = float(max_price, 2)
                    return max_price
                except Exception:
                    return None
            else:
                return None
    except Exception:
        pass

    result = None

    try:
        result = data["PlatPrices"]["formattedBasePrice"]
        result = result.replace("€", "")

        if result == "":
            return None

        if result == "FREE" or result == "N/A":
            return 0.0

        if result == "NOT_FOUND":
            return None

        result = float(result)
        result = round(result, 2)

        if result < 0:
            return None

        return result

    except Exception:
        pass

    return None


def check_released_date_is_futur(data):
    released = None
    try:
        released = data["PSStore"]["ReleaseDate"]
    except Exception:
        released = None

    if released is None:
        try:
            released = data["PlatPrices"]["ReleaseDate"]
        except Exception:
            released = None

    extract_date = datetime(2025, 11, 1, 17, 2, 28)
    extract_date_string = extract_date.strftime("%Y-%m-%d-%H-%M-%S")

    if released is not None:
        # Si c'est une string, la convertir en datetime
        try:
            if isinstance(released, str):
                released = datetime.strptime(released, "%Y-%m-%d")

            # Comparer avec la date d'extract
            return released > extract_date_string
        except Exception:
            released = None

    return None


def normalize_ratings(rating_pprices):
    rating_pegi = None
    rating_esrb = None

    # Mapping des ratings
    rating_map = {
        # PEGI 3
        "PEGI 3+": ("3", "Everyone"),
        "IARC 3+": ("3", "Everyone"),
        "Rating0+": ("3", "Everyone"),
        "SKOREA All": ("3", "Everyone"),
        "RatingAll": ("3", "Everyone"),
        "GCAM Provisional 3": ("3", "Everyone"),
        "PEGI Provisional 3": ("3", "Everyone"),
        "General": ("3", "Everyone"),
        "ESRB Everyone": ("3", "Everyone"),
        "USK Everyone": ("3", "Everyone"),
        "Suitable for general audiences.": ("3", "Everyone"),
        # PEGI 7
        "PEGI 7+": ("7", "Everyone 10+"),
        "IARC 7+": ("7", "Everyone 10+"),
        "Rating6+": ("7", "Everyone 10+"),
        "GCAM Provisional 7": ("7", "Everyone 10+"),
        "PEGI Provisional 7": ("7", "Everyone 10+"),
        "Parental guidance recommended": ("7", "Everyone 10+"),
        "Parental guidance recommended for younger viewers.": ("7", "Everyone 10+"),
        # PEGI 12
        "PEGI 12+": ("12", "Teen"),
        "Rating12+": ("12", "Teen"),
        "USK 12": ("12", "Teen"),
        "SKOREA 12": ("12", "Teen"),
        "ESRB Everyone 10+": ("12", "Teen"),
        "Restricted to persons 13 years and over.": ("12", "Teen"),
        # PEGI 16
        "PEGI 16+": ("16", "Mature 17+"),
        "IARC 16+": ("16", "Mature 17+"),
        "Rating16+": ("16", "Mature 17+"),
        "SKOREA 15": ("16", "Mature 17+"),
        "Not suitable for people under 15": ("16", "Mature 17+"),
        "Restricted to persons 15 years and over.": ("16", "Mature 17+"),
        "Restricted to persons 16 years and over.": ("16", "Mature 17+"),
        "Suitable for mature audiences 16 years and over.": ("16", "Mature 17+"),
        "Recommended for mature audiences": ("16", "Mature 17+"),
        "ESRB Teen": ("16", "Mature 17+"),
        # PEGI 18
        "PEGI 18+": ("18", "Adults Only 18+"),
        "Rating18+": ("18", "Adults Only 18+"),
        "Restricted to 18 and over": ("18", "Adults Only 18+"),
        "Restricted to persons 18 years and over.": ("18", "Adults Only 18+"),
        "ESRB Mature": ("18", "Mature 17+"),
        "SKOREA M": ("18", "Mature 17+"),
    }

    if rating_pprices in rating_map:
        rating_pegi, rating_esrb = rating_map[rating_pprices]

    return rating_pegi, rating_esrb


def get_rating_pegi_esrb(data):
    rating_pegi = None
    rating_esrb = None
    rating_desc = []

    try:
        rating_pegi = data["GGDeals"]["RatingPEGI"]
    except Exception:
        rating_pegi = None

    try:
        rating_esrb = data["GGDeals"]["RatingESRB"]
    except Exception:
        rating_esrb = None

    try:
        rating_pprices = data["PlatPrices"]["Rating"]
        rating_pegi_pp, rating_esrb_pp = normalize_ratings(rating_pprices)
        if rating_pegi is None:
            rating_pegi = rating_pegi_pp

        if rating_esrb is None:
            rating_esrb = rating_esrb_pp
    except Exception:
        pass

    try:
        nlist = data["GGDeals"]["RatingESRBDesc"]
        for element in nlist:
            if element not in rating_desc:
                rating_desc.append(element)
    except Exception:
        pass

    try:
        nlist = data["GGDeals"]["RatingPEGIDesc"]
        for element in nlist:
            if element not in rating_desc:
                rating_desc.append(element)
    except Exception:
        pass

    return rating_pegi, rating_esrb, rating_desc


def normalize_languages(lang_string):
    # Mapping des codes langue vers noms complets
    lang_map = {
        "en": "English",
        "fr": "French",
        "de": "German",
        "pt_BR": "Portuguese (Brazil)",
        "es": "Spanish",
        "nl": "Dutch",
        "it": "Italian",
        "ch": "Chinese",
        "zh": "Chinese (Simplified)",
        "pl": "Polish",
        "ru": "Russian",
        "pt": "Portuguese",
        "pt_PT": "Portuguese",
        "ja": "Japanese",
        "in": "Indonesian",
        "tr": "Turkish",
        "ko": "Korean",
        "es_MX": "Spanish (Mexico)",
        "ar": "Arabic",
        "ca": "Catalan",
        "uk": "Ukrainian",
        "hu": "Hungarian",
        "cs": "Czech",
        "no": "Norwegian",
        "sv": "Swedish",
        "th": "Thai",
        "fi": "Finnish",
        "hi": "Hindi",
        "hr": "Croatian",
        "eu": "Basque",
        "da": "Danish",
        "ms": "Malay",
        # Codes moins courants (ignorés ou mappés)
        "ch_HK": "Chinese",
        "en_GR": "English",
        "en_CZ": "English",
        "fr_CA": "French",
        "fr_BE": "French",
        "sk": "Slovaque",  # Slovaque - non dans ta liste
        "el": "Grec",  # Grec
        "ro": "Roumain",  # Roumain
        "he": "Hébreu",  # Hébreu
        "bg": "Bulgare",  # Bulgare
        "vi": "Vietnamien",  # Vietnamien
        "sl": "Slovène",  # Slovène
        "tl": "Tagalog",  # Tagalog
        "ga": "Irlandais",  # Irlandais
        "gl": "Galicien",  # Galicien
        "cy": "Gallois",  # Gallois
        "af": "Afrikaans",  # Afrikaans
        "gd": "Gaélique",  # Gaélique écossais
    }

    try:
        # Nettoyer la chaîne et parser
        lang_string = lang_string.strip()

        # Cas 1: Array JSON standard [\"en\", \"fr\"]
        if lang_string.startswith("["):
            # Réparer les JSON malformés (manque crochets fermants)
            if not lang_string.endswith("]"):
                lang_string += "]"
            codes = json.loads(lang_string)

        # Cas 2: Objet JSON {\"0\":\"en\", \"1\":\"fr\"}
        elif lang_string.startswith("{"):
            # Réparer les JSON malformés
            if not lang_string.endswith("}"):
                lang_string += "}"
            obj = json.loads(lang_string)
            codes = list(obj.values())

        # Cas 3: Code isolé \"en\"
        else:
            codes = [lang_string.strip('"')]

        # Convertir les codes en noms de langues
        languages = []
        for code in codes:
            code = code.strip()
            if code in lang_map and lang_map[code] is not None:
                lang_name = lang_map[code]
                if lang_name not in languages:  # Éviter les doublons
                    languages.append(lang_name)

        return languages

    except (json.JSONDecodeError, Exception):
        # En cas d'erreur, essayer d'extraire manuellement les codes
        codes = re.findall(r"\"([a-z_A-Z]+)\"", lang_string)
        languages = []
        for code in codes:
            if code in lang_map and lang_map[code] is not None:
                lang_name = lang_map[code]
                if lang_name not in languages:
                    languages.append(lang_name)
        return languages


def get_voice_subtitle_list(data):
    voice_list = []
    sub_list = []

    try:
        ggvoices = data["GGDeals"]["VoiceLang"]
        # "Delete More +" array str entry
        ggvoices = [lang for lang in ggvoices if lang != "More +"]
    except Exception:
        ggvoices = []

    try:
        ggsubs = data["GGDeals"]["SubtitleLang"]
    except Exception:
        ggsubs = []

    try:
        ppvoices = data["PlatPrices"]["VoiceLang"]
        ppvoices = normalize_languages(ppvoices)
    except Exception:
        ppvoices = []

    try:
        ppsubs = data["PlatPrices"]["SubtitleLang"]
        ppsubs = normalize_languages(ppsubs)
    except Exception:
        ppsubs = []

    try:
        for element in ggvoices:
            if element not in voice_list:
                voice_list.append(element)
    except Exception:
        pass

    try:
        for element in ppvoices:
            if element not in voice_list:
                voice_list.append(element)
    except Exception:
        pass

    try:
        for element in ggsubs:
            if element not in sub_list:
                sub_list.append(element)
    except Exception:
        pass

    try:
        for element in ppsubs:
            if element not in sub_list:
                sub_list.append(element)
    except Exception:
        pass

    return voice_list, sub_list


def update_genres_from_tags(tags):
    """
    Met à jour les genres en fonction d'une liste de tags.
    Retourne un dictionnaire avec les genres mis à jour.
    """
    # Initialiser tous les genres à 0
    genres = {
        "GenreAction": 0,
        "GenreAdventure": 0,
        "GenreCasual": 0,
        "GenreMMO": 0,
        "GenreRacing": 0,
        "GenreRPG": 0,
        "GenreSimulation": 0,
        "GenreSports": 0,
        "GenreStrategy": 0,
        "GenreTPS": 0,
        "GenreFPS": 0,
        "GenrePlatformer": 0,
        "GenreFighting": 0,
        "GenreArcade": 0,
        "GenrePuzzle": 0,
        "GenreMusic": 0,
        "GenreHorror": 0,
    }

    # Mapping des tags vers les genres
    tag_to_genre = {
        # Action
        "Action": "GenreAction",
        "Action Roguelike": "GenreAction",
        "Action RTS": "GenreAction",
        "Action Games": "GenreAction",
        "Character Action Game": "GenreAction",
        "Hack and Slash": "GenreAction",
        "Beat 'em up": "GenreAction",
        "Spectacle fighter": "GenreAction",
        # Adventure
        "Adventure": "GenreAdventure",
        "Action-Adventure": "GenreAdventure",
        "Choose Your Own Adventure": "GenreAdventure",
        "Point & Click": "GenreAdventure",
        "Walking Simulator": "GenreAdventure",
        "Visual Novel": "GenreAdventure",
        "Interactive Fiction": "GenreAdventure",
        # Casual
        "Casual": "GenreCasual",
        "Family Friendly": "GenreCasual",
        "Wholesome": "GenreCasual",
        "Cozy": "GenreCasual",
        "Relaxing": "GenreCasual",
        "Party Game": "GenreCasual",
        "Party": "GenreCasual",
        "Clicker": "GenreCasual",
        "Idler": "GenreCasual",
        # MMO
        "Massively Multiplayer": "GenreMMO",
        "MMORPG": "GenreMMO",
        "MOBA": "GenreMMO",
        # Racing
        "Racing": "GenreRacing",
        "Driving": "GenreRacing",
        "Combat Racing": "GenreRacing",
        "Automobile Sim": "GenreRacing",
        "Motocross": "GenreRacing",
        "Motorbike": "GenreRacing",
        "Bikes": "GenreRacing",
        "ATV": "GenreRacing",
        "Offroad": "GenreRacing",
        # RPG
        "RPG": "GenreRPG",
        "JRPG": "GenreRPG",
        "Action RPG": "GenreRPG",
        "Tactical RPG": "GenreRPG",
        "Strategy RPG": "GenreRPG",
        "CRPG": "GenreRPG",
        "Party-Based RPG": "GenreRPG",
        "Dungeon Crawler": "GenreRPG",
        "Souls-like": "GenreRPG",
        "RPGMaker": "GenreRPG",
        # Simulation
        "Simulation": "GenreSimulation",
        "Life Sim": "GenreSimulation",
        "Farming Sim": "GenreSimulation",
        "City Builder": "GenreSimulation",
        "Colony Sim": "GenreSimulation",
        "Management": "GenreSimulation",
        "Tycoon games": "GenreSimulation",
        "Medical Sim": "GenreSimulation",
        "Job Simulator": "GenreSimulation",
        "Flight": "GenreSimulation",
        "Space Sim": "GenreSimulation",
        "Political Sim": "GenreSimulation",
        "God Game": "GenreSimulation",
        "Hobby Sim": "GenreSimulation",
        # Sports
        "Sports": "GenreSports",
        "Football (American)": "GenreSports",
        "Basketball": "GenreSports",
        "Baseball": "GenreSports",
        "Football (Soccer)": "GenreSports",
        "Tennis": "GenreSports",
        "Hockey": "GenreSports",
        "Golf": "GenreSports",
        "Volleyball": "GenreSports",
        "Rugby": "GenreSports",
        "Cricket": "GenreSports",
        "Badminton": "GenreSports",
        "Boxing": "GenreSports",
        "Wrestling": "GenreSports",
        "Skateboarding": "GenreSports",
        "Snowboarding": "GenreSports",
        "Skiing": "GenreSports",
        "Cycling": "GenreSports",
        "BMX": "GenreSports",
        "Skating": "GenreSports",
        # Strategy
        "Strategy": "GenreStrategy",
        "Turn-Based Strategy": "GenreStrategy",
        "Turn-Based Tactics": "GenreStrategy",
        "Real Time Tactics": "GenreStrategy",
        "RTS": "GenreStrategy",
        "Grand Strategy": "GenreStrategy",
        "4X": "GenreStrategy",
        "Tower Defense": "GenreStrategy",
        "Tactical": "GenreStrategy",
        "Wargame": "GenreStrategy",
        # TPS (Third Person Shooter)
        "Third-Person Shooter": "GenreTPS",
        "Third Person": "GenreTPS",
        # FPS (First Person Shooter)
        "FPS": "GenreFPS",
        "First-Person": "GenreFPS",
        "Boomer Shooter": "GenreFPS",
        "Arena Shooter": "GenreFPS",
        "Hero Shooter": "GenreFPS",
        "Extraction Shooter": "GenreFPS",
        "Looter Shooter": "GenreFPS",
        # Platformer
        "Platformer": "GenrePlatformer",
        "2D Platformer": "GenrePlatformer",
        "3D Platformer": "GenrePlatformer",
        "Precision Platformer": "GenrePlatformer",
        "Puzzle Platformer": "GenrePlatformer",
        "Metroidvania": "GenrePlatformer",
        # Fighting
        "Fighting": "GenreFighting",
        "2D Fighter": "GenreFighting",
        "3D Fighter": "GenreFighting",
        "Martial Arts": "GenreFighting",
        # Arcade
        "Arcade": "GenreArcade",
        "Score Attack": "GenreArcade",
        "Bullet Hell": "GenreArcade",
        "Shoot 'Em Up": "GenreArcade",
        "Twin Stick Shooter": "GenreArcade",
        "Top-Down Shooter": "GenreArcade",
        "Pinball": "GenreArcade",
        # Puzzle
        "Puzzle": "GenrePuzzle",
        "Logic": "GenrePuzzle",
        "Match 3": "GenrePuzzle",
        "Sokoban": "GenrePuzzle",
        "Hidden Object": "GenrePuzzle",
        "Maze": "GenrePuzzle",
        "Escape Room": "GenrePuzzle",
        # Music
        "Music": "GenreMusic",
        "Rhythm": "GenreMusic",
        "Music-Based Procedural Generation": "GenreMusic",
        # Horror
        "Horror": "GenreHorror",
        "Survival Horror": "GenreHorror",
        "Psychological Horror": "GenreHorror",
    }

    # Parcourir les tags et mettre à jour les genres
    for tag in tags:
        if tag in tag_to_genre:
            genre_key = tag_to_genre[tag]
            genres[genre_key] = 1

    return genres


def merge_genres(dict_a, dict_b):
    """
    Fusionne deux dictionnaires de genres.
    Si au moins un des deux dict a la valeur 1 pour un genre, le résultat sera 1.
    """
    genre_merged = {}

    genre_keywords = [
        "GenreAction",
        "GenreAdventure",
        "GenreCasual",
        "GenreMMO",
        "GenreRacing",
        "GenreRPG",
        "GenreSimulation",
        "GenreSports",
        "GenreStrategy",
        "GenreTPS",
        "GenreFPS",
        "GenrePlatformer",
        "GenreFighting",
        "GenreArcade",
        "GenrePuzzle",
        "GenreMusic",
        "GenreHorror",
    ]

    for keyword in genre_keywords:
        # Si au moins un des deux dict a 1, le résultat est 1 (OR logique)
        value_a = dict_a.get(keyword, 0)
        value_b = dict_b.get(keyword, 0)
        genre_merged[keyword] = 1 if (value_a == 1 or value_b == 1) else 0

    return genre_merged


def get_array(dict_genre):
    genre_result = []

    genre_keywords = [
        "GenreAction",
        "GenreAdventure",
        "GenreCasual",
        "GenreMMO",
        "GenreRacing",
        "GenreRPG",
        "GenreSimulation",
        "GenreSports",
        "GenreStrategy",
        "GenreTPS",
        "GenreFPS",
        "GenrePlatformer",
        "GenreFighting",
        "GenreArcade",
        "GenrePuzzle",
        "GenreMusic",
        "GenreHorror",
    ]

    for keyword in genre_keywords:
        if dict_genre.get(keyword, 0) == 1:
            # Enlever "Genre" du début du nom
            genre_name = keyword.replace("Genre", "")
            genre_result.append(genre_name)

    return genre_result


def get_additionnal_features_tags(data):
    # "Single-player",
    # "Online multiplayer",
    # "Local multiplayer",
    # "Co-op",
    # "PvP",
    # "Online PvP",
    # "Online Co-op",
    # "Cross-Platform Multiplayer",
    # "Cross-platform co-op",
    # "Local PVP",
    # "Local co-op",
    # "MMO"
    try:
        ggfeatures = data["GGDeals"]["Features"].split(",")
    except Exception:
        ggfeatures = None

    if ggfeatures is not None:
        features_to_remove = [
            "Single-player",
            "Online multiplayer",
            "Local multiplayer",
        ]
        ggfeatures = [feat for feat in ggfeatures if feat not in features_to_remove]

        # Liste des tags valides
    valid_tags = {
        "Puzzle",
        "Platformer",
        "Action",
        "Action-Adventure",
        "3D",
        "Stealth",
        "Atmospheric",
        "Cinematic",
        "Story Rich",
        "Adventure",
        "Family Friendly",
        "Strategy",
        "Casual",
        "Education",
        "Time Attack",
        "Pixel Graphics",
        "Cute",
        "Precision Platformer",
        "Parkour",
        "Difficult",
        "Time Management",
        "Rhythm",
        "Colorful",
        "2D",
        "Stylized",
        "2D Platformer",
        "Card Game",
        "Solitaire",
        "Shoot 'Em Up",
        "Side Scroller",
        "Spectacle fighter",
        "Puzzle Platformer",
        "Cartoon",
        "Hand-drawn",
        "Comic Book",
        "Cyberpunk",
        "Dystopian",
        "Destruction",
        "Sci-fi",
        "Futuristic",
        "War",
        "Post-apocalyptic",
        "Fantasy",
        "Text-Based",
        "Arcade",
        "Experimental",
        "America",
        "Football (American)",
        "Robots",
        "Minimalist",
        "Logic",
        "Relaxing",
        "Maze",
        "Party",
        "Board Game",
        "Real-Time",
        "Team-Based",
        "Competitive",
        "Controller",
        "Basketball",
        "Fast-Paced",
        "Funny",
        "eSports",
        "Free to Play",
        "Massively Multiplayer",
        "Tabletop",
        "Match 3",
        "Alternate History",
        "Military",
        "Physics",
        "Shooter",
        "Violent",
        "Combat",
        "Conversation",
        "Gun Customization",
        "Open World",
        "Narration",
        "Narrative",
        "Retro",
        "Score Attack",
        "Female Protagonist",
        "Action Roguelike",
        "Beat 'em up",
        "Cats",
        "Procedural Generation",
        "Real Time Tactics",
        "World War II",
        "RTS",
        "Isometric",
        "Realistic",
        "Top-Down",
        "Historical",
        "Tactical",
        "Co-op Campaign",
        "Exploration",
        "Medieval",
        "Dog",
        "Mystery",
        "Choose Your Own Adventure",
        "Visual Novel",
        "Dating Sim",
        "Romance",
        "Choices Matter",
        "Multiple Endings",
        "Anime",
        "Pool",
        "JRPG",
        "Turn-Based",
        "Party-Based RPG",
        "Turn-Based Combat",
        "Linear",
        "Old School",
        "Sequel",
        "RPGMaker",
        "Soundtrack",
        "Metroidvania",
        "Hidden Object",
        "Point & Click",
        "Horror",
        "Bullet Hell",
        "Roguelite",
        "Arena Shooter",
        "Top-Down Shooter",
        "Roguelike",
        "Action RPG",
        "Cartoony",
        "Comedy",
        "Flight",
        "2.5D",
        "Abstract",
        "Hack and Slash",
        "Collectathon",
        "Creature Collector",
        "Dungeon Crawler",
        "Deckbuilding",
        "Dark Fantasy",
        "Perma Death",
        "Character Customization",
        "Remake",
        "1990's",
        "Wholesome",
        "Cozy",
        "Clicker",
        "Vampire",
        "Walking Simulator",
        "Philosophical",
        "Short",
        "Emotional",
        "Surreal",
        "Psychological",
        "Dark Humor",
        "Party Game",
        "Fighting",
        "Wrestling",
        "3D Fighter",
        "Sandbox",
        "Interactive Fiction",
        "LGBTQ+",
        "Well-Written",
        "Based On A Novel",
        "Drama",
        "Life Sim",
        "Sokoban",
        "Nonlinear",
        "Music",
        "Instrumental Music",
        "Psychedelic",
        "Rock Music",
        "Investigation",
        "Time Travel",
        "Nostalgia",
        "Dark",
        "Great Soundtrack",
        "Dark Comedy",
        "Survival Horror",
        "Thriller",
        "Aliens",
        "3D Platformer",
        "Jet",
        "PvE",
        "Lore-Rich",
        "Magic",
        "Supernatural",
        "Minigames",
        "Souls-like",
        "Survival",
        "Open World Survival Craft",
        "Crafting",
        "Base Building",
        "Early Access",
        "Science",
        "Building",
        "Immersive Sim",
        "LEGO",
        "Turn-Based Tactics",
        "Tactical RPG",
        "Strategy RPG",
        "Turn-Based Strategy",
        "2D Fighter",
        "Psychological Horror",
        "Escape Room",
        "Baseball",
        "Archery",
        "Cycling",
        "Horses",
        "Skiing",
        "Mythology",
        "Inventory Management",
        "Dinosaurs",
        "Boomer Shooter",
        "Conspiracy",
        "Management",
        "Agriculture",
        "Automation",
        "Capitalism",
        "Economy",
        "Farming Sim",
        "Nature",
        "Roguelike Deckbuilder",
        "Card Battler",
        "Replay Value",
        "Farming",
        "6DOF",
        "Bullet Time",
        "Classic",
        "Gore",
        "World War I",
        "Detective",
        "Pinball",
        "Dragons",
        "Swordplay",
        "Space",
        "1980s",
        "Runner",
        "Quick-Time Events",
        "Time Manipulation",
        "Sexual Content",
        "NSFW",
        "Zombies",
        "Modern",
        "City Builder",
        "Grand Strategy",
        "Colony Sim",
        "Moddable",
        "Real-Time with Pause",
        "FMV",
        "Dynamic Narration",
        "Diplomacy",
        "Political",
        "Politics",
        "Crime",
        "Level Editor",
        "Action RTS",
        "Naval",
        "Pirates",
        "MMORPG",
        "Dwarf",
        "Driving",
        "Vehicular Combat",
        "On-Rails Shooter",
        "Combat Racing",
        "Satire",
        "Boss Rush",
        "Twin Stick Shooter",
        "Demons",
        "Gothic",
        "Automobile Sim",
        "Martial Arts",
        "Memes",
        "Ninja",
        "Trading",
        "Hex Grid",
        "Gambling",
        "CRPG",
        "Lovecraftian",
        "Superhero",
        "Class-Based",
        "Medical Sim",
        "Social Deduction",
        "Voxel",
        "Cult Classic",
        "Beautiful",
        "Immersive",
        "Nudity",
        "Underwater",
        "Underground",
        "PlayStation exclusive",
        "Unique",
        "3D Vision",
        "Battle Royale",
        "Hero Shooter",
        "Loot",
        "Hentai",
        "Epic",
        "Extraction Shooter",
        "Looter Shooter",
        "Job Simulator",
        "Tanks",
        "Wargame",
        "Steampunk",
        "Mechs",
        "Grid-Based Movement",
        "Offroad",
        "Mining",
        "8-bit Music",
        "Mature",
        "Blood",
        "Documentary",
        "Photo Editing",
        "Crowdfunded",
        "Poker",
        "Mystery Dungeon",
        "Dungeons & Dragons",
        "Split Screen",
        "Asynchronous Multiplayer",
        "Cooking",
        "Artificial Intelligence",
        "Tower Defense",
        "Music-Based Procedural Generation",
        "Electronic Music",
        "Experience",
        "God Game",
        "Touch-Friendly",
        "Sniper",
        "Transportation",
        "Villain Protagonist",
        "Tycoon games",
        "Kickstarter",
        "Word Game",
        "MOBA",
        "Trading Card Game",
        "Episodic",
        "Intentionally Awkward Controls",
        "Asymmetric VR",
        "Space Sim",
        "Roguevania",
        "Jump Scare",
        "Parody",
        "Ambient",
        "Addictive",
        "Reboot",
        "Traditional Roguelike",
        "Voice Control",
    }

    # Récupérer les tags
    try:
        tags = data["GGDeals"]["Tags"].split(",")
        # Nettoyer les espaces
        tags = [tag.strip() for tag in tags if tag.strip()]
    except Exception:
        tags = []

    # Filtrer uniquement les tags valides
    valid_tags_found = [tag for tag in tags if tag in valid_tags]

    # Merger avec ggfeatures
    if ggfeatures is None:
        ggfeatures = []

    # Ajouter les tags valides sans doublons
    for tag in valid_tags_found:
        if tag not in ggfeatures:
            ggfeatures.append(tag)

    return ggfeatures


def get_genres_list(data):
    genres_list = []

    # Récupérer les sources de genres
    try:
        ggdeals = data["GGDeals"]
    except Exception:
        ggdeals = None

    try:
        pprices = data["PlatPrices"]
    except Exception:
        pprices = None

    try:
        ggtags = data["GGDeals"]["Tags"].split(",")
        genres_tags = update_genres_from_tags(ggtags)
    except Exception:
        genres_tags = None

    # Fusionner les dictionnaires disponibles
    merged_dict = None

    # Liste des sources à fusionner (dans l'ordre de priorité)
    sources = [pprices, ggdeals, genres_tags]
    available_sources = [src for src in sources if src is not None]

    if len(available_sources) > 0:
        merged_dict = available_sources[0]
        for source in available_sources[1:]:
            merged_dict = merge_genres(merged_dict, source)

    # Convertir en array
    if merged_dict is not None:
        try:
            genres_list = get_array(merged_dict)
        except Exception:
            genres_list = []

    return genres_list


def get_is_remaster(id_store: str, game_name: str):
    result = 0
    try:
        id_store = id_store.lower()
        game_name = game_name.lower()
        if "remaster" in id_store:
            # print(id_store)
            return 1
        if "remaster" in game_name:
            # print(game_name)
            return 1

    except Exception:
        result = 0

    return result


def get_game_name(file_name: str):
    return file_name.replace(".json", "")


def clean_and_merge_publishers(
    df: pd.DataFrame,
    publisher_col: str = "publisher",
    similarity_threshold: float = 0.85,
) -> pd.DataFrame:
    """
    Nettoie et fusionne les noms de publishers similaires (fautes d'orthographe, variations)
    """
    df_result = df.copy()

    # Dictionnaire de corrections manuelles
    manual_corrections = {
        "Bandai Namco Entertainment": [
            "Bandai Namco",
            "BANDAI NAMCO Entertainment",
            "Bandai Namco Entertainment Inc.",
        ],
        "Square Enix": ["SQUARE ENIX", "Square Enix Co., Ltd."],
        "Capcom": ["CAPCOM", "Capcom Co., Ltd."],
        "Ubisoft": ["UBISOFT", "Ubisoft Entertainment"],
        "Electronic Arts": ["EA", "EA Sports", "Electronic Arts Inc."],
        "Sony Interactive Entertainment": [
            "SIE",
            "Sony Interactive Entertainment LLC",
            "PlayStation Studios",
        ],
        "Activision": ["Activision Publishing", "Activision Blizzard"],
        "Warner Bros": ["Warner Bros.", "Warner Bros. Games", "WB Games"],
        "SEGA": ["Sega", "SEGA Corporation"],
        "Bethesda": ["Bethesda Softworks", "Bethesda Game Studios"],
        "Take-Two Interactive": ["Take-Two", "2K Games", "2K"],
        "Rockstar Games": ["Rockstar", "Rockstar North"],
        "Microids": ["Microïds", "Microids SA"],
        "Team17": ["Team17 Digital", "Team17 Digital Limited"],
        "Devolver Digital": ["Devolver", "Devolver Digital Inc."],
    }

    # Fonction de nettoyage de base
    def clean_publisher_name(name):
        if pd.isna(name):
            return name

        name = str(name).strip()

        # Supprimer les suffixes corporatifs communs
        suffixes = [
            " Inc.",
            " Inc",
            " LLC",
            " Ltd.",
            " Ltd",
            " Co., Ltd.",
            " Corporation",
            " Corp.",
            " Corp",
            " SA",
            " GmbH",
            " AB",
        ]
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[: -len(suffix)].strip()

        return name

    # Étape 1: Nettoyage de base
    df_result[f"{publisher_col}_clean"] = df_result[publisher_col].apply(
        clean_publisher_name
    )

    # Étape 2: Corrections manuelles
    reverse_mapping = {}
    for canonical, variations in manual_corrections.items():
        for variation in variations:
            reverse_mapping[variation] = canonical
            reverse_mapping[clean_publisher_name(variation)] = canonical

    df_result[f"{publisher_col}_temp"] = df_result[f"{publisher_col}_clean"].apply(
        lambda x: reverse_mapping.get(x, x) if pd.notna(x) else x
    )

    # Étape 3: Fusion par similarité

    # Obtenir les publishers uniques et leurs fréquences
    publisher_counts = df_result[f"{publisher_col}_temp"].value_counts()
    unique_publishers = publisher_counts.index.tolist()

    # Créer un mapping de similarité
    similarity_mapping = {}
    processed = set()
    merged_count = 0

    for i, pub1 in enumerate(unique_publishers):
        if pd.isna(pub1) or pub1 in processed:
            continue

        # Garder le publisher le plus fréquent comme canonical
        canonical = pub1
        count1 = publisher_counts[pub1]

        for pub2 in unique_publishers[i + 1 :]:
            if pd.isna(pub2) or pub2 in processed:
                continue

            # Calculer la similarité
            similarity = SequenceMatcher(
                None, str(pub1).lower(), str(pub2).lower()
            ).ratio()

            if similarity >= similarity_threshold:
                count2 = publisher_counts[pub2]

                # Le plus fréquent devient le canonical
                if count2 > count1:
                    canonical = pub2
                    count1 = count2

                similarity_mapping[pub2] = canonical
                processed.add(pub2)
                merged_count += 1

                print(
                    f"   ✓ Fusion: '{pub2}' → '{canonical}' (similarité: {similarity:.2f})"
                )

    # Appliquer le mapping de similarité
    df_result[f"{publisher_col}_normalized"] = df_result[f"{publisher_col}_temp"].apply(
        lambda x: similarity_mapping.get(x, x) if pd.notna(x) else x
    )

    # Stats
    original_count = df_result[publisher_col].nunique()
    cleaned_count = df_result[f"{publisher_col}_normalized"].nunique()

    print(f"\n📊 Résultats du nettoyage:")
    print(f"   Publishers originaux: {original_count}")
    print(f"   Publishers après nettoyage: {cleaned_count}")
    print(
        f"   Réduction: {original_count - cleaned_count} ({(original_count - cleaned_count) / original_count * 100:.1f}%)"
    )
    print(f"   Fusions automatiques: {merged_count}")

    # Remplacer la colonne originale et nettoyer
    df_result[publisher_col] = df_result[f"{publisher_col}_normalized"]
    df_result = df_result.drop(
        columns=[
            f"{publisher_col}_clean",
            f"{publisher_col}_temp",
            f"{publisher_col}_normalized",
        ]
    )

    return df_result
