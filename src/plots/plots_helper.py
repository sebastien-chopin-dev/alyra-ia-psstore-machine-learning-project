# -*- coding: utf-8 -*-
from pathlib import Path
import os
import matplotlib
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.axes import Axes
import numpy as np

from src.constants.constants import COLOR_A, COLOR_B, COLOR_C, PRICE_SEGMENTS

OUTPUT_EXPLO_PLOTS_PATH = os.path.join(Path.cwd(), "outputs/plots/exploration")


def generate_platform_proportion_data(df: pd.DataFrame):
    result = [
        {
            "label": "PS4 et PS5",
            "value": ((df["is_ps4"] == 1) & (df["is_ps5"] == 1)).sum(),
        },
        {
            "label": "PS4 Only",
            "value": ((df["is_ps4"] == 1) & (df["is_ps5"] == 0)).sum(),
        },
        {
            "label": "PS5 Only",
            "value": ((df["is_ps4"] == 0) & (df["is_ps5"] == 1)).sum(),
        },
    ]

    return result


def generate_multi_str_col_top_proportion_data(
    df: pd.DataFrame, col_name: str, top_count: int
):

    # Séparer les genres et exploser
    genres_exploded = df[col_name].str.split(",").explode()
    genres_exploded = genres_exploded.str.strip()
    genre_counts = genres_exploded.value_counts()

    # Garder le top 6 et regrouper le reste dans "Autres"
    top_val = genre_counts.head(top_count)
    autres = genre_counts.iloc[top_count:].sum()

    # Créer les données finales
    if autres > 0:
        final_counts = pd.concat([top_val, pd.Series({"Autres": autres})])
    else:
        final_counts = top_val

    result = []
    for label, value in final_counts.items():
        result.append(
            {
                "label": label,
                "value": value,
            }
        )

    return result


def generate_base_price_proportion_data(df: pd.DataFrame):

    result = []

    for seg in PRICE_SEGMENTS:
        result.append(
            {
                "label": seg["label"],
                "value": (
                    (df["base_price"] >= seg["value_min"])
                    & (df["base_price"] <= seg["value_max"])
                ).sum(),
            }
        )

    return result


def generate_binary_cols_proportion_plots(df: pd.DataFrame, column_name: str):
    # Identifier la colonne binaire
    binary_cols = [col for col in df.columns if col == column_name]

    if len(binary_cols) == 0:
        return None

    result = [
        {
            "label": "True",
            "value": (
                (df[binary_cols[0]] == 1).sum() / len(df) * 100 if len(df) > 0 else 0
            ),
        },
        {
            "label": "False",
            "value": (
                (df[binary_cols[0]] == 0).sum() / len(df) * 100 if len(df) > 0 else 0
            ),
        },
    ]

    return result


def layout_plots(n_cols: int):
    # Calculer le nombre de lignes nécessaires
    actual_rows = (n_cols + 2) // 3  # 3 colonnes par ligne

    # Limiter à 3 colonnes maximum
    actual_cols = min(n_cols, 3)

    # Créer la figure
    fig, axes = plt.subplots(actual_rows, actual_cols, figsize=(12, 12))

    # Normaliser axes pour toujours retourner un tableau 1D itérable
    if n_cols == 1:
        axes = [axes]
    elif actual_rows == 1 and actual_cols == 1:
        axes = [axes]
    elif actual_rows == 1:
        axes = list(axes)
    else:
        axes = list(axes.flatten())

    return fig, axes


def draw_binary_circular_plots(data: list, name: str, axe: plt.Axes):
    colors = sns.color_palette("crest")
    values = [item["value"] for item in data]
    labels = [item["label"] for item in data]

    wedges, texts, autotexts = axe.pie(
        values,
        labels=labels,
        colors=colors,
        autopct="%1.1f%%",
        startangle=90,
        shadow=False,
    )

    for autotext in autotexts:
        autotext.set_color("white")
        autotext.set_fontweight("bold")

    axe.set_title(f"{name}", fontweight="bold", pad=20)


def draw_proportion_plots(
    cpt: int, axes, df: pd.DataFrame, figure_name: str, save_file=False
):
    # Masquer les axes inutilisés
    for j in range(cpt, len(axes)):
        axes[j].axis("off")

    plt.subplots_adjust(hspace=0.4, wspace=0.4)
    plt.suptitle(
        f"{figure_name} - Analyse exploratoire sur les {len(df)} jeux",
        fontsize=14,
        fontweight="bold",
    )
    if save_file:
        plt.savefig(os.path.join(OUTPUT_EXPLO_PLOTS_PATH, figure_name))
    plt.tight_layout(pad=0.5, w_pad=0.5, h_pad=0.5)
    plt.show()


# Price distribution stats


def histogram_base_price_frequence(df: pd.DataFrame, axe: plt.Axes):

    # # Créer l'histogramme
    counts, bin_edges = np.histogram(df["base_price"], bins=100)

    # Tracer avec bar()
    axe.bar(
        bin_edges[:-1],  # Utiliser les bords gauches (sans le dernier)
        counts,
        color="steelblue",
        edgecolor="black",
        alpha=0.7,
        align="center",
    )

    # Personnaliser le graphique
    axe.set_xlabel("Prix euro", fontsize=12)
    axe.set_ylabel("Frequence", fontsize=12)
    axe.set_title("Distribution des prix", fontsize=14, fontweight="bold")
    axe.grid(axis="y", alpha=0.3, linestyle="--")
    axe.set_xticks(range(0, 100, 5))

    # Ajuster les limites de l'axe x
    axe.set_xlim(0, 100)

    # Afficher des statistiques
    mean_price = df["base_price"].mean()
    median_price = df["base_price"].median()

    axe.axvline(
        mean_price,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Moyenne: {mean_price:.2f}",
    )
    axe.axvline(
        median_price,
        color="green",
        linestyle="--",
        linewidth=2,
        label=f"Mediane: {median_price:.2f}",
    )
    axe.legend()


def histogram_base_price_unique_count(df: pd.DataFrame, axe: plt.Axes):
    price_counts = df["base_price"].value_counts().sort_index()
    axe.barh(price_counts.index, price_counts.values)
    axe.set_title("Frequence des prix uniques", fontsize=14)

    axe.set_xlabel("Frequence", fontsize=12)
    axe.set_ylabel("Prix", fontsize=12)

    axe.grid(axis="x", alpha=0.3)
    axe.set_ylim(top=90)
    # axe.set_yticks(range(5, 100, 5))


def histogram_base_price_unique_count_top(df: pd.DataFrame, axe: plt.Axes):
    # Compter les occurrences et prendre les plus fréquents
    top = df["base_price"].value_counts().head(15).sort_values()

    axe.barh(  # barh au lieu de bar pour horizontal
        range(len(top)),  # Positions sur l'axe y
        top.values,  # Fréquences sur l'axe x
        color="steelblue",
        edgecolor="black",
        alpha=0.7,
    )

    # Configurer les étiquettes de l'axe y avec les valeurs de base_price
    axe.set_yticks(range(len(top)))
    axe.set_yticklabels(top.index)

    # Labels inversés
    # axe.set_ylabel("Base Price")
    # axe.set_xlabel("Fréquence")
    axe.set_title("Top 15 des prix les plus fréquents")


def prices_distribution(df: pd.DataFrame, save_file=False):

    fig = plt.figure(figsize=(12, 8))

    # Première ligne : 1 graphique qui prend toute la largeur
    ax1 = plt.subplot2grid((2, 2), (0, 0), colspan=2)

    # Deuxième ligne : 2 graphiques
    ax2 = plt.subplot2grid((2, 2), (1, 0))
    ax3 = plt.subplot2grid((2, 2), (1, 1))

    histogram_base_price_frequence(df, ax1)

    data_price = generate_base_price_proportion_data(df)
    draw_binary_circular_plots(data_price, "", ax2)

    histogram_base_price_unique_count_top(df, ax3)

    plt.tight_layout()
    plt.show()

    if save_file:
        fig.savefig(os.path.join(OUTPUT_EXPLO_PLOTS_PATH, "Price_distribution.png"))


# Distribution des genres de jeux


def histogram_genres_count(df: pd.DataFrame, axe: plt.Axes):

    # Séparer les genres et exploser
    genres_exploded = df["genres"].str.split(",").explode()

    # Nettoyer les espaces éventuels
    genres_exploded = genres_exploded.str.strip()

    # Compter les occurrences
    genre_counts = genres_exploded.value_counts()

    axe.bar(
        range(len(genre_counts)),
        genre_counts.values,
        color="steelblue",
        edgecolor="black",
        alpha=0.7,
    )

    axe.set_xticks(range(len(genre_counts)))
    axe.set_xticklabels(genre_counts.index, rotation=45, ha="right")

    axe.set_xlabel("Genre")
    axe.set_ylabel("Nombre de jeux")
    # axe.set_title("Distribution des genres de jeux")


def genres_distribution(df: pd.DataFrame, save_file=False):

    # Créer la figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 7))
    ax1: plt.Axes
    ax2: plt.Axes

    data = generate_multi_str_col_top_proportion_data(df, "genres", top_count=11)
    draw_binary_circular_plots(
        data,
        "",
        ax1,
    )

    histogram_genres_count(df, ax2)

    fig.suptitle(
        "Analyse des genres (un jeu peut avoir plusieurs genres)",
        fontsize=16,
        fontweight="bold",
    )

    plt.tight_layout(pad=2.0)
    plt.show()

    if save_file:
        fig.savefig(os.path.join(OUTPUT_EXPLO_PLOTS_PATH, "Distribution_genre.png"))


# Relation baisse de prix la plus rapide


def bar_chart_genres_discount_speed(df: pd.DataFrame, axe: plt.Axes):
    # Filtrer les jeux qui ont eu une baisse
    df_with_discount = df[df["days_to_25_percent_discount"].notna()].copy()

    # Exploser les genres
    df_with_discount["genres_list"] = df_with_discount["genres"].str.split(",")
    df_exploded = df_with_discount.explode("genres_list")
    df_exploded["genres_list"] = df_exploded["genres_list"].str.strip()

    # Calculer le nombre moyen de jours par genre
    avg_days_by_genre = df_exploded.groupby("genres_list")[
        "days_to_25_percent_discount"
    ].mean()

    # Top 10 des plus rapides
    fastest_10 = avg_days_by_genre.sort_values().head(10)

    # Graphique horizontal
    axe.barh(
        range(len(fastest_10)),
        fastest_10.values,
        color="coral",
        edgecolor="black",
        alpha=0.7,
    )

    axe.set_yticks(range(len(fastest_10)))
    axe.set_yticklabels(fastest_10.index)
    axe.invert_yaxis()  # Plus rapide en haut

    axe.set_xlabel("Nombre moyen de jours avant -25%")
    axe.set_ylabel("Genre")
    axe.set_title("Genres avec baisses de prix les plus rapides", pad=15)

    # Ajouter les valeurs sur les barres
    for i, v in enumerate(fastest_10.values):
        axe.text(v + 5, i, f"{int(v)}j", va="center")


def histogram_days_to_discount(df: pd.DataFrame, axe: plt.Axes):
    # Filtrer les valeurs non nulles
    days_data = df[df["days_to_25_percent_discount"].notna()][
        "days_to_25_percent_discount"
    ]

    axe.hist(days_data, bins=100, color="steelblue", edgecolor="black", alpha=0.7)

    axe.set_xlabel("Nombre de jours avant -25%")
    axe.set_ylabel("Nombre de jeux")
    axe.set_title("Distribution du temps avant première baisse de 25%", pad=15)
    # axe.set_xticks(range(0, 100, 5))

    # Ajuster les limites de l'axe x
    axe.set_xlim(0, 800)

    # Ajouter une ligne verticale pour la médiane
    median = days_data.median()
    axe.axvline(
        median,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Médiane: {int(median)} jours",
    )
    axe.legend()


def number_days_to_lower_price_relation(df: pd.DataFrame, save_file=False):

    # Créer la figure
    fig = plt.figure(figsize=(12, 8))

    # Première ligne : 1 graphique qui prend toute la largeur
    ax1 = plt.subplot2grid((2, 2), (0, 0), colspan=2)

    histogram_days_to_discount(df, ax1)

    plt.tight_layout()
    plt.show()

    if save_file:
        fig.savefig(os.path.join(OUTPUT_EXPLO_PLOTS_PATH, "Distribution_genre.png"))
