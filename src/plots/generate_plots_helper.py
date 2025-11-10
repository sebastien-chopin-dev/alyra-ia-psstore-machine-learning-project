from pathlib import Path
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.axes
import matplotlib.figure

from src.constants.constants import COLOR_A, COLOR_B, COLOR_C

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


def draw_binary_circular_plots(data: list, name: str, axes: plt.Axes):
    colors = sns.color_palette("crest")
    values = [item["value"] for item in data]
    labels = [item["label"] for item in data]

    wedges, texts, autotexts = axes.pie(
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

    axes.set_title(f"{name}\n", fontweight="bold")


def draw_proportion_plots(cpt: int, axes, df: pd.DataFrame, figure_name: str):
    # Masquer les axes inutilisés
    for j in range(cpt, len(axes)):
        axes[j].axis("off")

    plt.subplots_adjust(hspace=0.4, wspace=0.4)
    plt.suptitle(
        f"{figure_name} - Analyse exploratoire sur les {len(df)} jeux",
        fontsize=14,
        fontweight="bold",
    )
    plt.savefig(os.path.join(OUTPUT_EXPLO_PLOTS_PATH, figure_name))
    plt.tight_layout(pad=0.5, w_pad=0.5, h_pad=0.5)
    plt.show()


def histogram_base_price_distribution(df: pd.DataFrame):
    # df_filtered = df[(df["base_price"] >= 5) & (df["base_price"] <= 80)]

    # Créer la figure
    plt.figure(figsize=(12, 6))

    # Créer l'histogramme
    plt.hist(
        df["base_price"],
        bins=100,
        color="steelblue",
        edgecolor="black",
        alpha=0.7,
    )

    # Personnaliser le graphique
    plt.xlabel("Prix (€)", fontsize=12)
    plt.ylabel("Fréquence", fontsize=12)
    plt.title("Distribution des prix", fontsize=14, fontweight="bold")
    plt.grid(axis="y", alpha=0.3, linestyle="--")
    plt.xticks(range(5, 100, 5))

    # Ajuster les limites de l'axe x
    plt.xlim(0, 100)

    # Afficher des statistiques
    mean_price = df["base_price"].mean()
    median_price = df["base_price"].median()

    plt.axvline(
        mean_price,
        color="red",
        linestyle="--",
        linewidth=2,
        label=f"Moyenne: {mean_price:.2f}€",
    )
    plt.axvline(
        median_price,
        color="green",
        linestyle="--",
        linewidth=2,
        label=f"Médiane: {median_price:.2f}€",
    )

    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_EXPLO_PLOTS_PATH, "Price_distribution.png"))
    plt.show()
