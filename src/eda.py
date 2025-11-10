from pathlib import Path
import os
import matplotlib
from mpl_toolkits.axes_grid1 import SubplotDivider
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Analyse exploratoire
from src.plots.generate_plots_helper import (
    draw_proportion_plots,
    generate_binary_cols_proportion_plots,
    generate_platform_proportion_data,
    draw_binary_circular_plots,
    histogram_base_price_distribution,
    layout_plots,
)

# generate basic stats (df)


def column_summary(df: pd.DataFrame):
    summary = []
    for col in df.columns:
        col_type = df[col].dtype
        non_null = df[col].notna().sum()
        null_count = df[col].isna().sum()
        unique_count = df[col].nunique()

        # Échantillon de valeurs
        sample_values = df[col].dropna().head(1).tolist()

        summary.append(
            {
                "Column": col,
                "Type": str(col_type),
                "Non-Null Count": non_null,
                "Null Count": null_count,
                "Unique Values": unique_count,
                "Sample Values": sample_values,
            }
        )

    # Afficher le résumé des colonnes
    print("=" * 80)
    print("Résumé détaillé des colonnes:")
    print("=" * 80)
    column_summary_df = pd.DataFrame(summary)
    print(column_summary_df.to_string(index=False))
    print("\n")

    return pd.DataFrame(summary)


# plot games analysis (df)


def generate_proportion_analyse_plot(df: pd.DataFrame):
    data_plt = generate_platform_proportion_data(df)
    list_cols_true_false_1 = [
        {"col": "is_dlc", "title": "Catégorie DLC"},
        {"col": "is_indie", "title": "De la catégorie Indie"},
        {"col": "is_vr", "title": "Mode VR disponible"},
        {"col": "is_opti_ps5_pro", "title": "Optimisé PS5 PRO"},
        {"col": "is_ps_exclusive", "title": "Exclusif PlayStation"},
    ]

    list_cols_true_false_2 = [
        {"col": "has_local_multiplayer", "title": "Mode multi local disponible"},
        {"col": "has_online_multiplayer", "title": "Mode multi en ligne disponible"},
        {"col": "is_online_only", "title": "Jeu en ligne uniquement"},
        {"col": "has_microtransactions", "title": "Microtransactions présentes"},
    ]

    fig, axes = layout_plots(6)

    draw_binary_circular_plots(data_plt, "Plateforme", axes[0])

    cpt = 1
    for col_plt in list_cols_true_false_1:
        data_plt = generate_binary_cols_proportion_plots(df, col_plt["col"])
        if data_plt is not None:
            draw_binary_circular_plots(data_plt, col_plt["title"], axes[cpt])
            cpt += 1

    draw_proportion_plots(cpt, axes, df, "Figure_1")

    fig, axes = layout_plots(4)
    cpt = 0
    for col_plt in list_cols_true_false_2:
        data_plt = generate_binary_cols_proportion_plots(df, col_plt["col"])
        if data_plt is not None:
            draw_binary_circular_plots(data_plt, col_plt["title"], axes[cpt])
            cpt += 1

    draw_proportion_plots(cpt, axes, df, "Figure_2")


# plot correlation matrix (df)

# create eda report (df)


def create_eda_report(df: pd.DataFrame):
    generate_proportion_analyse_plot(df)
    histogram_base_price_distribution(df)
