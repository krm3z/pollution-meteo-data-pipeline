import pandas as pd
import os

os.makedirs("output", exist_ok=True)

print("=== ETAPE 2 : NETTOYAGE ===")

# =========================
# 1. LECTURE
# =========================
df_pollution = pd.read_csv("output/pollution_concat.csv")
df_meteo = pd.read_csv("meteo.csv.gz", sep=";", compression="gzip")

print("\nTailles initiales :")
print("Pollution :", df_pollution.shape)
print("Meteo :", df_meteo.shape)

# =========================
# 2. NETTOYAGE POLLUTION
# =========================
# Garder seulement les colonnes utiles
pollution_cols = [
    "Date de début",
    "code site",
    "nom site",
    "Polluant",
    "valeur",
    "unité de mesure"
]

df_pollution = df_pollution[pollution_cols].copy()

# Renommer
df_pollution = df_pollution.rename(columns={
    "Date de début": "datetime",
    "code site": "code_site",
    "nom site": "nom_site",
    "Polluant": "polluant",
    "valeur": "valeur",
    "unité de mesure": "unite"
})

# Convertir date
df_pollution["datetime"] = pd.to_datetime(df_pollution["datetime"], errors="coerce")

# Convertir valeur en numérique
df_pollution["valeur"] = pd.to_numeric(df_pollution["valeur"], errors="coerce")

# Garder seulement certains polluants
polluants_utiles = ["NO2", "PM10", "PM2.5", "O3"]
df_pollution = df_pollution[df_pollution["polluant"].isin(polluants_utiles)].copy()

# Supprimer lignes inutiles
df_pollution = df_pollution.dropna(subset=["datetime", "code_site", "polluant", "valeur"])
df_pollution = df_pollution.drop_duplicates()

print("\nPollution après nettoyage :", df_pollution.shape)
print(df_pollution.head())

# =========================
# 3. PASSAGE EN FORMAT LARGE
# =========================
df_pollution_wide = df_pollution.pivot_table(
    index=["datetime", "code_site", "nom_site"],
    columns="polluant",
    values="valeur",
    aggfunc="mean"
).reset_index()

# Nettoyer les noms de colonnes
df_pollution_wide.columns.name = None
df_pollution_wide = df_pollution_wide.rename(columns={
    "PM2.5": "PM25"
})

print("\nPollution format large :", df_pollution_wide.shape)
print(df_pollution_wide.head())

# =========================
# 4. NETTOYAGE METEO
# =========================
meteo_cols = [
    "lat", "lon", "name", "reference_time",
    "t", "u", "ff", "pres"
]

df_meteo = df_meteo[meteo_cols].copy()

df_meteo = df_meteo.rename(columns={
    "lat": "latitude",
    "lon": "longitude",
    "name": "station_meteo",
    "reference_time": "datetime",
    "t": "temperature",
    "u": "humidity",
    "ff": "wind_speed",
    "pres": "pressure"
})

df_meteo["datetime"] = pd.to_datetime(df_meteo["datetime"], errors="coerce")

for col in ["latitude", "longitude", "temperature", "humidity", "wind_speed", "pressure"]:
    df_meteo[col] = pd.to_numeric(df_meteo[col], errors="coerce")

df_meteo = df_meteo.dropna(subset=["datetime", "latitude", "longitude"])
df_meteo = df_meteo.drop_duplicates()

print("\nMeteo après nettoyage :", df_meteo.shape)
print(df_meteo.head())

# =========================
# 5. SAUVEGARDE
# =========================
df_pollution_wide.to_csv("output/pollution_clean.csv", index=False)
df_meteo.to_csv("output/meteo_clean.csv", index=False)

print("\nFichiers générés :")
print("- output/pollution_clean.csv")
print("- output/meteo_clean.csv")
print("\n=== FIN ETAPE 2 ===")