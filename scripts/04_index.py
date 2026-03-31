import pandas as pd
import os

print("=== ETAPE 4 : DATASET FINAL ===")

# =========================
# 1. LECTURE
# =========================
df = pd.read_csv("output/merged_dataset.csv")

print("\nTaille initiale :", df.shape)

# =========================
# 2. CONVERSION NUMERIQUE
# =========================
numeric_cols = ["NO2", "O3", "PM10", "PM25", "temperature", "humidity", "wind_speed", "pressure"]
for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

# =========================
# 3. FILTRE QUALITE
# garder seulement les lignes avec au moins 2 polluants
# =========================
pollutants = ["NO2", "O3", "PM10", "PM25"]
df["nb_polluants_dispo"] = df[pollutants].notna().sum(axis=1)

df = df.dropna(subset=["datetime", "code_site", "nom_site"])
df = df[df["nb_polluants_dispo"] >= 2].copy()

# supprimer lignes sans données pollution
df = df.dropna(subset=pollutants, how="all")

print("\nAprès filtre qualité :", df.shape)

# =========================
# 4. NORMALISATION
# =========================
def normalize(series):
    s = pd.to_numeric(series, errors="coerce")
    min_val = s.min()
    max_val = s.max()
    if pd.isna(min_val) or pd.isna(max_val) or max_val == min_val:
        return pd.Series([0] * len(s), index=s.index)
    return (s - min_val) / (max_val - min_val)

for col in pollutants:
    df[f"{col}_norm"] = normalize(df[col].fillna(df[col].median()))

df["wind_norm"] = normalize(df["wind_speed"].fillna(df["wind_speed"].median()))

# =========================
# 5. SCORE POLLUTION
# =========================
df["pollution_score"] = (
    0.30 * df["NO2_norm"] +
    0.20 * df["O3_norm"] +
    0.25 * df["PM10_norm"] +
    0.25 * df["PM25_norm"]
)

# =========================
# 6. IMPACT FINAL
# =========================
df["impact_index"] = (df["pollution_score"] * (1 - 0.30 * df["wind_norm"])) * 100
df["impact_index"] = df["impact_index"].clip(0, 100).round(2)

df = df.dropna(subset=["impact_index"])

def classify_impact(x):
    if x <= 25:
        return "faible"
    elif x <= 50:
        return "modere"
    elif x <= 75:
        return "eleve"
    return "critique"

df["impact_level"] = df["impact_index"].apply(classify_impact)

# =========================
# 7. GARDER SEULEMENT LE NECESSAIRE
# =========================
final_cols = [
    "datetime",
    "code_site",
    "nom_site",
    "latitude_pollution",
    "longitude_pollution",
    "station_meteo",
    "distance_km",
    "NO2",
    "O3",
    "PM10",
    "PM25",
    "temperature",
    "humidity",
    "wind_speed",
    "pressure",
    "impact_index",
    "impact_level"
]

# garder seulement les colonnes existantes
final_cols = [c for c in final_cols if c in df.columns]
df = df[final_cols].copy()

# =========================
# 8. ARRONDIS
# =========================
for col in [
    "NO2", "O3", "PM10", "PM25",
    "temperature", "humidity", "wind_speed", "pressure",
    "distance_km", "latitude_pollution", "longitude_pollution",
    "impact_index"
]:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

# =========================
# 9. TRI FINAL
# =========================
df = df.sort_values(by=["datetime", "code_site"], ascending=[True, True])

# datetime en texte pour JSON
df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

# =========================
# 10. NETTOYAGE FINAL ANTI-NULL
# =========================

# colonnes numériques : remplacement par médiane
numeric_fill_cols = [
    "NO2", "O3", "PM10", "PM25",
    "temperature", "humidity", "wind_speed", "pressure",
    "distance_km", "latitude_pollution", "longitude_pollution",
    "impact_index"
]

for col in numeric_fill_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        median_value = df[col].median()
        df[col] = df[col].fillna(median_value)

# colonnes texte : remplacement par valeur explicite
text_fill_cols = [
    "code_site", "nom_site", "station_meteo", "impact_level"
]

for col in text_fill_cols:
    if col in df.columns:
        df[col] = df[col].fillna("unknown")

# datetime : au cas où
if "datetime" in df.columns:
    df["datetime"] = df["datetime"].fillna("unknown")

# sécurité finale : supprimer toute ligne encore incomplète
df = df.dropna()

print("\nAprès suppression des null :", df.shape)

# =========================
# 11. SAUVEGARDE FINALE UNIQUE
# =========================
os.makedirs("output", exist_ok=True)
df.to_csv("output/final_dataset.csv", index=False)
df.to_json("output/final_dataset.json", orient="records", indent=2, force_ascii=False)

print("\nTaille finale :", df.shape)
print("\nColonnes finales :")
print(df.columns.tolist())

print("\nAperçu :")
print(df.head())

print("\nFichiers générés :")
print("- output/final_dataset.csv")
print("- output/final_dataset.json")

print("\n=== FIN ETAPE 4 ===")