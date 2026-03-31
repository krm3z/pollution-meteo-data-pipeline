import pandas as pd
import os

print("=== ETAPE 5 : EXPORT FINAL PROPRE ===")

# =========================
# 1. LECTURE
# =========================
df = pd.read_csv("output/final_dataset.csv")

print("\nTaille avant sélection :", df.shape)
print("Colonnes avant sélection :")
print(df.columns.tolist())

# =========================
# 2. GARDER UNIQUEMENT LES COLONNES UTILES
# =========================
columns_to_keep = [
    "datetime",
    "code_site",
    "nom_site",
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

# garder seulement les colonnes présentes
columns_to_keep = [col for col in columns_to_keep if col in df.columns]
df = df[columns_to_keep].copy()

# =========================
# 3. ARRONDIR CERTAINES VALEURS
# =========================
numeric_cols = ["NO2", "O3", "PM10", "PM25", "temperature", "humidity", "wind_speed", "pressure", "impact_index"]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

# =========================
# 4. TRIER LES DONNEES
# =========================
if "datetime" in df.columns:
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
    df = df.sort_values(by=["datetime", "code_site"], ascending=[True, True])

# remettre datetime en texte pour le JSON
if "datetime" in df.columns:
    df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

# =========================
# 5. SAUVEGARDE CSV LEGER
# =========================
os.makedirs("output", exist_ok=True)
df.to_csv("output/final_dataset_light.csv", index=False)

# =========================
# 6. SAUVEGARDE JSON
# =========================
df.to_json("output/final_dataset.json", orient="records", force_ascii=False, indent=2)

print("\nTaille après sélection :", df.shape)
print("Colonnes finales :")
print(df.columns.tolist())

print("\nFichiers générés :")
print("- output/final_dataset_light.csv")
print("- output/final_dataset.json")

print("\nAperçu :")
print(df.head())

print("\n=== FIN ETAPE 5 ===")