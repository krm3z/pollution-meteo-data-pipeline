import pandas as pd
import glob
import os

print("=== ETAPE 1 : LECTURE DES FICHIERS ===")

# -------------------------
# 1) LIRE TOUS LES CSV POLLUTION
# -------------------------
pollution_files = sorted(glob.glob("data/pollution/*.csv"))

print(f"\nNombre de fichiers pollution trouvés : {len(pollution_files)}")
for f in pollution_files:
    print("-", f)

pollution_dfs = []

for file in pollution_files:
    try:
        df = pd.read_csv(file, sep=";", encoding="utf-8")
        df["source_file"] = os.path.basename(file)
        pollution_dfs.append(df)
    except Exception as e:
        print(f"Erreur lecture pollution {file} : {e}")

if len(pollution_dfs) == 0:
    raise ValueError("Aucun fichier pollution lisible.")

df_pollution = pd.concat(pollution_dfs, ignore_index=True)

print("\n=== POLLUTION ===")
print(df_pollution.head())
print("\nColonnes pollution :")
print(df_pollution.columns.tolist())
print("\nTaille pollution :", df_pollution.shape)

# -------------------------
# 2) LIRE LE FICHIER METEO
# -------------------------
meteo_file = "meteo.csv.gz"

try:
    df_meteo = pd.read_csv(meteo_file, sep=";", compression="gzip", low_memory=False)
except Exception as e:
    raise ValueError(f"Erreur lecture {meteo_file} : {e}")

print("\n=== METEO ===")
print(df_meteo.head())
print("\nColonnes meteo :")
print(df_meteo.columns.tolist())
print("\nTaille meteo :", df_meteo.shape)

# -------------------------
# 3) LIRE LE FICHIER STATIONS POLLUTION
# -------------------------
stations_file = "stations_pollution.xlsx"

df_stations = None

try:
    if os.path.exists(stations_file):
        df_stations = pd.read_excel(stations_file)
    else:
        print(f"\nFichier stations introuvable : {stations_file}")
except Exception as e:
    print(f"\nErreur lecture fichier stations : {e}")
    print("Si besoin, ouvre le fichier dans Excel et enregistre-le en .xlsx")

if df_stations is not None:
    print("\n=== STATIONS POLLUTION ===")
    print(df_stations.head())
    print("\nColonnes stations :")
    print(df_stations.columns.tolist())
    print("\nTaille stations :", df_stations.shape)

# -------------------------
# 4) SAUVEGARDE DU CSV POLLUTION CONCATENE
# -------------------------
os.makedirs("output", exist_ok=True)
df_pollution.to_csv("output/pollution_concat.csv", index=False)

print("\nFichier sauvegardé : output/pollution_concat.csv")
print("\n=== FIN ETAPE 1 ===")