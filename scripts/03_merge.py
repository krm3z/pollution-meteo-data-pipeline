import pandas as pd
import os
from math import radians, sin, cos, sqrt, atan2

print("=== ETAPE 3 : MERGE GEOSPATIAL ===")

# =========================
# 1. LECTURE
# =========================
df_pollution = pd.read_csv("output/pollution_clean.csv")
df_meteo = pd.read_csv("output/meteo_clean.csv")

# fichier stations pollution à la racine du projet
stations_file = "stations_pollution.xls"

print("\nTailles initiales :")
print("Pollution :", df_pollution.shape)
print("Meteo :", df_meteo.shape)

# =========================
# 2. LIRE LES STATIONS POLLUTION
# =========================
df_stations = pd.read_excel(stations_file, sheet_name="AirQualityStations")

# garder seulement les colonnes utiles
df_stations = df_stations[["NatlStationCode", "Name", "Latitude", "Longitude"]].copy()

# renommer
df_stations = df_stations.rename(columns={
    "NatlStationCode": "code_site",
    "Name": "nom_station_officiel",
    "Latitude": "latitude_pollution",
    "Longitude": "longitude_pollution"
})

# enlever lignes vides
df_stations = df_stations.dropna(subset=["code_site", "latitude_pollution", "longitude_pollution"])
df_stations = df_stations.drop_duplicates(subset=["code_site"])

print("\nStations pollution :", df_stations.shape)
print(df_stations.head())

# =========================
# 3. PREPARER LES STATIONS METEO UNIQUES
# =========================
df_meteo_stations = df_meteo[["station_meteo", "latitude", "longitude"]].dropna().drop_duplicates().copy()

df_meteo_stations = df_meteo_stations.rename(columns={
    "latitude": "latitude_meteo",
    "longitude": "longitude_meteo"
})

print("\nStations meteo uniques :", df_meteo_stations.shape)
print(df_meteo_stations.head())

# =========================
# 4. FONCTION DISTANCE HAVERSINE
# =========================
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # rayon Terre en km

    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c

# =========================
# 5. ASSOCIER CHAQUE STATION POLLUTION A LA STATION METEO LA PLUS PROCHE
# =========================
mapping_rows = []

for _, poll_row in df_stations.iterrows():
    best_station = None
    best_distance = None

    for _, meteo_row in df_meteo_stations.iterrows():
        dist = haversine(
            poll_row["latitude_pollution"],
            poll_row["longitude_pollution"],
            meteo_row["latitude_meteo"],
            meteo_row["longitude_meteo"]
        )

        if best_distance is None or dist < best_distance:
            best_distance = dist
            best_station = meteo_row["station_meteo"]

    mapping_rows.append({
        "code_site": poll_row["code_site"],
        "nom_station_officiel": poll_row["nom_station_officiel"],
        "latitude_pollution": poll_row["latitude_pollution"],
        "longitude_pollution": poll_row["longitude_pollution"],
        "station_meteo": best_station,
        "distance_km": round(best_distance, 2) if best_distance is not None else None
    })

df_mapping = pd.DataFrame(mapping_rows)

print("\nMapping pollution -> meteo :", df_mapping.shape)
print(df_mapping.head())

# sauvegarde utile pour debug / oral
os.makedirs("output", exist_ok=True)
df_mapping.to_csv("output/station_mapping.csv", index=False)

# =========================
# 6. AJOUTER LE MAPPING AUX DONNEES POLLUTION
# =========================
df_pollution["datetime"] = pd.to_datetime(df_pollution["datetime"], errors="coerce")
df_meteo["datetime"] = pd.to_datetime(df_meteo["datetime"], errors="coerce")

# enlever timezone météo si présente
try:
    df_meteo["datetime"] = df_meteo["datetime"].dt.tz_localize(None)
except Exception:
    pass

# arrondir à l'heure
df_pollution["datetime_hour"] = df_pollution["datetime"].dt.floor("h")
df_meteo["datetime_hour"] = df_meteo["datetime"].dt.floor("h")

# joindre stations pollution -> station météo la plus proche
df_pollution = df_pollution.merge(df_mapping, on="code_site", how="left")

print("\nPollution après ajout mapping :", df_pollution.shape)
print(df_pollution.head())

# =========================
# 7. AGREGER METEO PAR STATION + HEURE
# =========================
df_meteo_grouped = (
    df_meteo.groupby(["station_meteo", "datetime_hour"])[["temperature", "humidity", "wind_speed", "pressure"]]
    .mean()
    .reset_index()
)

print("\nMeteo groupée par station + heure :", df_meteo_grouped.shape)
print(df_meteo_grouped.head())

# =========================
# 8. MERGE FINAL GEOSPATIAL
# =========================
df_merged = pd.merge(
    df_pollution,
    df_meteo_grouped,
    on=["station_meteo", "datetime_hour"],
    how="inner"
)

# petit nettoyage
df_merged = df_merged.drop_duplicates()

print("\nDataset fusionné géospatial :", df_merged.shape)
print(df_merged.head())

# =========================
# 9. GARDER LES COLONNES UTILES POUR LA SUITE
# =========================
cols_to_keep = [
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
    "pressure"
]

cols_to_keep = [c for c in cols_to_keep if c in df_merged.columns]
df_merged = df_merged[cols_to_keep].copy()

# =========================
# 10. SAUVEGARDE
# =========================
df_merged.to_csv("output/merged_dataset.csv", index=False)

print("\nFichier généré : output/merged_dataset.csv")
print("\nColonnes finales merge :")
print(df_merged.columns.tolist())

print("\n=== FIN ETAPE 3 ===")