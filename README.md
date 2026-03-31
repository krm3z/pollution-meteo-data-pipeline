# Challenge 48h - Partie Data

## Objectif
Construire un pipeline data qui fusionne des données de pollution atmosphérique et des données météo, puis produire un indice d’impact exploitable par l’application.

## Sources
- Pollution : données temps réel de pollution atmosphérique réglementée
- Météo : archive SYNOP OMM

## Étapes du pipeline
1. Récupération des fichiers OpenData
2. Nettoyage des données pollution
3. Nettoyage des données météo
4. Jointure géospatiale : association de chaque station pollution à la station météo la plus proche
5. Fusion par station associée et par heure
6. Calcul d’un impact_index entre 0 et 100
7. Export final en CSV et JSON
8. Exposition via API FastAPI

## Fichiers finaux
- `output/final_dataset.csv`
- `output/final_dataset.json`
- `output/station_mapping.csv`

## Description des champs
- `datetime` : date et heure de la mesure
- `code_site` : identifiant station pollution
- `nom_site` : nom station pollution
- `latitude_pollution`, `longitude_pollution` : coordonnées station pollution
- `station_meteo` : station météo la plus proche
- `distance_km` : distance entre station pollution et station météo
- `NO2`, `O3`, `PM10`, `PM25` : concentrations des polluants
- `temperature`, `humidity`, `wind_speed`, `pressure` : données météo associées
- `impact_index` : indice de 0 à 100
- `impact_level` : faible / modere / eleve / critique

## API
- `/` : test API
- `/data` : récupérer les données
- `/stats` : statistiques globales
- `/sites` : liste des sites
- `/health` : état de l’API