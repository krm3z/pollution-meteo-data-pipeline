# 🌍 Pollution & Weather Data Pipeline

## 📌 Contexte
Projet réalisé dans le cadre d’un challenge data en groupe (48h).

👉 Cette version correspond à mon implémentation personnelle incluant :
- une jointure géospatiale avancée
- un pipeline de traitement complet
- une API pour exploiter les données

---

## 🎯 Objectif
Construire un pipeline data capable de :

- nettoyer des données de pollution atmosphérique
- intégrer des données météo
- associer les stations entre elles (géographiquement)
- générer un indice d’impact environnemental
- exposer les données via une API

---

## 📊 Données utilisées

### 🌫️ Pollution
- mesures horaires de qualité de l’air (NO2, O3, PM10, PM25)
- stations réparties sur le territoire

### 🌦️ Météo
- données SYNOP (température, humidité, vent, pression)
- stations météo OMM

---

## ⚙️ Pipeline Data

### 1. Nettoyage des données
- suppression des valeurs invalides
- conversion des types
- normalisation des formats

### 2. Jointure géospatiale
- calcul de distance entre stations
- association à la station météo la plus proche
- création d’un mapping pollution → météo

### 3. Fusion temporelle
- regroupement des données météo par heure
- alignement avec les données de pollution

### 4. Calcul de l’indice
- normalisation des polluants
- pondération :
  - NO2 → 30%
  - O3 → 20%
  - PM10 → 25%
  - PM25 → 25%
- prise en compte du vent (dispersion)
- score final entre 0 et 100

### 5. Classification
- faible
- modéré
- élevé
- critique

---

## 📦 Données finales

Fichiers générés :

- `output/final_dataset.csv`
- `output/final_dataset.json`
- `output/station_mapping.csv`

### Colonnes principales

- `datetime`
- `code_site`
- `nom_site`
- `latitude_pollution`, `longitude_pollution`
- `station_meteo`
- `distance_km`
- `NO2`, `O3`, `PM10`, `PM25`
- `temperature`, `humidity`, `wind_speed`, `pressure`
- `impact_index`
- `impact_level`

👉 Dataset final :
- nettoyé
- sans valeurs null
- directement exploitable

---

## 🚀 API

API construite avec **FastAPI**

### Endpoints disponibles

- `/` → test API
- `/data` → récupérer les données
- `/stats` → statistiques globales
- `/sites` → liste des stations
- `/health` → état du service

---

## 🛠️ Technologies utilisées

- Python
- Pandas
- FastAPI
- Uvicorn

---

## 💡 Points forts du projet

- jointure géospatiale réaliste
- pipeline data complet
- dataset propre et exploitable
- API fonctionnelle
- gestion des valeurs manquantes

---

## 📈 Améliorations possibles

- modèle de prédiction (régression)
- visualisation (dashboard)
- API avec filtres avancés

---

## 👤 Auteur

Kerem Uysal
