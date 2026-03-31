from fastapi import FastAPI
import pandas as pd
import json

app = FastAPI(title="Challenge 48h Data API")

df = pd.read_csv("output/final_dataset.csv")
df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")


def dataframe_to_clean_json(dataframe: pd.DataFrame):
    df_clean = dataframe.copy()

    if "datetime" in df_clean.columns:
        df_clean["datetime"] = pd.to_datetime(df_clean["datetime"], errors="coerce")

    # pandas gère les NaN -> null ici
    json_str = df_clean.to_json(orient="records", date_format="iso")
    return json.loads(json_str)


@app.get("/")
def home():
    return {"message": "API Challenge 48h OK"}


@app.get("/data")
def get_data(limit: int = 100, code_site: str | None = None, impact_level: str | None = None):
    result = df.copy()

    if code_site:
        result = result[result["code_site"] == code_site]

    if impact_level:
        result = result[result["impact_level"] == impact_level]

    result = result.head(limit)

    return dataframe_to_clean_json(result)


@app.get("/stats")
def get_stats():
    stats = {
        "rows": int(len(df)),
        "columns": df.columns.tolist(),
        "impact_mean": None,
        "impact_min": None,
        "impact_max": None
    }

    if "impact_index" in df.columns:
        impact = pd.to_numeric(df["impact_index"], errors="coerce")
        if impact.notna().any():
            stats["impact_mean"] = float(impact.mean())
            stats["impact_min"] = float(impact.min())
            stats["impact_max"] = float(impact.max())

    return stats


@app.get("/sites")
def get_sites(limit: int = 100):
    result = (
        df[["code_site", "nom_site", "latitude_pollution", "longitude_pollution"]]
        .drop_duplicates()
        .head(limit)
    )

    return dataframe_to_clean_json(result)


@app.get("/health")
def health():
    return {"status": "ok"}