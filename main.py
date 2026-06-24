from fastapi import FastAPI
import mlflow.sklearn
import pandas as pd
import uvicorn

mlflow.set_tracking_uri("http://localhost:5000")

app = FastAPI(title="Sales Forecast API")

RUN_ID = "1545e54a108d4a82a50d2b5e86d12fac"
model = mlflow.sklearn.load_model(f"runs:/{RUN_ID}/demand_model")

ALL_COLUMNS = [
    "product_headphones", "product_laptop", "product_monitor",
    "product_phone", "product_tablet",
    "region_APAC", "region_EMEA", "region_LATAM", "region_NA"
]

@app.get("/")
def root():
    return {"status": "running", "service": "Sales Forecast API"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.get("/predict")
def predict(product: str, region: str):
    df = pd.get_dummies(pd.DataFrame([{"product": product, "region": region}]))
    for col in ALL_COLUMNS:
        if col not in df.columns:
            df[col] = 0
    df = df[ALL_COLUMNS]
    prediction = model.predict(df)[0]
    return {
        "product": product,
        "region": region,
        "forecast_revenue": round(float(prediction), 2)
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)