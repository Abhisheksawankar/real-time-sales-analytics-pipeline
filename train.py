import mlflow
import mlflow.sklearn
import pandas as pd
import psycopg2
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_absolute_error


mlflow.set_tracking_uri("http://localhost:5000")
mlflow.set_experiment("sales-demand-forecast")

conn = psycopg2.connect(
    host="localhost", port=5432,
    database="sales", user="postgres", password="secret123"
)
df = pd.read_sql("SELECT * FROM sales_aggregates", conn)
conn.close()

print(f"Loaded {len(df)} rows from Postgres")

if len(df) == 0:
    print("No data yet — run producer + Spark job first")
    exit()

df_encoded = pd.get_dummies(df[['product', 'region']])
X = df_encoded
y = df['total_revenue']

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

with mlflow.start_run():
    params = {"n_estimators": 200, "max_depth": 5, "learning_rate": 0.1}
    mlflow.log_params(params)

    model = GradientBoostingRegressor(**params)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    r2  = round(r2_score(y_test, y_pred), 4)
    mae = round(mean_absolute_error(y_test, y_pred), 2)

    mlflow.log_metric("r2_score", r2)
    mlflow.log_metric("mae", mae)

    # compatible with mlflow server v2.11.1
    mlflow.sklearn.log_model(model, artifact_path="demand_model")

    print(f"Model trained — R2: {r2}, MAE: {mae}")
    print("View at http://localhost:5000")