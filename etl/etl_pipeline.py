import pandas as pd
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
OUTPUT_DIR = BASE_DIR / "output"
SCHEMA_PATH = BASE_DIR / "etl" / "schema.json"
REPORT_PATH = BASE_DIR / "reports" / "run_report.txt"

def load_schema():
    with open(SCHEMA_PATH) as f:
        return json.load(f)

def validate_columns(df, schema):
    missing = [col for col in schema["required_columns"] if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

def apply_type_casting(df, schema):
    for col, dtype in schema["column_types"].items():
        if col in df.columns:
            if dtype == "float":
                df[col] = pd.to_numeric(df[col], errors="coerce")
            else:
                df[col] = df[col].astype(str)
    return df

def transform(df):
    df = df.drop_duplicates(subset=["claim_id"])
    df = df.dropna(subset=["claim_amount"])
    df["claim_amount_usd"] = df["claim_amount"] * 1.0
    return df

def write_report(df):
    with open(REPORT_PATH, "w") as f:
        f.write(f"Records after processing: {len(df)}\n")
        f.write(f"Total claim amount: {df['claim_amount'].sum():.2f}\n")

def run():
    schema = load_schema()
    files = list(RAW_DIR.glob("*.csv"))
    if not files:
        raise FileNotFoundError("No input CSV files found in data/raw")

    df = pd.read_csv(files[0])
    validate_columns(df, schema)
    df = apply_type_casting(df, schema)
    df = transform(df)

    OUTPUT_DIR.mkdir(exist_ok=True)
    df.to_csv(OUTPUT_DIR / "processed_claims.csv", index=False)

    write_report(df)
    print("ETL pipeline executed successfully")

if __name__ == "__main__":
    run()
