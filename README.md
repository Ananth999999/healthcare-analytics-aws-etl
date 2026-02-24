# Healthcare Analytics ETL Pipeline on AWS (Simulated)

This project demonstrates a realistic **healthcare-style analytics ETL pipeline** built with **Python**, designed to mirror common data engineering workflows on AWS (S3-style raw landing → validation → transformation → analytics-ready output).  
It focuses on **data quality**, **schema enforcement**, **deduplication**, and **metrics reporting**—the same concerns typically required in production analytics pipelines.

> Note: This repository uses sample (synthetic) data and a local file structure to simulate S3-style ingestion.

---

## What This Pipeline Does

### Input (Raw Layer)
- Reads raw claims-style data from: `data/raw/claims_sample.csv`

### Validation
- Enforces required columns using `etl/schema.json`
- Applies type casting (e.g., numeric conversion for amounts)
- Flags invalid values by coercing bad numeric values to null

### Transformations (Curated Layer)
- Deduplicates by `claim_id`
- Drops records with missing `claim_amount`
- Adds derived field `claim_amount_usd`
- Produces analytics-ready output

### Output
- Writes processed dataset to: `output/processed_claims.csv`
- Generates a run summary report: `reports/run_report.txt`

---

## Repository Structure

healthcare-analytics-aws-etl/  
 ├── data/raw/  
 │    └── claims_sample.csv  
 ├── etl/  
 │    ├── etl_pipeline.py  
 │    └── schema.json  
 ├── output/  
 │    └── processed_claims.csv  
 ├── reports/  
 │    └── run_report.txt  
 ├── README.md  
 └── requirements.txt  

---

## How to Run

### 1) Run the ETL pipeline
python etl/etl_pipeline.py

### 2) Check outputs
- Processed file: `output/processed_claims.csv`
- Report file: `reports/run_report.txt`

---

## Key Engineering Practices Demonstrated

- Schema validation + required-field checks
- Type casting + coercion handling for bad numeric values
- Deduplication strategy for repeated claim records
- Curated output for analytics consumption
- Lightweight reporting for operational visibility

---

## Tech Stack

- Python (pandas)
- JSON schema configuration
- Git / GitHub
