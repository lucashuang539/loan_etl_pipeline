# loan_etl_pipeline
This is my ETL pipeline project.

## Quick Start
```bash
# 1. Install dependencies
pip install pandas pyyaml

# 2. Run pipeline (from project root)
python src/main.py

# What It Does

Extracts loan data from CSV → Transforms (cleans, filters, calculates DTI) → Loads to SQLite
Input: data/input/sample_data.csv
Output: data/output/loans.db (approved_loans table)

# Project Structure
loan_etl_pipeline/
├── config/config.yaml     # Settings (input/output paths)
├── data/input/            # Put your CSV files here
├── src/                   # ETL code
└── requirements.txt       # Dependencies

# Verify Success

After running, check:

Terminal: Shows "ETL Pipeline Completed Successfully"
Files: logs/etl_pipeline.log and data/output/loans.db are created

# Sample Data Format
application_id,applicant_name,applicant_income,loan_amount,credit_score
1001,John Doe,75000,25000,720

# Need Help?

Check logs/etl_pipeline.log for errors
Edit config/config.yaml to change file paths