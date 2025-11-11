import pandas as pd
import logging

def transform_data(df):
    """
    Clean and transform the extracted data
    Args:
        df (pandas.DataFrame): Raw data from extraction
    Returns:
        pandas.DataFrame: Transformed data
    """
    try:
        logging.info("Starting data transformation")
        original_rows = len(df)
        
        # 1. Handle missing values: Drop records with nulls in key fields
        df_clean = df.dropna(subset=['applicant_income', 'loan_amount', 'credit_score'])
        if len(df_clean) < original_rows:
            logging.warning(f"Removed {original_rows - len(df_clean)} records with missing values")
        
        # 2. Data type conversion
        df_clean['applicant_income'] = pd.to_numeric(df_clean['applicant_income'], errors='coerce')
        df_clean['loan_amount'] = pd.to_numeric(df_clean['loan_amount'], errors='coerce')
        df_clean['credit_score'] = pd.to_numeric(df_clean['credit_score'], errors='coerce')
        
        # 3. Create derived field: Debt-to-Income ratio
        df_clean['dti_ratio'] = (df_clean['loan_amount'] / df_clean['applicant_income']).round(2)
        
        # 4. Filter data: Only keep applications with credit score > 600
        df_filtered = df_clean[df_clean['credit_score'] > 600]
        logging.info(f"Data transformation complete. Final shape: {len(df_filtered)} rows")
        
        return df_filtered
        
    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise e

# Test this module independently
if __name__ == "__main__":
    from extract import extract_data, load_config
    config = load_config()
    raw_df = extract_data(config['input']['file_path'])
    transformed_df = transform_data(raw_df)
    print("Data transformation test successful!")
    print(transformed_df.head())