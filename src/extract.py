import pandas as pd
import yaml
import logging

def load_config():
    """Load configuration from YAML file"""
    with open('config/config.yaml', 'r') as file:
        config = yaml.safe_load(file)
    return config

def extract_data(file_path):
    """
    Extract data from CSV file
    Args:
        file_path (str): Path to the input CSV file
    Returns:
        pandas.DataFrame: Raw extracted data
    """
    try:
        logging.info(f"Starting data extraction from {file_path}")
        df = pd.read_csv(file_path)
        logging.info(f"Successfully extracted data. Shape: {len(df)} rows, {len(df.columns)} columns")
        return df
    except FileNotFoundError as e:
        logging.error(f"Data file not found: {e}")
        raise e
    except Exception as e:
        logging.error(f"Unexpected error during data extraction: {e}")
        raise e

# Test this module independently
if __name__ == "__main__":
    config = load_config()
    df = extract_data(config['input']['file_path'])
    print("Data extraction test successful!")
    print(df.head())