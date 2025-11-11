import sqlite3
import pandas as pd
import logging
import os

def load_data(df, database_path, table_name):
    """
    Load transformed data into SQLite database
    Args:
        df (pandas.DataFrame): Transformed data
        database_path (str): Path to SQLite database file
        table_name (str): Name of the target table
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logging.info(f"Starting data loading to database {database_path}")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(database_path), exist_ok=True)
        
        # Create database connection
        conn = sqlite3.connect(database_path)
        
        # Write DataFrame to database
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        
        # Validate loaded data
        validation_df = pd.read_sql(f"SELECT COUNT(*) as row_count FROM {table_name}", conn)
        loaded_rows = validation_df.iloc[0]['row_count']
        
        conn.close()
        
        logging.info(f"Successfully loaded {loaded_rows} rows into table {table_name}")
        return True
        
    except Exception as e:
        logging.error(f"Error during data loading: {e}")
        return False

# Test this module independently
if __name__ == "__main__":
    from extract import extract_data, load_config
    from transform import transform_data
    
    config = load_config()
    raw_df = extract_data(config['input']['file_path'])
    transformed_df = transform_data(raw_df)
    success = load_data(transformed_df, 
                       config['output']['database_path'], 
                       config['output']['table_name'])
    print(f"Data loading test successful: {success}")