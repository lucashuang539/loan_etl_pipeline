import logging
import yaml
import sys
import os

# Add src directory to Python path for module imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.extract import extract_data, load_config
from src.transform import transform_data
from src.load import load_data

def setup_logging(log_level, log_file):
    """Configure logging system"""
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)  # Also output to console
        ]
    )

def main():
    """
    Main ETL pipeline function
    Returns:
        bool: True if pipeline executed successfully, False otherwise
    """
    try:
        # Load configuration
        config = load_config()
        
        # Setup logging
        setup_logging(config['logging']['level'], config['logging']['file_path'])
        
        logging.info("=== ETL Pipeline Started ===")
        
        # Execute ETL process
        logging.info("Phase 1: Data Extraction")
        raw_data = extract_data(config['input']['file_path'])
        
        logging.info("Phase 2: Data Transformation")
        transformed_data = transform_data(raw_data)
        
        logging.info("Phase 3: Data Loading")
        success = load_data(transformed_data, 
                          config['output']['database_path'], 
                          config['output']['table_name'])
        
        if success:
            logging.info("=== ETL Pipeline Completed Successfully ===")
            return True
        else:
            logging.error("=== ETL Pipeline Failed ===")
            return False
            
    except Exception as e:
        logging.critical(f"Critical error in ETL pipeline: {e}")
        return False

# Entry point of the script
if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)