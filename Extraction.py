import os
import sys
import pandas as pd
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine
import logging


if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load config
load_dotenv("config.env")
API_KEY = os.getenv("API_KEY")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_NAME = os.getenv("DB_NAME")

DATA_LAKE_DIR = "DataLake"
EXTRACT_DIR = "extracted"
os.makedirs(EXTRACT_DIR, exist_ok=True)

def add_metadata(df, source):
    df["extracted_at"] = datetime.now(timezone.utc).isoformat()
    df["data_source"] = source
    return df

def save_csv(df, filename):
    path = os.path.join(EXTRACT_DIR, filename)
    df.to_csv(path, index=False)
    logger.info(f"[OK] Saved: {path} ({len(df)} rows)")
    return True

def extract_api():
    logger.info("Extracting API data...")
    try:
        url = f"https://openexchangerates.org/api/latest.json?app_id={API_KEY}"
        data = requests.get(url, timeout=30).json()
        df = pd.DataFrame([{
            'base': data.get('base'),
            'timestamp': data.get('timestamp'),
            'rates': str(data.get('rates', {}))
        }])
        return save_csv(add_metadata(df, "API"), "exchange_rates.csv")
    except Exception as e:
        logger.error(f"API extraction failed: {e}")
        return False

def extract_mysql():
    logger.info("Extracting MySQL tables...")
    try:
        engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        tables = ["orders", "order_items"]
        results = [save_csv(add_metadata(pd.read_sql(f"SELECT * FROM {tbl}", engine), f"MySQL:{tbl}"), f"{tbl}.csv") for tbl in tables]
        engine.dispose()
        return all(results)
    except Exception as e:
        logger.error(f"MySQL extraction failed: {e}")
        return False

def extract_datalake():
    logger.info("Extracting Data Lake CSVs...")
    if not os.path.exists(DATA_LAKE_DIR): 
        logger.warning(f"No Data Lake directory '{DATA_LAKE_DIR}'")
        return False
    files = [f for f in os.listdir(DATA_LAKE_DIR) if f.endswith(".csv")]
    results = []
    for f in files:
        try:
            df = pd.read_csv(os.path.join(DATA_LAKE_DIR, f))
            results.append(save_csv(add_metadata(df, f"DataLake:{f}"), f))
        except Exception as e:
            logger.error(f"Error extracting {f}: {e}")
            results.append(False)
    return all(results)

def generate_report():
    files = [f for f in os.listdir(EXTRACT_DIR) if f.endswith(".csv")]
    if not files: return
    summary = pd.DataFrame([{
        'File': f,
        'Rows': len(pd.read_csv(os.path.join(EXTRACT_DIR, f))),
        'Columns': len(pd.read_csv(os.path.join(EXTRACT_DIR, f)).columns)
    } for f in files])
    logger.info("\n" + summary.to_string(index=False))

def main():
    logger.info("=== START EXTRACTION PIPELINE ===")
    results = {
        'API': extract_api(),
        'MySQL': extract_mysql(),
        'DataLake': extract_datalake()
    }
    generate_report()
    for src, ok in results.items():
        logger.info(f"{src}: {'[OK]' if ok else '[FAILED]'}")
    logger.info("=== EXTRACTION PIPELINE COMPLETED ===")

if __name__ == "__main__":
    main()
