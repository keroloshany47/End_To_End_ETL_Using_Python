import pandas as pd
import glob, os
from datetime import datetime
import logging

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

EXTRACT_DIR = "extracted"
STAGING_DIR = "staging_1"
QUALITY_REPORT_DIR = "quality_reports"
os.makedirs(STAGING_DIR, exist_ok=True)
os.makedirs(QUALITY_REPORT_DIR, exist_ok=True)

quality_metrics = []

# ---------------------------
# Helper function
# ---------------------------
def save_cleaned(df, file_name):
    path = os.path.join(STAGING_DIR, file_name)
    df.to_csv(path, index=False)
    logger.info(f"Saved cleaned file: {path} ({len(df)} rows)")
    return path

def clean_csv(file_path):
    file_name = os.path.basename(file_path)
    df = pd.read_csv(file_path)
    original_rows = len(df)
    
    # Remove duplicates
    duplicates = df.duplicated().sum()
    if duplicates > 0:
        df = df.drop_duplicates()
    
    # File-specific null handling
    nulls_handled = 0
    if file_name == "customers.csv":
        for col in ["phone", "email", "last_name"]:
            nulls_handled += df[col].isnull().sum()
            df[col] = df[col].fillna("Unknown")
    elif file_name in ["staffs.csv", "stores.csv"]:
        df = df.fillna({"phone":"Unknown", "email":"Unknown", "zip_code":0, "store_id":0, "manager_id":0})
    elif file_name == "order_items.csv":
        before = len(df)
        df = df.dropna(subset=["order_id","product_id"])
        nulls_handled += before - len(df)
    else:
        before = len(df)
        df = df.dropna()
        nulls_handled += before - len(df)
    
    # Validation
    invalid_records = 0
    if "list_price" in df.columns:
        invalid_records += (df["list_price"]<0).sum()
        df = df[df["list_price"]>=0]
    if "quantity" in df.columns:
        invalid_records += (df["quantity"]<0).sum()
        df = df[df["quantity"]>=0]
    
    # Date parsing
    for col in ["order_date","required_date","shipped_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    if "order_date" in df.columns and "required_date" in df.columns:
        before = len(df)
        df = df.dropna(subset=["order_date","required_date"])
        invalid_records += before - len(df)
    
    # Metadata & save
    df["extracted_at"] = datetime.now().isoformat()
    df["data_source"] = file_name
    save_cleaned(df, file_name)
    
    # Store metrics
    final_rows = len(df)
    total_issues = duplicates + nulls_handled + invalid_records
    quality_metrics.append({
        "file_name": file_name,
        "original_rows": original_rows,
        "final_rows": final_rows,
        "duplicates_removed": duplicates,
        "nulls_handled": nulls_handled,
        "invalid_records_removed": invalid_records,
        "data_quality_score": round(100*(1 - total_issues/max(original_rows,1)),2)
    })

def generate_report():
    if not quality_metrics: return
    df = pd.DataFrame(quality_metrics)
    logger.info("\n=== DATA QUALITY SUMMARY ===")
    logger.info(df.to_string(index=False))
    
    # Save report
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(QUALITY_REPORT_DIR, f"quality_report_{ts}.csv")
    df.to_csv(path, index=False)
    logger.info(f"Saved detailed report: {path}")

def main():
    logger.info("=== START DATA QUALITY CHECKS ===")
    files = glob.glob(os.path.join(EXTRACT_DIR, "*.csv"))
    if not files:
        logger.error("No CSV files found to process!")
        return
    for f in files:
        try:
            clean_csv(f)
        except Exception as e:
            logger.error(f"Error processing {f}: {e}")
    generate_report()
    logger.info("=== DATA QUALITY CHECKS COMPLETED ===")

if __name__ == "__main__":
    main()
