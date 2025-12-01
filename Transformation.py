import os
import pandas as pd
import json, ast
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STAGING_1, STAGING_2 = "staging_1", "staging_2"
os.makedirs(STAGING_2, exist_ok=True)

def safe_rate(df):
    try:
        r = df.loc[0,"rates"]
        try: return json.loads(r).get("EGP",1)
        except: return ast.literal_eval(r).get("EGP",1)
    except: 
        logger.warning("Using default EGP rate 1.0")
        return 1.0

def transform_products(df, rate):
    df["local_price"] = df["list_price"]*rate
    df["price_category"] = pd.cut(df["local_price"], bins=[0,5000,15000,30000,float('inf')],
                                  labels=["Budget","Mid-Range","Premium","Luxury"])
    return df

def transform_orders(df):
    for col in ["order_date","required_date","shipped_date"]:
        df[col] = pd.to_datetime(df[col])
    df["delivery_latency_days"] = (df["shipped_date"]-df["order_date"]).dt.days
    df["late_delivery"] = ((df["shipped_date"]-df["required_date"]).dt.days>0).astype(int)
    status = {1:"Pending",2:"Processing",3:"Shipped",4:"Delivered",5:"Cancelled"}
    df["order_status_desc"] = df["order_status"].map(status)
    df["order_year"]=df["order_date"].dt.year
    df["order_month"]=df["order_date"].dt.month
    df["order_quarter"]=df["order_date"].dt.quarter
    df["order_day_of_week"]=df["order_date"].dt.day_name()
    return df

def transform_customers(df, stores_df):
    df["local_customer"] = df["city"].isin(stores_df["city"]).astype(int)
    return df

def copy_remaining():
    transformed = {"products.csv","orders.csv","customers.csv"}
    for f in os.listdir(STAGING_1):
        if f.endswith(".csv") and f not in transformed:
            pd.read_csv(f"{STAGING_1}/{f}").to_csv(f"{STAGING_2}/{f}", index=False)

def main():
    products = pd.read_csv(f"{STAGING_1}/products.csv")
    orders = pd.read_csv(f"{STAGING_1}/orders.csv")
    customers = pd.read_csv(f"{STAGING_1}/customers.csv")
    stores = pd.read_csv(f"{STAGING_1}/stores.csv")
    exchange_rates = pd.read_csv(f"{STAGING_1}/exchange_rates.csv")

    rate = safe_rate(exchange_rates)
    products = transform_products(products, rate)
    orders = transform_orders(orders)
    customers = transform_customers(customers, stores)

    products.to_csv(f"{STAGING_2}/products.csv", index=False)
    orders.to_csv(f"{STAGING_2}/orders.csv", index=False)
    customers.to_csv(f"{STAGING_2}/customers.csv", index=False)

    copy_remaining()

    logger.info(f"Transformation completed. Files saved in {STAGING_2}")

if __name__=="__main__":
    main()
