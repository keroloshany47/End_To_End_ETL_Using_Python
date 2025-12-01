import os
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

STAGING_2 = "staging_2"
INFO_MART = "Information_Mart"
os.makedirs(INFO_MART, exist_ok=True)

# -------------------------------
# Helpers
# -------------------------------
def load_csv(file, parse_dates=None):
    path = f"{STAGING_2}/{file}"
    return pd.read_csv(path, parse_dates=parse_dates) if parse_dates else pd.read_csv(path)

def safe_extract_id(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.extract(r'(\d+)').astype(int)
    return df

def build_dim_table(df, rename_dict=None):
    df_copy = df.copy()
    if rename_dict:
        df_copy.rename(columns=rename_dict, inplace=True)
    return df_copy

def build_dim_date(orders):
    dates = pd.melt(
        orders[["order_date", "required_date", "shipped_date"]].reset_index(),
        id_vars=["index"],
        value_vars=["order_date", "required_date", "shipped_date"],
        var_name="date_type",
        value_name="date"
    )[["date"]].drop_duplicates().reset_index(drop=True)
    
    dim_date = dates.copy()
    dim_date["date_id"] = dim_date.index + 1
    dim_date["year"] = dim_date["date"].dt.year
    dim_date["month"] = dim_date["date"].dt.month
    dim_date["quarter"] = dim_date["date"].dt.quarter
    dim_date["day"] = dim_date["date"].dt.day
    dim_date["day_of_week"] = dim_date["date"].dt.day_name()
    dim_date["month_name"] = dim_date["date"].dt.month_name()
    return dim_date

def build_fact_sales(order_items, orders, products, dim_date):
    fact = order_items.merge(
        orders[["order_id", "customer_id", "store_id", "staff_id", "order_date", "shipped_date", "order_status"]],
        on="order_id", how="left"
    ).merge(
        products[["product_id", "local_price", "brand_id", "category_id"]],
        on="product_id", how="left"
    )
    fact["total_price"] = fact["quantity"] * fact["local_price"]
    
    for col in ["order_date", "shipped_date"]:
        fact = fact.merge(dim_date[["date", "date_id"]].rename(columns={"date": col, "date_id": f"{col}_id"}), on=col, how="left")
    
    total_revenue = fact["total_price"].sum()
    avg_order_value = fact.groupby("order_id")["total_price"].sum().mean()
    
    logger.info(f"fact_sales: {len(fact):,} records | Total Revenue: {total_revenue:,.2f} EGP | Average Order Value: {avg_order_value:,.2f} EGP")
    return fact

def save_tables(tables):
    for name, df in tables.items():
        path = f"{INFO_MART}/{name}.csv"
        df.to_csv(path, index=False)
        logger.info(f"Saved: {name}.csv")


# -------------------------------
# Main
# -------------------------------
def main():
    logger.info("START DATA MODELING (STAR SCHEMA)")

    # Load source data
    orders = load_csv("orders.csv", parse_dates=["order_date", "required_date", "shipped_date"])
    order_items = load_csv("order_items.csv")
    products = load_csv("products.csv")
    customers = load_csv("customers.csv")
    stores = load_csv("stores.csv")
    staffs = load_csv("staffs.csv")
    
    # Clean IDs
    id_cols = ["product_id", "order_id", "customer_id", "staff_id", "store_id", "item_id"]
    for df in [orders, order_items, products, customers, stores, staffs]:
        safe_extract_id(df, id_cols)
    
    # Build dimension tables
    dim_customer = build_dim_table(customers, {"customer_id": "cust_id", "first_name": "cust_first_name", "last_name": "cust_last_name"})
    dim_product = build_dim_table(products, {"product_id": "prod_id", "product_name": "prod_name"})
    dim_store = build_dim_table(stores)
    dim_staff = build_dim_table(staffs, {"first_name": "staff_first_name", "last_name": "staff_last_name"})
    dim_date = build_dim_date(orders)
    
    # Build fact table
    fact_sales = build_fact_sales(order_items, orders, products, dim_date)
    
    # Save all tables
    save_tables({
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_store": dim_store,
        "dim_staff": dim_staff,
        "dim_date": dim_date,
        "fact_sales": fact_sales
    })
    
    logger.info("DATA MODELING COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    main()
