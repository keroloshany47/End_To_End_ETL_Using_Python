import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import warnings
warnings.filterwarnings('ignore')

# Setup
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")
VIZ_DIR = "Visualizations"
os.makedirs(VIZ_DIR, exist_ok=True)

print("\n" + "="*60)
print("STARTING VISUALIZATION CREATION")
print("="*60 + "\n")

# ============================================
# LOAD DATA
# ============================================
print("Step 1: Loading data files...")
try:
    fact_sales = pd.read_csv("Information_Mart/fact_sales.csv")
    dim_product = pd.read_csv("Information_Mart/dim_product.csv")
    dim_customer = pd.read_csv("Information_Mart/dim_customer.csv")
    dim_date = pd.read_csv("Information_Mart/dim_date.csv", parse_dates=['date'])
    print(f"[OK] Loaded {len(fact_sales):,} sales records")
    print(f"[OK] Loaded {len(dim_product):,} products")
    print(f"[OK] Loaded {len(dim_customer):,} customers")
    print(f"[OK] Loaded {len(dim_date):,} dates\n")
except Exception as e:
    print(f"[ERROR] Could not load data: {e}")
    print("Please make sure you ran Modeling.py first!")
    exit(1)

# ============================================
# PREPARE DATA - Smart Merge
# ============================================
print("Step 2: Preparing data for analysis...")

# Start with fact table - select only needed columns
sales = fact_sales[['product_id', 'customer_id', 'order_date_id', 'quantity', 'total_price']].copy()
initial_count = len(sales)

# Merge with products - only needed columns
sales = sales.merge(
    dim_product[['prod_id', 'prod_name']], 
    left_on='product_id', 
    right_on='prod_id', 
    how='left'
).drop('prod_id', axis=1)

# Merge with customers - only needed columns
sales = sales.merge(
    dim_customer[['cust_id', 'cust_first_name', 'cust_last_name', 'city', 'state']], 
    left_on='customer_id', 
    right_on='cust_id', 
    how='left'
).drop('cust_id', axis=1)

# Merge with dates - only needed columns
sales = sales.merge(
    dim_date[['date_id', 'date', 'year', 'month', 'day_of_week']], 
    left_on='order_date_id', 
    right_on='date_id', 
    how='left'
).drop('date_id', axis=1)

# Create customer full name
sales['customer_name'] = sales['cust_first_name'] + ' ' + sales['cust_last_name']

print(f"[OK] Data prepared: {len(sales):,} records ready for visualization\n")

# ============================================
# CHART 1: TIME-SERIES ANALYSIS
# ============================================
print("Step 3: Creating Chart 1 - Time-Series Analysis...")

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Time-Series Analysis: Sales Over Time', fontsize=18, fontweight='bold', y=0.995)

# 1.1 Daily Sales Trend
daily_sales = sales.groupby('date')['total_price'].sum().reset_index()
daily_sales = daily_sales.sort_values('date')
axes[0, 0].plot(daily_sales['date'], daily_sales['total_price'], 
                linewidth=2, color='#2E86AB', marker='o', markersize=4)
axes[0, 0].fill_between(daily_sales['date'], daily_sales['total_price'], alpha=0.3, color='#2E86AB')
axes[0, 0].set_title('Daily Sales Revenue', fontsize=13, fontweight='bold', pad=10)
axes[0, 0].set_xlabel('Date', fontsize=11)
axes[0, 0].set_ylabel('Revenue (EGP)', fontsize=11)
axes[0, 0].tick_params(axis='x', rotation=45)
axes[0, 0].grid(True, alpha=0.3)
axes[0, 0].ticklabel_format(style='plain', axis='y')

# 1.2 Monthly Sales
monthly_sales = sales.groupby(['year', 'month'])['total_price'].sum().reset_index()
monthly_sales['period'] = monthly_sales['year'].astype(str) + '-' + monthly_sales['month'].astype(str).str.zfill(2)
bars = axes[0, 1].bar(range(len(monthly_sales)), monthly_sales['total_price'], 
                       color='#27AE60', alpha=0.8, edgecolor='black', linewidth=0.5)
axes[0, 1].set_title('Monthly Sales Revenue', fontsize=13, fontweight='bold', pad=10)
axes[0, 1].set_xlabel('Month', fontsize=11)
axes[0, 1].set_ylabel('Revenue (EGP)', fontsize=11)
axes[0, 1].set_xticks(range(len(monthly_sales)))
axes[0, 1].set_xticklabels(monthly_sales['period'], rotation=45, ha='right')
axes[0, 1].grid(True, alpha=0.3, axis='y')
axes[0, 1].ticklabel_format(style='plain', axis='y')

# 1.3 Sales by Day of Week
dow_sales = sales.groupby('day_of_week')['total_price'].sum().reset_index()
day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
dow_sales['day_of_week'] = pd.Categorical(dow_sales['day_of_week'], categories=day_order, ordered=True)
dow_sales = dow_sales.sort_values('day_of_week')
colors = sns.color_palette("coolwarm", len(dow_sales))
axes[1, 0].bar(dow_sales['day_of_week'], dow_sales['total_price'], 
               color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
axes[1, 0].set_title('Sales by Day of Week', fontsize=13, fontweight='bold', pad=10)
axes[1, 0].set_xlabel('Day', fontsize=11)
axes[1, 0].set_ylabel('Revenue (EGP)', fontsize=11)
axes[1, 0].tick_params(axis='x', rotation=45)
axes[1, 0].grid(True, alpha=0.3, axis='y')
axes[1, 0].ticklabel_format(style='plain', axis='y')

# 1.4 Sales Trend with Moving Average
daily_sorted = daily_sales.sort_values('date').copy()
daily_sorted['MA7'] = daily_sorted['total_price'].rolling(window=7, min_periods=1).mean()
axes[1, 1].plot(daily_sorted['date'], daily_sorted['total_price'], 
                label='Daily Sales', alpha=0.4, color='gray', linewidth=1)
axes[1, 1].plot(daily_sorted['date'], daily_sorted['MA7'], 
                label='7-Day Moving Avg', linewidth=3, color='#E74C3C')
axes[1, 1].set_title('Sales Trend with 7-Day Moving Average', fontsize=13, fontweight='bold', pad=10)
axes[1, 1].set_xlabel('Date', fontsize=11)
axes[1, 1].set_ylabel('Revenue (EGP)', fontsize=11)
axes[1, 1].legend(loc='upper left')
axes[1, 1].grid(True, alpha=0.3)
axes[1, 1].tick_params(axis='x', rotation=45)
axes[1, 1].ticklabel_format(style='plain', axis='y')

plt.tight_layout()
plt.savefig(f'{VIZ_DIR}/1_time_series_analysis.png', dpi=300, bbox_inches='tight')
print(f"[OK] Saved: {VIZ_DIR}/1_time_series_analysis.png\n")
plt.close()

# ============================================
# CHART 2: TOP N PERFORMANCE
# ============================================
print("Step 4: Creating Chart 2 - Top N Performance...")

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Top N Performance Analysis', fontsize=18, fontweight='bold', y=0.995)

# 2.1 Top 10 Products by Revenue
top_products_rev = sales.groupby('prod_name')['total_price'].sum().nlargest(10).reset_index()
axes[0, 0].barh(range(len(top_products_rev)), top_products_rev['total_price'], 
                color='#16A085', alpha=0.8, edgecolor='black', linewidth=0.5)
axes[0, 0].set_yticks(range(len(top_products_rev)))
axes[0, 0].set_yticklabels(top_products_rev['prod_name'], fontsize=9)
axes[0, 0].set_title('Top 10 Products by Revenue', fontsize=13, fontweight='bold', pad=10)
axes[0, 0].set_xlabel('Revenue (EGP)', fontsize=11)
axes[0, 0].invert_yaxis()
axes[0, 0].grid(True, alpha=0.3, axis='x')
axes[0, 0].ticklabel_format(style='plain', axis='x')

# 2.2 Top 10 Products by Quantity
top_products_qty = sales.groupby('prod_name')['quantity'].sum().nlargest(10).reset_index()
axes[0, 1].barh(range(len(top_products_qty)), top_products_qty['quantity'], 
                color='#8E44AD', alpha=0.8, edgecolor='black', linewidth=0.5)
axes[0, 1].set_yticks(range(len(top_products_qty)))
axes[0, 1].set_yticklabels(top_products_qty['prod_name'], fontsize=9)
axes[0, 1].set_title('Top 10 Products by Quantity Sold', fontsize=13, fontweight='bold', pad=10)
axes[0, 1].set_xlabel('Quantity', fontsize=11)
axes[0, 1].invert_yaxis()
axes[0, 1].grid(True, alpha=0.3, axis='x')

# 2.3 Top 10 Customers by Spending
top_customers = sales.groupby('customer_name')['total_price'].sum().nlargest(10).reset_index()
axes[1, 0].bar(range(len(top_customers)), top_customers['total_price'], 
               color='#C0392B', alpha=0.8, edgecolor='black', linewidth=0.5)
axes[1, 0].set_xticks(range(len(top_customers)))
axes[1, 0].set_xticklabels(top_customers['customer_name'], rotation=45, ha='right', fontsize=8)
axes[1, 0].set_title('Top 10 Customers by Total Spending', fontsize=13, fontweight='bold', pad=10)
axes[1, 0].set_ylabel('Total Spending (EGP)', fontsize=11)
axes[1, 0].grid(True, alpha=0.3, axis='y')
axes[1, 0].ticklabel_format(style='plain', axis='y')

# 2.4 Revenue vs Quantity Scatter
product_stats = sales.groupby('prod_name').agg({
    'total_price': 'sum',
    'quantity': 'sum'
}).nlargest(30, 'total_price')
scatter = axes[1, 1].scatter(product_stats['quantity'], product_stats['total_price'], 
                             s=150, alpha=0.6, color='#2980B9', edgecolor='black', linewidth=0.5)
axes[1, 1].set_title('Revenue vs Quantity (Top 30 Products)', fontsize=13, fontweight='bold', pad=10)
axes[1, 1].set_xlabel('Total Quantity Sold', fontsize=11)
axes[1, 1].set_ylabel('Total Revenue (EGP)', fontsize=11)
axes[1, 1].grid(True, alpha=0.3)
axes[1, 1].ticklabel_format(style='plain', axis='both')

plt.tight_layout()
plt.savefig(f'{VIZ_DIR}/2_top_n_performance.png', dpi=300, bbox_inches='tight')
print(f"[OK] Saved: {VIZ_DIR}/2_top_n_performance.png\n")
plt.close()

# ============================================
# CHART 3: DISTRIBUTION ANALYSIS
# ============================================
print("Step 5: Creating Chart 3 - Distribution Analysis...")

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Distribution & Customer Segmentation Analysis', fontsize=18, fontweight='bold', y=0.995)

# 3.1 Customer Spending Distribution
customer_spending = sales.groupby('customer_id')['total_price'].sum()
axes[0, 0].hist(customer_spending, bins=30, color='#5DADE2', alpha=0.7, edgecolor='black', linewidth=0.8)
mean_val = customer_spending.mean()
median_val = customer_spending.median()
axes[0, 0].axvline(mean_val, color='red', linestyle='--', linewidth=2.5, 
                   label=f'Mean: {mean_val:,.0f} EGP')
axes[0, 0].axvline(median_val, color='green', linestyle='--', linewidth=2.5,
                   label=f'Median: {median_val:,.0f} EGP')
axes[0, 0].set_title('Customer Spending Distribution', fontsize=13, fontweight='bold', pad=10)
axes[0, 0].set_xlabel('Total Spending (EGP)', fontsize=11)
axes[0, 0].set_ylabel('Number of Customers', fontsize=11)
axes[0, 0].legend()
axes[0, 0].grid(True, alpha=0.3, axis='y')

# 3.2 Order Value Distribution
axes[0, 1].hist(sales['total_price'], bins=50, color='#E67E22', alpha=0.7, edgecolor='black', linewidth=0.8)
axes[0, 1].set_title('Order Value Distribution', fontsize=13, fontweight='bold', pad=10)
axes[0, 1].set_xlabel('Order Value (EGP)', fontsize=11)
axes[0, 1].set_ylabel('Number of Orders', fontsize=11)
axes[0, 1].grid(True, alpha=0.3, axis='y')

# 3.3 Customer Segmentation
segments = pd.cut(customer_spending, 
                  bins=[0, 10000, 50000, 100000, float('inf')],
                  labels=['Low\n(0-10K)', 'Medium\n(10K-50K)', 'High\n(50K-100K)', 'VIP\n(100K+)'])
segment_counts = segments.value_counts()
colors_seg = ['#F4D03F', '#F39C12', '#E74C3C', '#8B0000']
wedges, texts, autotexts = axes[1, 0].pie(segment_counts.values, labels=segment_counts.index, 
                                            autopct='%1.1f%%', startangle=90, colors=colors_seg,
                                            explode=[0.05, 0.05, 0.05, 0.1],
                                            textprops={'fontsize': 11, 'fontweight': 'bold'})
for autotext in autotexts:
    autotext.set_color('white')
axes[1, 0].set_title('Customer Segmentation by Spending', fontsize=13, fontweight='bold', pad=10)

# 3.4 Order Quantity Distribution
qty_dist = sales.groupby('quantity').size().head(15)
axes[1, 1].bar(qty_dist.index.astype(str), qty_dist.values, 
               color='#229954', alpha=0.8, edgecolor='black', linewidth=0.5)
axes[1, 1].set_title('Order Quantity Distribution (Top 15)', fontsize=13, fontweight='bold', pad=10)
axes[1, 1].set_xlabel('Quantity per Order', fontsize=11)
axes[1, 1].set_ylabel('Number of Orders', fontsize=11)
axes[1, 1].grid(True, alpha=0.3, axis='y')

plt.tight_layout()
plt.savefig(f'{VIZ_DIR}/3_distribution_analysis.png', dpi=300, bbox_inches='tight')
print(f"[OK] Saved: {VIZ_DIR}/3_distribution_analysis.png\n")
plt.close()

# ============================================
# CHART 4: GEOGRAPHICAL ANALYSIS
# ============================================
print("Step 6: Creating Chart 4 - Geographical Analysis...")

fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('Geographical Distribution Analysis', fontsize=18, fontweight='bold', y=0.995)

# 4.1 Top 15 Cities by Revenue
city_sales = sales.groupby('city')['total_price'].sum().nlargest(15).reset_index()
colors_cities = sns.color_palette("rocket_r", len(city_sales))
axes[0, 0].barh(range(len(city_sales)), city_sales['total_price'], 
                color=colors_cities, alpha=0.8, edgecolor='black', linewidth=0.5)
axes[0, 0].set_yticks(range(len(city_sales)))
axes[0, 0].set_yticklabels(city_sales['city'], fontsize=9)
axes[0, 0].set_title('Top 15 Cities by Revenue', fontsize=13, fontweight='bold', pad=10)
axes[0, 0].set_xlabel('Revenue (EGP)', fontsize=11)
axes[0, 0].invert_yaxis()
axes[0, 0].grid(True, alpha=0.3, axis='x')
axes[0, 0].ticklabel_format(style='plain', axis='x')

# 4.2 Top 10 States by Revenue
state_sales = sales.groupby('state')['total_price'].sum().nlargest(10).reset_index()
colors_states = sns.color_palette("viridis", len(state_sales))
axes[0, 1].bar(range(len(state_sales)), state_sales['total_price'],
               color=colors_states, alpha=0.8, edgecolor='black', linewidth=0.5)
axes[0, 1].set_xticks(range(len(state_sales)))
axes[0, 1].set_xticklabels(state_sales['state'], rotation=45, ha='right', fontsize=9)
axes[0, 1].set_title('Top 10 States by Revenue', fontsize=13, fontweight='bold', pad=10)
axes[0, 1].set_ylabel('Revenue (EGP)', fontsize=11)
axes[0, 1].grid(True, alpha=0.3, axis='y')
axes[0, 1].ticklabel_format(style='plain', axis='y')

# 4.3 Top 15 Cities by Customer Count
city_customers = sales.groupby('city')['customer_id'].nunique().nlargest(15).reset_index()
axes[1, 0].barh(range(len(city_customers)), city_customers['customer_id'],
                color='#17A589', alpha=0.8, edgecolor='black', linewidth=0.5)
axes[1, 0].set_yticks(range(len(city_customers)))
axes[1, 0].set_yticklabels(city_customers['city'], fontsize=9)
axes[1, 0].set_title('Top 15 Cities by Customer Count', fontsize=13, fontweight='bold', pad=10)
axes[1, 0].set_xlabel('Number of Unique Customers', fontsize=11)
axes[1, 0].invert_yaxis()
axes[1, 0].grid(True, alpha=0.3, axis='x')

# 4.4 Revenue Distribution by Top 8 States (Pie)
top_states = sales.groupby('state')['total_price'].sum().nlargest(8)
colors_pie = sns.color_palette("Set3", len(top_states))
wedges, texts, autotexts = axes[1, 1].pie(top_states.values, labels=top_states.index, 
                                            autopct='%1.1f%%', startangle=90, colors=colors_pie,
                                            textprops={'fontsize': 10})
for autotext in autotexts:
    autotext.set_color('black')
    autotext.set_fontweight('bold')
axes[1, 1].set_title('Revenue Distribution (Top 8 States)', fontsize=13, fontweight='bold', pad=10)

plt.tight_layout()
plt.savefig(f'{VIZ_DIR}/4_geographical_analysis.png', dpi=300, bbox_inches='tight')
print(f"[OK] Saved: {VIZ_DIR}/4_geographical_analysis.png\n")
plt.close()

# ============================================
# FINAL SUMMARY
# ============================================
print("="*60)
print("VISUALIZATION COMPLETED SUCCESSFULLY!")
print("="*60)
print(f"\nCreated 4 high-quality visualizations in '{VIZ_DIR}/' folder:")
print("  1. 1_time_series_analysis.png")
print("  2. 2_top_n_performance.png")
print("  3. 3_distribution_analysis.png")
print("  4. 4_geographical_analysis.png")
print("\n" + "-"*60)
print("KEY BUSINESS METRICS:")
print("-"*60)
print(f"Total Revenue:        {sales['total_price'].sum():>20,.2f} EGP")
print(f"Total Orders:         {len(sales):>20,}")
print(f"Unique Customers:     {sales['customer_id'].nunique():>20,}")
print(f"Unique Products:      {sales['product_id'].nunique():>20,}")
print(f"Average Order Value:  {sales['total_price'].mean():>20,.2f} EGP")
print(f"Median Order Value:   {sales['total_price'].median():>20,.2f} EGP")
print(f"Date Range:           {sales['date'].min().strftime('%Y-%m-%d')} to {sales['date'].max().strftime('%Y-%m-%d')}")
print(f"Total Quantity Sold:  {sales['quantity'].sum():>20,} units")
print("="*60 + "\n")