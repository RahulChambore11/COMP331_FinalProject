import pandas as pd
from pathlib import Path

# Define paths
RAW = Path("data/raw")
PROC = Path("data/processed")
RES = Path("results")
PROC.mkdir(parents=True, exist_ok=True)
RES.mkdir(parents=True, exist_ok=True)

import seaborn as sns
import matplotlib.pyplot as plt

sns.set(style="whitegrid", palette="muted", font_scale=1.2)

# ---------------------------
# DimStore
# ---------------------------
dim_store = pd.read_csv(RAW / "stores data-set.csv")
dim_store["Type"] = dim_store["Type"].astype(str).str.strip().str.upper()
dim_store = dim_store.drop_duplicates(subset=["Store"]).copy()
dim_store["StoreKey"] = range(1, len(dim_store) + 1)
dim_store.to_csv(PROC / "dim_store.csv", index=False)

# ---------------------------
# DimDate (from Sales dates)
# ---------------------------
date_set = set()
for chunk in pd.read_csv(RAW / "sales data-set.csv", usecols=lambda c: c == "Date", chunksize=300_000):
    dates = pd.to_datetime(chunk["Date"], errors="coerce", dayfirst=True).dt.date.dropna().unique()
    date_set.update(dates)

dim_date = pd.DataFrame({"Date": sorted(date_set)})
dim_date["Date"] = pd.to_datetime(dim_date["Date"], errors="coerce")
dim_date["DateKey"] = dim_date["Date"].dt.strftime("%Y%m%d").astype(int)
dim_date["Year"] = dim_date["Date"].dt.year
dim_date["Month"] = dim_date["Date"].dt.month
dim_date["Day"] = dim_date["Date"].dt.day
dim_date["Week"] = dim_date["Date"].dt.isocalendar().week.astype(int)
dim_date["Quarter"] = dim_date["Date"].dt.quarter
dim_date.to_csv(PROC / "dim_date.csv", index=False)

# ---------------------------
# DimFeatures
# ---------------------------
features = pd.read_csv(RAW / "Features data set.csv")
# Convert Date column explicitly
features["Date"] = pd.to_datetime(features["Date"], errors="coerce", dayfirst=True)
features = features.dropna(subset=["Date"])
features["DateKey"] = features["Date"].dt.strftime("%Y%m%d").astype(int)
features = features.drop_duplicates(subset=["Store", "DateKey"])
features.to_csv(PROC / "dim_features.csv", index=False)

# ---------------------------
# FactSales
# ---------------------------
store_map = dim_store.set_index("Store")["StoreKey"].to_dict()
date_map = dim_date.set_index("Date")["DateKey"].to_dict()

fact_chunks = []
for chunk in pd.read_csv(RAW / "sales data-set.csv", chunksize=100_000):
    # Force Date column to datetime
    chunk["Date"] = pd.to_datetime(chunk["Date"], errors="coerce", dayfirst=True)

fact_chunks = []
for chunk in pd.read_csv(RAW / "sales data-set.csv", chunksize=100_000):
    # Force Date column to datetime
    chunk["Date"] = pd.to_datetime(chunk["Date"], errors="coerce", dayfirst=True)

    # Debugging tip: check dtype
    print("Date dtype:", chunk["Date"].dtype)

    # Drop invalid sales
    if "Weekly_Sales" in chunk.columns:
        chunk = chunk[chunk["Weekly_Sales"].notna() & (chunk["Weekly_Sales"] > 0)]
    chunk = chunk.dropna(subset=["Store", "Date"])

    # Keys
    chunk["StoreKey"] = chunk["Store"].map(store_map).fillna(0).astype(int)
    chunk["DateKey"] = chunk["Date"].dt.strftime("%Y%m%d").astype(int)

    # Select columns
    keep = [c for c in ["StoreKey", "DateKey", "Dept", "Weekly_Sales", "IsHoliday"] if c in chunk.columns]
    fact_chunks.append(chunk[keep])

    # Drop invalid sales
    if "Weekly_Sales" in chunk.columns:
        chunk = chunk[chunk["Weekly_Sales"].notna() & (chunk["Weekly_Sales"] > 0)]
    chunk = chunk.dropna(subset=["Store", "Date"])

    # Keys
    chunk["StoreKey"] = chunk["Store"].map(store_map).fillna(0).astype(int)
    chunk["DateKey"] = chunk["Date"].dt.strftime("%Y%m%d").astype(int)

    # Select columns
    keep = [c for c in ["StoreKey", "DateKey", "Dept", "Weekly_Sales", "IsHoliday"] if c in chunk.columns]
    fact_chunks.append(chunk[keep])

fact_sales = pd.concat(fact_chunks, ignore_index=True)
fact_sales.to_csv(PROC / "fact_sales.csv", index=False)
print("FactSales saved:", len(fact_sales), "rows")


fact_sales = pd.concat(fact_chunks, ignore_index=True)
fact_sales.to_csv(PROC / "fact_sales.csv", index=False)
print("FactSales saved:", len(fact_sales), "rows")


# ---------------------------
# Integrity summary
# ---------------------------
orphan_store_rows = int((fact_sales["StoreKey"] == 0).sum()) if "StoreKey" in fact_sales.columns else None
orphan_date_rows = int((~fact_sales["DateKey"].isin(dim_date["DateKey"])).sum()) if "DateKey" in fact_sales.columns else None

pd.DataFrame([{
    "fact_rows": len(fact_sales),
    "orphan_store_rows": orphan_store_rows,
    "orphan_date_rows": orphan_date_rows
}]).to_csv(RES / "fk_integrity_fact.csv", index=False)

print("ETL complete. Processed tables saved in data/processed; integrity summary in results/fk_integrity_fact.csv.")





import matplotlib.pyplot as plt

# Load dim_store
dim_store = pd.read_csv(PROC / "dim_store.csv")

# Plot distribution of store types
dim_store["Type"].value_counts().plot(kind="bar", color="skyblue")
plt.title("Store Type Distribution")
plt.xlabel("Store Type")
plt.ylabel("Count")
plt.tight_layout()
plt.savefig(RES / "chart_store_type_distribution.png")
plt.close()

# Load fact_sales
fact_sales = pd.read_csv(PROC / "fact_sales.csv")

# Plot histogram of weekly sales
fact_sales["Weekly_Sales"].hist(bins=50, color="green")
plt.title("Weekly Sales Distribution")
plt.xlabel("Weekly Sales")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig(RES / "chart_weekly_sales_hist.png")
plt.close()

dim_store = pd.read_csv(PROC / "dim_store.csv")

sns.countplot(x="Type", data=dim_store)
plt.title("Store Type Distribution")
plt.tight_layout()
plt.savefig(RES / "chart_store_type_distribution.png")
plt.close()


fact_sales = pd.read_csv(PROC / "fact_sales.csv")

sns.histplot(fact_sales["Weekly_Sales"], bins=50, color="green", kde=True)
plt.title("Weekly Sales Distribution")
plt.xlabel("Weekly Sales")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig(RES / "chart_weekly_sales_hist.png")
plt.close()

