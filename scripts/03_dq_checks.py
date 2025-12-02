import pandas as pd
from pathlib import Path
from config import SALES_COLS, FEATURES_COLS, STORES_COLS, DATE_COL, DATE_FORMAT

RAW = Path("data/raw")
OUT = Path("results")
OUT.mkdir(parents=True, exist_ok=True)

def read_stores():
    df = pd.read_csv(RAW / "stores data-set.csv", usecols=STORES_COLS)
    df["Type"] = df["Type"].astype(str).str.strip().str.upper()
    df.drop_duplicates(subset=["Store"], inplace=True)
    df.to_csv("data/processed/dim_store.csv", index=False)
    return df

def read_sales(chunksize=300_000):
    # Read only required columns
    parts = []
    for chunk in pd.read_csv(RAW / "sales data-set.csv", usecols=lambda c: True, chunksize=chunksize):
        if DATE_COL in chunk.columns:
            chunk[DATE_COL] = pd.to_datetime(chunk[DATE_COL], errors="coerce", format=DATE_FORMAT)
        parts.append(chunk[ [c for c in chunk.columns] ])  # keep all for now
        break  # first chunk to infer checks; remove break for full run later
    return pd.concat(parts, ignore_index=True)

def read_features(chunksize=300_000):
    parts = []
    for chunk in pd.read_csv(RAW / "Features data set.csv", usecols=lambda c: True, chunksize=chunksize):
        if DATE_COL in chunk.columns:
            chunk[DATE_COL] = pd.to_datetime(chunk[DATE_COL], errors="coerce", format=DATE_FORMAT)
        parts.append(chunk[ [c for c in chunk.columns] ])
        break
    return pd.concat(parts, ignore_index=True)

def completeness(df, name):
    df.isna().sum().to_frame("missing").to_csv(OUT / f"dq_missing_{name}.csv")

def validity_sales(df):
    out = {}
    if "Weekly_Sales" in df.columns:
        out["weekly_sales_nonpositive_or_null"] = int(((df["Weekly_Sales"] <= 0) | df["Weekly_Sales"].isna()).sum())
    if "Dept" in df.columns and pd.api.types.is_numeric_dtype(df["Dept"]):
        out["dept_nonpositive"] = int((df["Dept"] <= 0).sum())
    if DATE_COL in df.columns:
        out["date_nulls"] = int(df[DATE_COL].isna().sum())
    pd.DataFrame([out]).to_csv(OUT / "dq_validity_sales.csv", index=False)

def uniqueness(df, key_cols, name):
    dup = int(df.duplicated(subset=key_cols, keep=False).sum())
    pd.DataFrame({"duplicate_rows":[dup], "key":[",".join(key_cols)], "rows":[len(df)]}).to_csv(OUT / f"dq_uniqueness_{name}.csv", index=False)

def consistency_fk(sales_df, stores_df, features_df):
    orphan_stores = None
    orphan_dates = None
    if "Store" in sales_df.columns and "Store" in stores_df.columns:
        orphan_stores = int((~sales_df["Store"].isin(stores_df["Store"])).sum())
    if DATE_COL in sales_df.columns and DATE_COL in features_df.columns:
        s_dates = pd.to_datetime(sales_df[DATE_COL], errors="coerce").dt.date.dropna().unique()
        f_dates = pd.to_datetime(features_df[DATE_COL], errors="coerce").dt.date.dropna().unique()
        orphan_dates = int(len(set(s_dates) - set(f_dates)))
    pd.DataFrame([{"orphan_sales_store": orphan_stores, "orphan_sales_date": orphan_dates}]).to_csv(OUT / "dq_consistency_fk.csv", index=False)

if __name__ == "__main__":
    stores = read_stores()
    sales = read_sales()
    features = read_features()

    completeness(stores, "stores")
    completeness(sales, "sales")
    completeness(features, "features")

    validity_sales(sales)

    if set(["Store","Dept",DATE_COL]).issubset(sales.columns):
        uniqueness(sales, ["Store","Dept",DATE_COL], "sales_store_dept_date")
    if "Store" in stores.columns:
        uniqueness(stores, ["Store"], "stores_storeid")
    if set(["Store",DATE_COL]).issubset(features.columns):
        uniqueness(features, ["Store",DATE_COL], "features_store_date")

    consistency_fk(sales, stores, features)
    print("DQ checks saved in results/")
