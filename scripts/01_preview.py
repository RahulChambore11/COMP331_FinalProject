import pandas as pd
from pathlib import Path

RAW = Path("data/raw")
OUT = Path("results")
OUT.mkdir(parents=True, exist_ok=True)

files = [
    "sales data-set.csv",
    "stores data-set.csv",
    "Features data set.csv",
]

for fname in files:
    fpath = RAW / fname
    print(f"\n=== Preview: {fname} ===")
    # Read just the first few rows
    df = pd.read_csv(fpath, nrows=5)
    # Save preview to results
    df.to_csv(OUT / f"preview_{Path(fname).stem}.csv", index=False)
    # Print basic info
    print("Columns:", list(df.columns))
    print(df.head(3))
print("\nPreviews saved in results/preview_*.csv")
