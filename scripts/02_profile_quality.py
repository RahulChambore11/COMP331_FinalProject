import pandas as pd
from pathlib import Path

RAW = Path("data/raw")
OUT = Path("results")
OUT.mkdir(parents=True, exist_ok=True)

def profile_small(path_stem, usecols=None):
    df = pd.read_csv(RAW / path_stem, usecols=usecols)
    # dtypes
    dtypes = pd.DataFrame(df.dtypes.astype(str), columns=["dtype"])
    dtypes.to_csv(OUT / f"dtypes_{Path(path_stem).stem}.csv")
    # missing
    missing = df.isna().sum().sort_values(ascending=False)
    missing.to_frame("missing").to_csv(OUT / f"missing_{Path(path_stem).stem}.csv")
    # duplicates (full row)
    dup_count = int(df.duplicated().sum())
    pd.DataFrame({"duplicate_rows":[dup_count], "rows":[len(df)]}).to_csv(OUT / f"dups_{Path(path_stem).stem}.csv", index=False)

def missing_chunked(path_stem, chunksize=250_000):
    total = 0
    miss = None
    for chunk in pd.read_csv(RAW / path_stem, chunksize=chunksize):
        total += len(chunk)
        cur = chunk.isna().sum()
        miss = cur if miss is None else miss.add(cur, fill_value=0)
    pd.DataFrame({"missing": miss.astype(int)}).to_csv(OUT / f"missing_{Path(path_stem).stem}.csv")
    pd.DataFrame({"rows":[total]}).to_csv(OUT / f"rows_{Path(path_stem).stem}.csv", index=False)

if __name__ == "__main__":
    # Stores is small
    profile_small("stores data-set.csv")

    # Sales and Features may be large; use chunked missing counts
    missing_chunked("sales data-set.csv")
    missing_chunked("Features data set.csv")
    print("Profiles saved in results/")
