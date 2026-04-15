from __future__ import annotations

from pathlib import Path
import pandas as pd


REPO_ROOT = Path(r"C:\Users\teaching\Downloads\outage-recovery-forecasting")
INPUT_CSV = REPO_ROOT / "data_raw" / "POUS.csv"
OUTPUT_DIR = REPO_ROOT / "data_transients"
OUTPUT_CSV = OUTPUT_DIR / "FL_POUS.csv"


def main() -> None:
    if not INPUT_CSV.exists():
        raise FileNotFoundError(f"Input file not found: {INPUT_CSV}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Read as strings where identifiers matter. Keep the source schema otherwise intact.
    df = pd.read_csv(
        INPUT_CSV,
        dtype={"CountyFIPS": "string", "storm": "string"},
        low_memory=False,
    )

    if "CountyFIPS" not in df.columns:
        raise KeyError(f"CountyFIPS column not found. Available columns: {df.columns.tolist()}")

    # Preserve 5-digit county FIPS codes with leading zeros.
    df["CountyFIPS"] = df["CountyFIPS"].astype("string").str.strip().str.zfill(5)

    # Florida county FIPS codes begin with state code 12.
    fl = df[df["CountyFIPS"].str.startswith("12", na=False)].copy()

    # Keep output stable and easy to inspect.
    sort_cols = [c for c in ["storm", "event_start", "CountyFIPS"] if c in fl.columns]
    if sort_cols:
        fl = fl.sort_values(sort_cols, kind="stable")

    fl.to_csv(OUTPUT_CSV, index=False)

    print(f"Input rows: {len(df):,}")
    print(f"Florida rows: {len(fl):,}")
    print(f"Unique Florida counties: {fl['CountyFIPS'].nunique(dropna=True):,}")
    if "storm" in fl.columns:
        print(f"Unique Florida storms: {fl['storm'].nunique(dropna=True):,}")
    print(f"Wrote: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
