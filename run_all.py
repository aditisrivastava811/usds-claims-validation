from __future__ import annotations

from pathlib import Path
from typing import Dict, List
import zipfile

import pandas as pd


# -----------------------------
# Paths
# -----------------------------
CMS_DIR = Path("data/raw/cms")

# You may have either:
#   - data/raw/new/*.csv  (after unzipping), OR
#   - data/raw/new/New Claims System Outputs/*.csv
NEW_ROOT = Path("data/raw/new")
NEW_SYSTEM_DIR = (
    NEW_ROOT / "New Claims System Outputs"
    if (NEW_ROOT / "New Claims System Outputs").exists()
    else NEW_ROOT
)

OUTPUTS_DIR = Path("outputs")


# -----------------------------
# Required inputs (by patterns)
# -----------------------------
# CMS downloads sometimes prepend numeric IDs to filenames (e.g., "176541_...zip"),
# so we match by substring patterns instead of exact filenames.
REQUIRED_CMS_PATTERNS: Dict[str, List[str]] = {
    "beneficiary_2008": ["Beneficiary_Summary_File", "2008"],
    "beneficiary_2009": ["Beneficiary_Summary_File", "2009"],
    "beneficiary_2010": ["Beneficiary_Summary_File", "2010"],
    # CMS Carrier Claims are split across Sample_1A and Sample_1B
    "carrier_claims_1": ["Carrier_Claims", "Sample_1A"],
    "carrier_claims_2": ["Carrier_Claims", "Sample_1B"],
}


def _name_matches_all_patterns(filename: str, patterns: List[str]) -> bool:
    lower = filename.lower()
    return all(p.lower() in lower for p in patterns)


def find_required_cms_files(cms_dir: Path) -> Dict[str, Path]:
    cms_dir.mkdir(parents=True, exist_ok=True)

    zip_files = sorted(cms_dir.glob("*.zip"))
    found: Dict[str, Path] = {}
    missing: List[str] = []

    for label, patterns in REQUIRED_CMS_PATTERNS.items():
        matches = [z for z in zip_files if _name_matches_all_patterns(z.name, patterns)]
        if not matches:
            missing.append(f"{label}: {patterns}")
        else:
            # Deterministic pick if multiple match
            found[label] = matches[0]

    if missing:
        print("ERROR: Missing required CMS input files.")
        print(f"Expected to find ZIP files in: {cms_dir.as_posix()}/ matching these patterns:\n")
        for m in missing:
            print(f"  - {m}")

        print(f"\nZIP files currently present in {cms_dir.as_posix()}/:")
        if zip_files:
            for z in zip_files:
                print(f"  - {z.name}")
        else:
            print("  (none found)")

        raise SystemExit(1)

    print("All required CMS input files found:")
    for label, path in found.items():
        print(f"  - {label}: {path.name}")

    return found


def validate_new_system_outputs(new_dir: Path) -> Dict[str, Path]:
    """
    Validate the 5 required new-system output CSVs exist and return them as a dict.
    """
    if not new_dir.exists():
        raise SystemExit(f"ERROR: New system output directory not found: {new_dir}")

    required_patterns = {
        "beneficiary_2008": "2008_Beneficiary",
        "beneficiary_2009": "2009_Beneficiary",
        "beneficiary_2010": "2010_Beneficiary",
        "carrier_claims_1": "Carrier_Claims_Sample_1A",
        "carrier_claims_2": "Carrier_Claims_Sample_1B",
    }

    files = list(new_dir.glob("*.csv"))
    found: Dict[str, Path] = {}

    for label, pattern in required_patterns.items():
        matches = [f for f in files if pattern in f.name]
        if not matches:
            raise SystemExit(f"ERROR: Missing new system output for {label} (pattern: {pattern}) in {new_dir}")
        found[label] = matches[0]

    print("\nNew system output files found:")
    for k, v in found.items():
        print(f" - {k}: {v.name}")

    return found


def list_zip_contents(zip_path: Path, max_preview: int = 8) -> None:
    with zipfile.ZipFile(zip_path) as z:
        members = z.namelist()

    csvs = [m for m in members if m.lower().endswith(".csv")]
    print(f"\nCMS ZIP: {zip_path.name}")
    print(f"  CSV files inside ZIP: {len(csvs)}")
    for name in csvs[:max_preview]:
        print(f"    - {name}")
    if len(csvs) > max_preview:
        print("    - ...")


def load_single_csv_from_zip(zip_path: Path) -> pd.DataFrame:
    with zipfile.ZipFile(zip_path) as z:
        csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV found in ZIP: {zip_path.name}")

        # For CMS DE-SynPUF zips, there is typically one CSV
        csv_name = csv_names[0]
        with z.open(csv_name) as f:
            return pd.read_csv(f, dtype=str)


def normalize_value(x: object) -> str:
    """
    Normalize values for comparison.

    Design choice: treat NULL/NaN and empty/whitespace-only strings as equivalent.
    """
    if x is None:
        return ""
    if pd.isna(x):
        return ""
    return str(x).strip()


def compare_dataframes_on_id(
    df_old: pd.DataFrame,
    df_new: pd.DataFrame,
    id_col: str,
) -> pd.DataFrame:
    """
    Compare two DataFrames row-by-row on shared IDs.

    Returns per-column mismatch summary:
      - column
      - mismatched_rows
      - mismatch_rate (over shared IDs)
    """
    old_ids = set(df_old[id_col].dropna())
    new_ids = set(df_new[id_col].dropna())
    shared_ids = sorted(list(old_ids & new_ids))

    old = df_old[df_old[id_col].isin(shared_ids)].copy()
    new = df_new[df_new[id_col].isin(shared_ids)].copy()

    # Align by ID
    old = old.sort_values(id_col).set_index(id_col)
    new = new.sort_values(id_col).set_index(id_col)

    common_cols = [c for c in old.columns if c in new.columns]
    shared_n = len(shared_ids)

    results = []
    for col in common_cols:
        old_norm = old[col].map(normalize_value)
        new_norm = new[col].map(normalize_value)

        mismatches = (old_norm != new_norm)
        mismatch_count = int(mismatches.sum())
        mismatch_rate = (mismatch_count / shared_n) if shared_n else 0.0

        results.append(
            {"column": col, "mismatched_rows": mismatch_count, "mismatch_rate": mismatch_rate}
        )

    return (
        pd.DataFrame(results)
        .sort_values(["mismatched_rows", "column"], ascending=[False, True])
        .reset_index(drop=True)
    )


def main() -> None:
    # ============================================================
    # 1) Validate inputs
    # ============================================================
    cms_files = find_required_cms_files(CMS_DIR)
    new_outputs = validate_new_system_outputs(NEW_SYSTEM_DIR)

    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

    # ============================================================
    # 2) Inspect CMS ZIP contents
    # ============================================================
    print("\nInspecting CMS input ZIP contents:")
    for label in [
        "beneficiary_2008",
        "beneficiary_2009",
        "beneficiary_2010",
        "carrier_claims_1",
        "carrier_claims_2",
    ]:
        list_zip_contents(cms_files[label])

    # ============================================================
    # 3) Multi-year beneficiary comparison
    # ============================================================
    for year in ["2008", "2009", "2010"]:
        key = f"beneficiary_{year}"
        print(f"\n=== Beneficiary {year}: CMS vs New System ===")

        df_cms = load_single_csv_from_zip(cms_files[key])
        df_new = pd.read_csv(new_outputs[key], dtype=str)

        print(f"CMS rows: {len(df_cms)} | NEW rows: {len(df_new)}")

        # ID-level comparison
        cms_ids = set(df_cms["DESYNPUF_ID"].dropna())
        new_ids = set(df_new["DESYNPUF_ID"].dropna())

        missing_in_new = sorted(cms_ids - new_ids)
        missing_in_cms = sorted(new_ids - cms_ids)

        print(f"Missing in NEW: {len(missing_in_new)}")
        print(f"Missing in CMS: {len(missing_in_cms)}")

        id_report_path = OUTPUTS_DIR / f"{key}_id_discrepancies.csv"
        pd.DataFrame(
            {
                "missing_in_new_sample": pd.Series(missing_in_new[:200]),
                "missing_in_cms_sample": pd.Series(missing_in_cms[:200]),
            }
        ).to_csv(id_report_path, index=False)
        print(f"Wrote ID discrepancy report: {id_report_path}")

        # Column-level mismatch analysis (NULL/empty treated as equal)
        mismatch_summary = compare_dataframes_on_id(
            df_old=df_cms,
            df_new=df_new,
            id_col="DESYNPUF_ID",
        )

        mismatch_path = OUTPUTS_DIR / f"{key}_column_mismatches.csv"
        mismatch_summary.to_csv(mismatch_path, index=False)

        print("Top 5 mismatched columns:")
        print(mismatch_summary.head(5).to_string(index=False))
        print(f"Wrote column mismatch report: {mismatch_path}")

    # ============================================================
    # 4) Carrier claims (deferred)
    # ============================================================
    print("\nCarrier claims comparison deferred due to size.")
    print("Approach: chunked processing + aggregation (planned).")

    print("\nPipeline run complete.")


if __name__ == "__main__":
    main()

