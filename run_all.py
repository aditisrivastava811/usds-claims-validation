from pathlib import Path
from typing import Dict, List
import zipfile

CMS_DIR = Path("data/raw/cms")

# We validate by substring patterns (not exact filenames) because CMS downloads
# sometimes prepend numeric IDs to filenames.
REQUIRED_PATTERNS: Dict[str, str] = {
    "beneficiary_2008": "DE1_0_2008_Beneficiary_Summary_File_Sample_1",
    "beneficiary_2009": "DE1_0_2009_Beneficiary_Summary_File_Sample_1",
    "beneficiary_2010": "DE1_0_2010_Beneficiary_Summary_File_Sample_1",
    "carrier_claims_1": "DE1_0_2008_to_2010_Carrier_Claims_Sample_1.zip",
    "carrier_claims_2": "DE1_0_2008_to_2010_Carrier_Claims_Sample_1B.zip",
}

def find_matching_zip(cms_dir: Path, pattern: str) -> Path | None:
    candidates = sorted(cms_dir.glob("*.zip"))
    for p in candidates:
        if pattern in p.name:
            return p
    return None

def validate_required_files(cms_dir: Path) -> Dict[str, Path]:
    cms_dir.mkdir(parents=True, exist_ok=True)

    found: Dict[str, Path] = {}
    missing: List[str] = []

    for key, pattern in REQUIRED_PATTERNS.items():
        match = find_matching_zip(cms_dir, pattern)
        if match is None:
            missing.append(f"{key}: {pattern}")
        else:
            found[key] = match

    if missing:
        existing = sorted(p.name for p in cms_dir.glob("*.zip"))

        print("ERROR: Missing required CMS input files.")
        print("Expected to find ZIPs in: data/raw/cms/ matching these patterns:\n")
        for m in missing:
            print(f"  - {m}")

        print("\nZIP files currently present in data/raw/cms/:")
        if existing:
            for name in existing:
                print(f"  - {name}")
        else:
            print("  (none found)")

        raise SystemExit(1)

    print("All required CMS input ZIP files are present.")
    return found

def list_zip_contents(zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path) as z:
        members = z.namelist()

    csvs = [m for m in members if m.lower().endswith(".csv")]

    print(f"\nFile: {zip_path.name}")
    print(f"  CSV files inside ZIP: {len(csvs)}")
    if csvs:
        for name in csvs[:5]:
            print(f"    - {name}")
        if len(csvs) > 5:
            print("    - ...")

def main() -> None:
    found = validate_required_files(CMS_DIR)

    # Only operate on the five required files (ignore extra ZIPs).
    for key in ["beneficiary_2008", "beneficiary_2009", "beneficiary_2010", "carrier_claims_1", "carrier_claims_2"]:
        list_zip_contents(found[key])

    print("\nPreflight validation complete.")

if __name__ == "__main__":
    main()

