# usds-claims-validation

This repository contains a small, reproducible pipeline to validate Medicare DE-SynPUF “old system” CMS files against a “new system” output.

## Required input files (CMS DE-SynPUF)

Per the exercise instructions, the only required CMS files are:

### Beneficiary Summary (3 files)
- DE1.0 Sample 1 2008 Beneficiary Summary File (ZIP)
- DE1.0 Sample 1 2009 Beneficiary Summary File (ZIP)
- DE1.0 Sample 1 2010 Beneficiary Summary File (ZIP)

### Carrier Claims (2 files)
- DE1.0 Sample 1 2008–2010 Carrier Claims 1 (ZIP)
- DE1.0 Sample 1 2008–2010 Carrier Claims 2 (ZIP)

Place the downloaded ZIP files in:


Note: This repo does not commit raw input data. The `data/` directory is intentionally gitignored.

## Folder structure

- `data/raw/cms/` — CMS DE-SynPUF ZIP files (required inputs)
- `data/raw/new/` — “new system” files (if provided for comparison)
- `src/` — source code (future)
- `outputs/` — generated outputs (gitignored)

## Quickstart

1) Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run_all.py
# usds-claims-validation
