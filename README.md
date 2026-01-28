# USDS Claims Validation

This repository contains a reproducible validation pipeline that compares
CMS DE-SynPUF Medicare claims data (“legacy CMS baseline”) to outputs from a
hypothetical **New Claims System**.

The goal is to:
- Verify input completeness
- Compare beneficiary-level records across systems
- Quantify and surface data discrepancies
- Produce reviewable artifacts for downstream analysis

---

## Project Structure

usds-claims-validation/
│
├── data/
│ └── raw/
│ ├── cms/ # CMS DE-SynPUF input ZIP files (not committed)
│ └── new/ # New system output files (CSV or ZIP)
│
├── outputs/ # Generated discrepancy reports
│
├── run_all.py # Main validation pipeline
├── requirements.txt
└── README.md


##Raw input data is **not** committed to the repository.

---

## Required Input Files (CMS DE-SynPUF)

Per the exercise instructions, **only the following CMS files are required**.

### Beneficiary Summary Files (3 ZIPs)
- DE1.0 Sample 1 2008 Beneficiary Summary File
- DE1.0 Sample 1 2009 Beneficiary Summary File
- DE1.0 Sample 1 2010 Beneficiary Summary File

### Carrier Claims Files (2 ZIPs)
- DE1.0 Sample 1 2008–2010 Carrier Claims (Part 1)
- DE1.0 Sample 1 2008–2010 Carrier Claims (Part 2)

Place these ZIP files in: data/raw/cms/

---

## CMS Carrier Claims File Naming (Important)

CMS DE-SynPUF Carrier Claims files use **inconsistent naming** and often include
numeric prefixes added at download time (e.g. `176541_…zip`).

The pipeline therefore:
- Matches CMS ZIPs using **substring patterns**, not exact filenames
- Treats the two carrier ZIPs as:
  - `carrier_claims_1` → Sample 1
  - `carrier_claims_2` → Sample 1B

This allows robust validation even when filenames vary.

---

## New System Outputs

New system outputs may be provided as:
- CSV files directly, or
- A ZIP archive containing CSVs

Supported outputs:
- Beneficiary Summary Files (2008–2010)
- Carrier Claims (Sample 1A and 1B)

Expected location: data/raw/new/

---

## What the Pipeline Does

### 1. Preflight Validation
- Confirms all required CMS ZIPs are present
- Confirms new system outputs exist
- Prints CSV contents of each CMS ZIP for manual verification

### 2. Baseline Ingestion
- Loads CMS Beneficiary Summary files (2008–2010)
- Loads corresponding New System CSVs
- Reads all data as strings to avoid type-coercion artifacts

### 3. Beneficiary ID Comparison
For each year:
- Compares `DESYNPUF_ID` sets between CMS and New System
- Reports:
  - Missing in New
  - Missing in CMS
- Writes a discrepancy sample CSV

### 4. Column-Level Mismatch Analysis
For beneficiaries present in **both systems**:
- Compares values column-by-column
- **Treats NULL and empty values as equivalent**
- Calculates:
  - Number of mismatched rows per column
  - Mismatch rate per column
- Outputs ranked mismatch reports

### 5. Carrier Claims (Deferred)
Carrier claims ingestion was tested but **full comparison is deferred**
due to data size (~4.7M rows).

Planned approach:
- Chunked CSV ingestion
- Aggregation by beneficiary and service date
- Metric-level comparison instead of row-by-row

---

## Generated Artifacts

The pipeline produces review-ready CSV outputs:
outputs/
├── beneficiary_2008_id_discrepancies.csv
├── beneficiary_2008_column_mismatches.csv
├── beneficiary_2009_id_discrepancies.csv
├── beneficiary_2009_column_mismatches.csv
├── beneficiary_2010_id_discrepancies.csv
└── beneficiary_2010_column_mismatches.csv


These artifacts allow reviewers to inspect **exact differences**
rather than relying on summary statistics alone.

---

## Results Summary & Interpretation

### Beneficiary ID Alignment
Across all three years:
- CMS and New System row counts are identical
- A small number of IDs (~49–58 per year) appear in one system but not the other

This pattern is consistent across years and suggests boundary or filtering
differences rather than systemic data loss.

### Column-Level Differences
- Overall mismatch rates are extremely low (≈0.01% or less)
- Most discrepancies appear in payment or cost-related fields
- Demographic fields largely align

Because NULL and empty values are treated as equivalent, reported mismatches
represent **true value differences**, not formatting artifacts.

### Carrier Claims
- Successfully ingested union of Sample 1 + Sample 1B
- Deferred full comparison due to scale
- Follow-up work is clearly scoped and documented

---

## Key Takeaways

- The New Claims System largely matches CMS baseline outputs
- Discrepancies are:
  - Small in magnitude
  - Consistent across years
  - Isolated to a limited set of fields
- The pipeline emphasizes **explainability over volume**
- Design choices prioritize scalability and auditability

Overall, the results suggest the New System is a strong candidate for
further validation, with remaining work focused on large-scale claims
aggregation rather than structural correctness.

---

## Running the Pipeline

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python run_all.py

##Notes

Raw CMS data is not included in the repository

All processing is deterministic and repeatable

The pipeline is designed to be extended to additional years and claim types







