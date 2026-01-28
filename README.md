USDS Claims Validation – Data Engineering Take-Home
 
1. Overview
This project implements a simple ETL-style data validation pipeline to compare CMS DE-SynPUF baseline outputs with results from a new claims processing system.
The goal is to verify that the new system produces results consistent with the CMS baseline, surface discrepancies, quantify their scope, and generate concrete artifacts that support review and discussion.
The pipeline is intentionally lightweight, designed to run locally, and produces explicit outputs that can be inspected without additional infrastructure.
 
2. How I Interpreted the Assignment
The prompt asks to:
Use the language and database you consider most appropriate.
Build a simple ETL pipeline into a database or lightweight datastore.
I interpreted this as an opportunity to demonstrate:
•	Practical data ingestion and validation
•	Clear comparison logic that can be explained and reasoned about
•	Explicit, reproducible outputs that communicate findings
•	Sensible engineering tradeoffs for dataset size and scope
Python was chosen for readability and strong data tooling.
CSV files were used as a lightweight datastore for validation artifacts, allowing results to be reviewed directly without standing up additional services.
All CMS input files explicitly listed in the assignment are validated and incorporated into the pipeline.

3. Data Sources
   
3.1 CMS DE-SynPUF Inputs (Required)
The pipeline validates and uses the following CMS ZIP files:
Beneficiary Summary Files
•	2008 Beneficiary Summary
•	2009 Beneficiary Summary
•	2010 Beneficiary Summary
Carrier Claims Files
•	2008–2010 Carrier Claims Sample 1A
•	2008–2010 Carrier Claims Sample 1B
CMS downloads sometimes prepend numeric IDs to filenames (for example, 176541_...zip), so files are identified using substring matching rather than exact filenames.

3.2 New Claims System Outputs
New-system outputs are expected as CSV files located at:
data/raw/new/New Claims System Outputs/
These include:
•	Beneficiary summary outputs for 2008, 2009, and 2010
•	Carrier claims outputs for Sample 1A and Sample 1B

4. Pipeline Behavior

4.1 Input Validation
•	Confirms all required CMS ZIP files are present
•	Confirms all expected new-system CSV outputs are present
•	Fails early with clear error messages if any required inputs are missing

4.2 CMS ZIP Inspection
•	Lists the CSV contents of each CMS ZIP to verify expected structure and naming

4.3 Beneficiary Comparisons (2008–2010)
For each year:
•	Load CMS and new-system beneficiary data
•	Compare row counts
•	Compare beneficiary identifiers (DESYNPUF_ID)
•	Quantify ID discrepancies
•	Perform column-level comparisons for shared beneficiaries
During comparison:
•	Only beneficiaries present in both datasets are compared
•	NULL / NaN values and empty or whitespace-only strings are treated as equivalent to avoid false mismatches

4.4 Artifact Generation
All validation results are written as CSV files under outputs/.
These artifacts are the primary outputs of the pipeline.

5. Output Files
5.1 ID Discrepancy Reports
For each beneficiary year (2008–2010), the pipeline produces a CSV containing example beneficiary IDs that are present in one dataset but missing in the other:
outputs/beneficiary_2008_id_discrepancies.csv
outputs/beneficiary_2009_id_discrepancies.csv
outputs/beneficiary_2010_id_discrepancies.csv
The pipeline reports complete counts of missing IDs in console output.
The CSV files contain a bounded set of representative IDs intended to support manual inspection and spot-checking.

5.2 Column Mismatch Reports
For each year, the pipeline also produces a complete column-level mismatch summary:
outputs/beneficiary_2008_column_mismatches.csv
outputs/beneficiary_2009_column_mismatches.csv
outputs/beneficiary_2010_column_mismatches.csv
Each row represents a column present in both datasets and includes:
•	column: column name
•	mismatched_rows: number of rows where values differ
•	mismatch_rate: proportion of mismatches over shared beneficiary IDs
These reports are intended to highlight which fields differ most and whether discrepancies are isolated or systematic.
 
6. Results Summary
Across all three years:
•	CMS and new-system row counts match exactly
•	ID discrepancies are small and symmetric
•	Column-level mismatches affect a very small fraction of records
•	Differences are concentrated in date and payment-related fields, which are commonly sensitive to formatting or processing differences
Overall, the new system aligns closely with the CMS baseline data.
 
7. Carrier Claims Handling
Full carrier claims comparison was intentionally deferred.
Reason:
•	Carrier claims data contain several million rows
•	A row-by-row comparison is not representative of how large claims systems are typically validated
Planned scalable approach:
•	Chunked ingestion
•	Aggregation by beneficiary and service period
•	Metric-level comparisons (claim counts, totals, distributions)
This approach mirrors common production validation strategies for large claims datasets.
 
8. How to Run
python run_all.py
 
9. Notes
•	The pipeline is intentionally explicit and easy to follow
•	Validation logic and outputs are designed to support discussion and review
•	The structure is straightforward to extend to additional years or claim types


