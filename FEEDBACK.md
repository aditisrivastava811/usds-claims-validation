Feedback

Assignment Experience

Overall, I enjoyed this exercise. It closely resembles real-world data engineering
validation work, especially around establishing trust in a new system before
cutover.https://github.com/aditisrivastava811/usds-claims-validation/blob/main/FEEDBACK.md

The most interesting part was identifying discrepancies that are statistically
small but operationally important (e.g. payment-related fields) and deciding
how to surface them without over-noising the analysis.


Interpretation of Requirements

I interpreted the assignment as an exercise in:
Building a reliable validation pipeline
Making explicit design tradeoffs
Communicating findings clearly to technical and non-technical stakeholders
The instruction to “use the language and database you consider most appropriate” guided my decision to prioritize correctness, clarity, and reproducibility over infrastructure complexity.

Datastore Decision

I chose CSV artifacts as a lightweight datastore rather than standing up a full database. This allowed for:
Faster iteration within the time constraint
Clear, auditable outputs
Easy reviewer access without environment setup
If extending this system, I would migrate outputs to DuckDB or Postgres for incremental and query-optimized validation.

Scope Decisions
Full carrier claims comparison was intentionally deferred due to dataset size. Rather than forcing an impractical implementation, I documented a scalable aggregation-based approach consistent with production validation practices.


Time Spent

Approximate total time spent (excluding environment setup): **5 hours**

This includes:
- Input validation and ingest
- ID and column-level comparisons
- Output artifact generation
- Documentation and interpretation


Fit for the Role

This exercise aligns well with my background in building and validating data
pipelines that support high-stakes operational decisions.

I am particularly comfortable with:
- Defensive data validation
- Large-file ingestion strategies
- Designing pipelines that favor transparency and auditability
- Communicating technical results to non-technical stakeholders

I look forward to discussing this assignment. 

