# Datashards


- #14851

### Import/Export via CLI

- **REQ-DS-001**: Import/Export via CLI
  - **Functionality**: Implement and document CLI commands for importing and exporting data from/to datashards.
  - **Description**: Provide robust CLI tools to facilitate data operations for users managing datashards.
  - **Acceptance Criteria**:
    - Successful execution of `ydb import` and `ydb export` commands.
    - Verification of data integrity post-import and post-export.
    - Documentation includes examples of usage.
    - Integration testing with various data formats (JSON, CSV, Parquet).
  - **Cases**:
    - Case 1.1: [Import from CSV - 1GB](path/to/test/import_csv_1gb) - Validate import from CSV file.
    - Case 1.2: [Export to JSON - 10GB](path/to/test/export_json_10gb) - Validate export to JSON data.
    - Case 1.3: [Import from Parquet - 100GB](path/to/test/import_parquet_100gb) - Validate import from Parquet file.
    - Case 1.4: [Export to CSV - 1TB](path/to/test/export_csv_1tb) - Validate export to CSV data.
