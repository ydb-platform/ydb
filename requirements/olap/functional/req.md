# Requirements for YDB Analytics System

## Introduction
This document outlines the detailed functional and non-functional requirements for the YDB analytics system, including associated test cases for verification.

## Functional Requirements

### Bulk Upsert Data Insertion

- **REQ-BULK-001**: The system should support data insertion using `bulk_upsert` across all data types and transports.
  - **Description**: Enable the insertion of all data types (e.g., Int32, Uint64, String) using transports like arrow and BoxedValue, ensuring data visibility and integrity.
  - **Cases**:
    - Case 1.1: [All Data Types Support via Arrow](path/to/test/1) - Verify bulk insertion of all data types using arrow transport. Status: Pending
    - Case 1.2: [All Data Types Support via BoxedValue](path/to/test/2) - Verify bulk insertion using BoxedValue transport. Status: Pending
    - Case 1.3: [Data Visibility Post-Insertion](path/to/test/3) - Ensure latest values are visible post-insertion.
    - Case 1.4: [Handling Duplicate Keys in Batch](path/to/test/4) - Confirm the system records the last occurrence for duplicate keys in a batch.
    - Case 1.5: [JDBC and Python DBApi Integration](path/to/test/5) - Validate insertion through both JDBC and Python DBApi interfaces.
    - Case 1.6: [Data Integrity Violation Prevention](path/to/test/6) - Test bulk upsert with integrity issues (e.g., invalid UINT value of -1) to ensure no problematic data is applied.
    - Case 1.7: [Parallel Execution in Threads](path/to/test/7) - Verify that parallel executions do not result in errors when multiple threads perform bulk upserts.
    - Case 1.8: [Insertion Speed Comparison](path/to/test/8) - Compare performance of bulk upsert versus INSERT INTO for 100,000 rows.
    - Case 1.9: [Server Failure Handling](path/to/test/9) - Confirm appropriate error handling and retry behavior during cluster shutdown and restart scenarios.
    - Case 1.10: [ALTER TABLE ADD COLUMN Operation](path/to/test/10) - Ensure bulk upsert completion when a table schema is altered mid-operation.
    - Case 1.11: [Metric Visibility in UI](path/to/test/11) - Validate that operational metrics are visible and accurate in the user interface.


### INSERT INTO, UPSERT, and REPLACE Operations

- #14668

### Data Reading Operations

- #14680
- #13527
- #14639

### Other
- #13952
- #13956
- #13959
- #14601

### INSERT INTO, UPSERT, and REPLACE Operations

- **REQ-INS-001**: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.
  - **Description**: Ensure data can be inserted or updated with proper handling of duplicates and expected transactional behaviors.
  - **Cases**:
    - Case 2.1: [Data Type Support in All Columns](path/to/test/12) - Verify data types, including PK fields, are supported in all columns.
    - Case 2.2: [Handling Existing Data with INSERT INTO](path/to/test/13) - Confirm errors are appropriately thrown when inserting existing data.
    - Case 2.3: [UPSERT and REPLACE Operations](path/to/test/14) - Validate that UPSERT updates and REPLACE correctly overwrites existing data.
    - Case 2.4: [Handling New Data with INSERT INTO](path/to/test/15) - Validate new data insertion behavior.
    - Case 2.5: [Large Dataset Insertion](path/to/test/16) - Test the system's capability to handle inserting 1 million rows in a single operation.
    - Case 2.6: [Transaction Rollback Effects](path/to/test/17) - Validate data visibility and integrity after transaction rollbacks.
    - Case 2.7: [Concurrent Transactions](path/to/test/18) - Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back.
    - Case 2.8: [Parallel Insertion in Multiple Threads](path/to/test/19) - Verify that parallel data insertion operations across threads execute without conflicts or errors.
    - Case 2.9: [Data Integrity Violation for INSERT INTO](path/to/test/20) - Ensure that inserting data with violation does not succeed, similar to bulk upsert.
    - Case 2.10: [Cluster Failure During INSERT INTO](path/to/test/21) - Confirm error messages and retry logic during cluster interruptions are handled consistently.
    - Case 2.11: [Data Representation in GUI](path/to/test/22) - Ensure data volumes are reflected accurately in reports and dashboards.

### Data Reading Operations

- **REQ-READ-001**: Provide robust and efficient data reading capabilities.
  - **Description**: Execute and validate operations concerning reading data, including aggregation and supported data types.
  - **Cases**:
    - Case 3.1: [LogBench Operations](path/to/test/23) - Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets.
    - Case 3.2: [TPC-H S1 Execution](path/to/test/24) - Confirm successful execution and performance of TPC-H S1 queries.

## Federated Queries Support

- **REQ-FEDQ-001**: Allow and manage federated query execution.
  - **Description**: Enable and validate operations involving federated query execution, taking advantage of diverse data sources.
  - **Cases**:
    - Case 10.1: [Cross-Source Federated Queries](path/to/test/40) - Test execution of queries spanning multiple federated data sources.
    - Case 10.2: [Federated Source Data Insertions](path/to/test/41) - Validate data insertions deriving from federated sources.