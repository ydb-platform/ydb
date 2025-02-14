# Traceability Matrix

## Functional Requirements

### Bulk Upsert Data Insertion
#### REQ-BULK-001
Description: The system should support data insertion using `bulk_upsert` across all data types and transports.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-BULK-001-1.1 | All Data Types Support via Arrow | Verify bulk insertion of all data types using arrow transport. Status: Pending |  | Pending |
| REQ-BULK-001-1.2 | All Data Types Support via BoxedValue | Verify bulk insertion using BoxedValue transport. Status: Pending |  | Pending |
| REQ-BULK-001-1.3 | Data Visibility Post-Insertion | Ensure latest values are visible post-insertion. |  | Pending |
| REQ-BULK-001-1.4 | Handling Duplicate Keys in Batch | Confirm the system records the last occurrence for duplicate keys in a batch. |  | Pending |
| REQ-BULK-001-1.5 | JDBC and Python DBApi Integration | Validate insertion through both JDBC and Python DBApi interfaces. |  | Pending |
| REQ-BULK-001-1.6 | Data Integrity Violation Prevention | Test bulk upsert with integrity issues (e.g., invalid UINT value of -1) to ensure no problematic data is applied. |  | Pending |
| REQ-BULK-001-1.7 | Parallel Execution in Threads | Verify that parallel executions do not result in errors when multiple threads perform bulk upserts. |  | Pending |
| REQ-BULK-001-1.8 | Insertion Speed Comparison | Compare performance of bulk upsert versus INSERT INTO for 100,000 rows. |  | Pending |
| REQ-BULK-001-1.9 | Server Failure Handling | Confirm appropriate error handling and retry behavior during cluster shutdown and restart scenarios. |  | Pending |
| REQ-BULK-001-1.10 | ALTER TABLE ADD COLUMN Operation | Ensure bulk upsert completion when a table schema is altered mid-operation. |  | Pending |
| REQ-BULK-001-1.11 | Metric Visibility in UI | Validate that operational metrics are visible and accurate in the user interface. |  | Pending |

### INSERT INTO, UPSERT, and REPLACE Operations
#### REQ-INS-001
Description: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-INS-001-2.1 | Data Type Support in All Columns | Verify data types, including PK fields, are supported in all columns. |  | Pending |
| REQ-INS-001-2.2 | Handling Existing Data with INSERT INTO | Confirm errors are appropriately thrown when inserting existing data. |  | Pending |
| REQ-INS-001-2.3 | UPSERT and REPLACE Operations | Validate that UPSERT updates and REPLACE correctly overwrites existing data. |  | Pending |
| REQ-INS-001-2.4 | Handling New Data with INSERT INTO | Validate new data insertion behavior. |  | Pending |
| REQ-INS-001-2.5 | Large Dataset Insertion | Test the system's capability to handle inserting 1 million rows in a single operation. |  | Pending |
| REQ-INS-001-2.6 | Transaction Rollback Effects | Validate data visibility and integrity after transaction rollbacks. |  | Pending |
| REQ-INS-001-2.7 | Concurrent Transactions | Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back. |  | Pending |
| REQ-INS-001-2.8 | Parallel Insertion in Multiple Threads | Verify that parallel data insertion operations across threads execute without conflicts or errors. |  | Pending |
| REQ-INS-001-2.9 | Data Integrity Violation for INSERT INTO | Ensure that inserting data with violation does not succeed, similar to bulk upsert. |  | Pending |
| REQ-INS-001-2.10 | Cluster Failure During INSERT INTO | Confirm error messages and retry logic during cluster interruptions are handled consistently. |  | Pending |
| REQ-INS-001-2.11 | Data Representation in GUI | Ensure data volumes are reflected accurately in reports and dashboards. |  | Pending |

### Data Reading Operations
#### REQ-READ-001
Description: Provide robust and efficient data reading capabilities.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-READ-001-3.1 | LogBench Operations | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. |  | Pending |
| REQ-READ-001-3.2 | TPC-H S1 Execution | Confirm successful execution and performance of TPC-H S1 queries. |  | Pending |

### INSERT INTO, UPSERT, and REPLACE Operations
#### REQ-INS-001
Description: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-INS-001-2.1 | Data Type Support in All Columns | Verify data types, including PK fields, are supported in all columns. |  | Pending |
| REQ-INS-001-2.2 | Handling Existing Data with INSERT INTO | Confirm errors are appropriately thrown when inserting existing data. |  | Pending |
| REQ-INS-001-2.3 | UPSERT and REPLACE Operations | Validate that UPSERT updates and REPLACE correctly overwrites existing data. |  | Pending |
| REQ-INS-001-2.4 | Handling New Data with INSERT INTO | Validate new data insertion behavior. |  | Pending |
| REQ-INS-001-2.5 | Large Dataset Insertion | Test the system's capability to handle inserting 1 million rows in a single operation. |  | Pending |
| REQ-INS-001-2.6 | Transaction Rollback Effects | Validate data visibility and integrity after transaction rollbacks. |  | Pending |
| REQ-INS-001-2.7 | Concurrent Transactions | Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back. |  | Pending |
| REQ-INS-001-2.8 | Parallel Insertion in Multiple Threads | Verify that parallel data insertion operations across threads execute without conflicts or errors. |  | Pending |
| REQ-INS-001-2.9 | Data Integrity Violation for INSERT INTO | Ensure that inserting data with violation does not succeed, similar to bulk upsert. |  | Pending |
| REQ-INS-001-2.10 | Cluster Failure During INSERT INTO | Confirm error messages and retry logic during cluster interruptions are handled consistently. |  | Pending |
| REQ-INS-001-2.11 | Data Representation in GUI | Ensure data volumes are reflected accurately in reports and dashboards. |  | Pending |

### Data Reading Operations
#### REQ-READ-001
Description: Provide robust and efficient data reading capabilities.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-READ-001-3.1 | LogBench Operations | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. |  | Pending |
| REQ-READ-001-3.2 | TPC-H S1 Execution | Confirm successful execution and performance of TPC-H S1 queries. |  | Pending |

## Federated Queries Support

#### REQ-FEDQ-001
Description: Allow and manage federated query execution.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-FEDQ-001-10.1 | Cross-Source Federated Queries | Test execution of queries spanning multiple federated data sources. |  | Pending |
| REQ-FEDQ-001-10.2 | Federated Source Data Insertions | Validate data insertions deriving from federated sources. |  | Pending |

