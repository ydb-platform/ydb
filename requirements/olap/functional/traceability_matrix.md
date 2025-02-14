# Traceability Matrix

## Functional Requirements

### Bulk Upsert Data Insertion
#### REQ-BULK-001
**Description**: The system should support data insertion using `bulk_upsert` across all data types and transports.

| Case ID | Name | Description | Issue | Test Case Status |
|---------|------|-------------|-------|------------------|
| REQ-BULK-001-1.1 | Verify bulk insertion of all data types using arrow transport. Status: Pending | N/A | Pending |
| REQ-BULK-001-1.2 | Verify bulk insertion using BoxedValue transport. Status: Pending | N/A | Pending |
| REQ-BULK-001-1.3 | Ensure latest values are visible post-insertion. | N/A | Pending |
| REQ-BULK-001-1.4 | Confirm the system records the last occurrence for duplicate keys in a batch. | N/A | Pending |
| REQ-BULK-001-1.5 | Validate insertion through both JDBC and Python DBApi interfaces. | N/A | Pending |
| REQ-BULK-001-1.6 | Test bulk upsert with integrity issues (e.g., invalid UINT value of -1) to ensure no problematic data is applied. | N/A | Pending |
| REQ-BULK-001-1.7 | Verify that parallel executions do not result in errors when multiple threads perform bulk upserts. | N/A | Pending |
| REQ-BULK-001-1.8 | Compare performance of bulk upsert versus INSERT INTO for 100,000 rows. | N/A | Pending |
| REQ-BULK-001-1.9 | Confirm appropriate error handling and retry behavior during cluster shutdown and restart scenarios. | N/A | Pending |
| REQ-BULK-001-1.10 | Ensure bulk upsert completion when a table schema is altered mid-operation. | N/A | Pending |
| REQ-BULK-001-1.11 | Validate that operational metrics are visible and accurate in the user interface. | N/A | Pending |

### INSERT INTO, UPSERT, and REPLACE Operations
#### REQ-INS-001
**Description**: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.

| Case ID | Name | Description | Issue | Test Case Status |
|---------|------|-------------|-------|------------------|
| REQ-INS-001-2.1 | Verify data types, including PK fields, are supported in all columns. | N/A | Pending |
| REQ-INS-001-2.2 | Confirm errors are appropriately thrown when inserting existing data. | N/A | Pending |
| REQ-INS-001-2.3 | Validate that UPSERT updates and REPLACE correctly overwrites existing data. | N/A | Pending |
| REQ-INS-001-2.4 | Validate new data insertion behavior. | N/A | Pending |
| REQ-INS-001-2.5 | Test the system's capability to handle inserting 1 million rows in a single operation. | N/A | Pending |
| REQ-INS-001-2.6 | Validate data visibility and integrity after transaction rollbacks. | N/A | Pending |
| REQ-INS-001-2.7 | Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back. | N/A | Pending |
| REQ-INS-001-2.8 | Verify that parallel data insertion operations across threads execute without conflicts or errors. | N/A | Pending |
| REQ-INS-001-2.9 | Ensure that inserting data with violation does not succeed, similar to bulk upsert. | N/A | Pending |
| REQ-INS-001-2.10 | Confirm error messages and retry logic during cluster interruptions are handled consistently. | N/A | Pending |
| REQ-INS-001-2.11 | Ensure data volumes are reflected accurately in reports and dashboards. | N/A | Pending |

### Data Reading Operations
#### REQ-READ-001
**Description**: Provide robust and efficient data reading capabilities.

| Case ID | Name | Description | Issue | Test Case Status |
|---------|------|-------------|-------|------------------|
| REQ-READ-001-3.1 | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. | N/A | Pending |
| REQ-READ-001-3.2 | Confirm successful execution and performance of TPC-H S1 queries. | N/A | Pending |

### INSERT INTO, UPSERT, and REPLACE Operations
#### REQ-INS-001
**Description**: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.

| Case ID | Name | Description | Issue | Test Case Status |
|---------|------|-------------|-------|------------------|
| REQ-INS-001-2.1 | Verify data types, including PK fields, are supported in all columns. | N/A | Pending |
| REQ-INS-001-2.2 | Confirm errors are appropriately thrown when inserting existing data. | N/A | Pending |
| REQ-INS-001-2.3 | Validate that UPSERT updates and REPLACE correctly overwrites existing data. | N/A | Pending |
| REQ-INS-001-2.4 | Validate new data insertion behavior. | N/A | Pending |
| REQ-INS-001-2.5 | Test the system's capability to handle inserting 1 million rows in a single operation. | N/A | Pending |
| REQ-INS-001-2.6 | Validate data visibility and integrity after transaction rollbacks. | N/A | Pending |
| REQ-INS-001-2.7 | Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back. | N/A | Pending |
| REQ-INS-001-2.8 | Verify that parallel data insertion operations across threads execute without conflicts or errors. | N/A | Pending |
| REQ-INS-001-2.9 | Ensure that inserting data with violation does not succeed, similar to bulk upsert. | N/A | Pending |
| REQ-INS-001-2.10 | Confirm error messages and retry logic during cluster interruptions are handled consistently. | N/A | Pending |
| REQ-INS-001-2.11 | Ensure data volumes are reflected accurately in reports and dashboards. | N/A | Pending |

### Data Reading Operations
#### REQ-READ-001
**Description**: Provide robust and efficient data reading capabilities.

| Case ID | Name | Description | Issue | Test Case Status |
|---------|------|-------------|-------|------------------|
| REQ-READ-001-3.1 | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. | N/A | Pending |
| REQ-READ-001-3.2 | Confirm successful execution and performance of TPC-H S1 queries. | N/A | Pending |

