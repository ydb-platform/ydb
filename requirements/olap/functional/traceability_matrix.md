# Traceability Matrix

## REQ-FEDQ-001
**Description**: Allow and manage federated query execution.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-FEDQ-001-10.1 | Test execution of queries spanning multiple federated data sources. | path/to/test/40 | N/A | Pending |
| REQ-FEDQ-001-10.2 | Validate data insertions deriving from federated sources. | path/to/test/41 | N/A | Pending |

## REQ-BULK-001
**Description**: The system should support data insertion using `bulk_upsert` across all data types and transports.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-BULK-001-1.1 | Verify bulk insertion of all data types using arrow transport. Status: Pending | path/to/test/1 | N/A | Pending |
| REQ-BULK-001-1.2 | Verify bulk insertion using BoxedValue transport. Status: Pending | path/to/test/2 | N/A | Pending |
| REQ-BULK-001-1.3 | Ensure latest values are visible post-insertion. | path/to/test/3 | N/A | Pending |
| REQ-BULK-001-1.4 | Confirm the system records the last occurrence for duplicate keys in a batch. | path/to/test/4 | N/A | Pending |
| REQ-BULK-001-1.5 | Validate insertion through both JDBC and Python DBApi interfaces. | path/to/test/5 | N/A | Pending |
| REQ-BULK-001-1.6 | Test bulk upsert with integrity issues (e.g., invalid UINT value of -1) to ensure no problematic data is applied. | path/to/test/6 | N/A | Pending |
| REQ-BULK-001-1.7 | Verify that parallel executions do not result in errors when multiple threads perform bulk upserts. | path/to/test/7 | N/A | Pending |
| REQ-BULK-001-1.8 | Compare performance of bulk upsert versus INSERT INTO for 100,000 rows. | path/to/test/8 | N/A | Pending |
| REQ-BULK-001-1.9 | Confirm appropriate error handling and retry behavior during cluster shutdown and restart scenarios. | path/to/test/9 | N/A | Pending |
| REQ-BULK-001-1.10 | Ensure bulk upsert completion when a table schema is altered mid-operation. | path/to/test/10 | N/A | Pending |
| REQ-BULK-001-1.11 | Validate that operational metrics are visible and accurate in the user interface. | path/to/test/11 | N/A | Pending |

## REQ-INS-001
**Description**: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-INS-001-2.1 | Verify data types, including PK fields, are supported in all columns. | path/to/test/12 | N/A | Pending |
| REQ-INS-001-2.2 | Confirm errors are appropriately thrown when inserting existing data. | path/to/test/13 | N/A | Pending |
| REQ-INS-001-2.3 | Validate that UPSERT updates and REPLACE correctly overwrites existing data. | path/to/test/14 | N/A | Pending |
| REQ-INS-001-2.4 | Validate new data insertion behavior. | path/to/test/15 | N/A | Pending |
| REQ-INS-001-2.5 | Test the system's capability to handle inserting 1 million rows in a single operation. | path/to/test/16 | N/A | Pending |
| REQ-INS-001-2.6 | Validate data visibility and integrity after transaction rollbacks. | path/to/test/17 | N/A | Pending |
| REQ-INS-001-2.7 | Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back. | path/to/test/18 | N/A | Pending |
| REQ-INS-001-2.8 | Verify that parallel data insertion operations across threads execute without conflicts or errors. | path/to/test/19 | N/A | Pending |
| REQ-INS-001-2.9 | Ensure that inserting data with violation does not succeed, similar to bulk upsert. | path/to/test/20 | N/A | Pending |
| REQ-INS-001-2.10 | Confirm error messages and retry logic during cluster interruptions are handled consistently. | path/to/test/21 | N/A | Pending |
| REQ-INS-001-2.11 | Ensure data volumes are reflected accurately in reports and dashboards. | path/to/test/22 | N/A | Pending |

## REQ-READ-001
**Description**: Provide robust and efficient data reading capabilities.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-READ-001-3.1 | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. | path/to/test/23 | N/A | Pending |
| REQ-READ-001-3.2 | Confirm successful execution and performance of TPC-H S1 queries. | path/to/test/24 | N/A | Pending |

## REQ-INS-001
**Description**: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-INS-001-2.1 | Verify data types, including PK fields, are supported in all columns. | path/to/test/12 | N/A | Pending |
| REQ-INS-001-2.2 | Confirm errors are appropriately thrown when inserting existing data. | path/to/test/13 | N/A | Pending |
| REQ-INS-001-2.3 | Validate that UPSERT updates and REPLACE correctly overwrites existing data. | path/to/test/14 | N/A | Pending |
| REQ-INS-001-2.4 | Validate new data insertion behavior. | path/to/test/15 | N/A | Pending |
| REQ-INS-001-2.5 | Test the system's capability to handle inserting 1 million rows in a single operation. | path/to/test/16 | N/A | Pending |
| REQ-INS-001-2.6 | Validate data visibility and integrity after transaction rollbacks. | path/to/test/17 | N/A | Pending |
| REQ-INS-001-2.7 | Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back. | path/to/test/18 | N/A | Pending |
| REQ-INS-001-2.8 | Verify that parallel data insertion operations across threads execute without conflicts or errors. | path/to/test/19 | N/A | Pending |
| REQ-INS-001-2.9 | Ensure that inserting data with violation does not succeed, similar to bulk upsert. | path/to/test/20 | N/A | Pending |
| REQ-INS-001-2.10 | Confirm error messages and retry logic during cluster interruptions are handled consistently. | path/to/test/21 | N/A | Pending |
| REQ-INS-001-2.11 | Ensure data volumes are reflected accurately in reports and dashboards. | path/to/test/22 | N/A | Pending |

## REQ-READ-001
**Description**: Provide robust and efficient data reading capabilities.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-READ-001-3.1 | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. | path/to/test/23 | N/A | Pending |
| REQ-READ-001-3.2 | Confirm successful execution and performance of TPC-H S1 queries. | path/to/test/24 | N/A | Pending |

## REQ-BULK-001
**Description**: The system should support data insertion using `bulk_upsert` across all data types and transports.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-BULK-001-1.1 | Verify bulk insertion of all data types using arrow transport. Status: Pending | path/to/test/1 | N/A | Pending |
| REQ-BULK-001-1.2 | Verify bulk insertion using BoxedValue transport. Status: Pending | path/to/test/2 | N/A | Pending |
| REQ-BULK-001-1.3 | Ensure latest values are visible post-insertion. | path/to/test/3 | N/A | Pending |
| REQ-BULK-001-1.4 | Confirm the system records the last occurrence for duplicate keys in a batch. | path/to/test/4 | N/A | Pending |
| REQ-BULK-001-1.5 | Validate insertion through both JDBC and Python DBApi interfaces. | path/to/test/5 | N/A | Pending |
| REQ-BULK-001-1.6 | Test bulk upsert with integrity issues (e.g., invalid UINT value of -1) to ensure no problematic data is applied. | path/to/test/6 | N/A | Pending |
| REQ-BULK-001-1.7 | Verify that parallel executions do not result in errors when multiple threads perform bulk upserts. | path/to/test/7 | N/A | Pending |
| REQ-BULK-001-1.8 | Compare performance of bulk upsert versus INSERT INTO for 100,000 rows. | path/to/test/8 | N/A | Pending |
| REQ-BULK-001-1.9 | Confirm appropriate error handling and retry behavior during cluster shutdown and restart scenarios. | path/to/test/9 | N/A | Pending |
| REQ-BULK-001-1.10 | Ensure bulk upsert completion when a table schema is altered mid-operation. | path/to/test/10 | N/A | Pending |
| REQ-BULK-001-1.11 | Validate that operational metrics are visible and accurate in the user interface. | path/to/test/11 | N/A | Pending |

## REQ-INS-001
**Description**: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-INS-001-2.1 | Verify data types, including PK fields, are supported in all columns. | path/to/test/12 | N/A | Pending |
| REQ-INS-001-2.2 | Confirm errors are appropriately thrown when inserting existing data. | path/to/test/13 | N/A | Pending |
| REQ-INS-001-2.3 | Validate that UPSERT updates and REPLACE correctly overwrites existing data. | path/to/test/14 | N/A | Pending |
| REQ-INS-001-2.4 | Validate new data insertion behavior. | path/to/test/15 | N/A | Pending |
| REQ-INS-001-2.5 | Test the system's capability to handle inserting 1 million rows in a single operation. | path/to/test/16 | N/A | Pending |
| REQ-INS-001-2.6 | Validate data visibility and integrity after transaction rollbacks. | path/to/test/17 | N/A | Pending |
| REQ-INS-001-2.7 | Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back. | path/to/test/18 | N/A | Pending |
| REQ-INS-001-2.8 | Verify that parallel data insertion operations across threads execute without conflicts or errors. | path/to/test/19 | N/A | Pending |
| REQ-INS-001-2.9 | Ensure that inserting data with violation does not succeed, similar to bulk upsert. | path/to/test/20 | N/A | Pending |
| REQ-INS-001-2.10 | Confirm error messages and retry logic during cluster interruptions are handled consistently. | path/to/test/21 | N/A | Pending |
| REQ-INS-001-2.11 | Ensure data volumes are reflected accurately in reports and dashboards. | path/to/test/22 | N/A | Pending |

## REQ-READ-001
**Description**: Provide robust and efficient data reading capabilities.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-READ-001-3.1 | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. | path/to/test/23 | N/A | Pending |
| REQ-READ-001-3.2 | Confirm successful execution and performance of TPC-H S1 queries. | path/to/test/24 | N/A | Pending |

## REQ-INS-001
**Description**: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-INS-001-2.1 | Verify data types, including PK fields, are supported in all columns. | path/to/test/12 | N/A | Pending |
| REQ-INS-001-2.2 | Confirm errors are appropriately thrown when inserting existing data. | path/to/test/13 | N/A | Pending |
| REQ-INS-001-2.3 | Validate that UPSERT updates and REPLACE correctly overwrites existing data. | path/to/test/14 | N/A | Pending |
| REQ-INS-001-2.4 | Validate new data insertion behavior. | path/to/test/15 | N/A | Pending |
| REQ-INS-001-2.5 | Test the system's capability to handle inserting 1 million rows in a single operation. | path/to/test/16 | N/A | Pending |
| REQ-INS-001-2.6 | Validate data visibility and integrity after transaction rollbacks. | path/to/test/17 | N/A | Pending |
| REQ-INS-001-2.7 | Ensure that inserts in concurrent transactions to the same key are properly managed, with one completing while another rolls back. | path/to/test/18 | N/A | Pending |
| REQ-INS-001-2.8 | Verify that parallel data insertion operations across threads execute without conflicts or errors. | path/to/test/19 | N/A | Pending |
| REQ-INS-001-2.9 | Ensure that inserting data with violation does not succeed, similar to bulk upsert. | path/to/test/20 | N/A | Pending |
| REQ-INS-001-2.10 | Confirm error messages and retry logic during cluster interruptions are handled consistently. | path/to/test/21 | N/A | Pending |
| REQ-INS-001-2.11 | Ensure data volumes are reflected accurately in reports and dashboards. | path/to/test/22 | N/A | Pending |

## REQ-READ-001
**Description**: Provide robust and efficient data reading capabilities.

| Case ID | Description | Path | Issue | Test Case Status |
|---------|-------------|------|-------|------------------|
| REQ-READ-001-3.1 | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. | path/to/test/23 | N/A | Pending |
| REQ-READ-001-3.2 | Confirm successful execution and performance of TPC-H S1 queries. | path/to/test/24 | N/A | Pending |

