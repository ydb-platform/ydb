# Traceability Matrix

#### [ISSUE-14851](https://github.com/ydb-platform/ydb/issues/14851): Test Suite: datashard/types
[![PROGRESS](https://img.shields.io/badge/PROGRESS-1%2F12:8%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14851-test-suite-datashardtypes)

**Description**: * get all types of data;
* get a column feature: regular column, primary key, indexed column, TTL column, ...;
* perform all operations, including SELECT, INSERT, DELETE, TTL,  CDC, ASYNC REPLICATION, BACKUP/RESTORE, INDEX...

Надо не забыть тесты-кейсы с совместимостью версий (фреймворк с @maximyurchuk )

Есть мнение, что после реализации тест-кейсов нам очень бы помог coverage для анализа непокрытого кода

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14033 | Stress test for all supported data types | Write a stress test which performs the steps:<br>\* create an oltp table with all supported data types;<br>\* run UPSERT, SELECT, DELETE operations in a long cycle;<br>\* drop the table.<br><br><br> | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14033)](https://github.com/ydb-platform/ydb/issues/14033) | Pending |
| #14980 | Test for DML operations and all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Make different DML operations : UPDATE, INSERT, UPSERT, DELETE<br><br>3. Check SELECT results | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14980)](https://github.com/ydb-platform/ydb/issues/14980) | Pending |
| #14981 | Test for TTL on all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br>\* TTL turned on.<br><br>2. Insert data.<br><br>3. Check that some data is deleted by TTL. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14981)](https://github.com/ydb-platform/ydb/issues/14981) | Pending |
| #14983 | Test for async replication for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Create async replication.<br><br>3. Write some data to the source table.<br><br>4. Check that data appear in the destinations table. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14983)](https://github.com/ydb-platform/ydb/issues/14983) | Pending |
| #14984 | Test for dump/restore to filesystem for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Insert some data.<br><br>3. Dump to file system.<br><br>4. Restore from file system.<br><br>5. Check all data exists. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14984)](https://github.com/ydb-platform/ydb/issues/14984) | Pending |
| #14985 | Test for export/import to S3 for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Insert some data.<br><br>3. Export to S3.<br><br>4. Import from S3.<br><br>5. Check all data exists. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14985)](https://github.com/ydb-platform/ydb/issues/14985) | Pending |
| #14986 | Test for secondary indexes for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Insert some data.<br><br>3. Select WITH index.<br> | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14986)](https://github.com/ydb-platform/ydb/issues/14986) | Pending |
| #14987 | Test for vector index for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Insert some data.<br><br>3. Select WITH vector index. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14987)](https://github.com/ydb-platform/ydb/issues/14987) | Pending |
| #14988 | Test for copy table for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Insert some data.<br><br>3. Copy table.<br><br>4. Check that all data exists. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14988)](https://github.com/ydb-platform/ydb/issues/14988) | Pending |
| #14989 | Test for split/merge for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Insert some data.<br><br>3. Split table.<br><br>4. Check that all data exists.<br><br>5. Merge table.<br><br>6. Check that all data exists. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14989)](https://github.com/ydb-platform/ydb/issues/14989) | Pending |
| #14990 | Test for parametrized queries for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Insert some data using parametrized queries.<br><br>3. Select all data using parametrized queries. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14990)](https://github.com/ydb-platform/ydb/issues/14990) | Pending |
| #14991 | Test for all clauses of SELECT for all types | 1. Create a table with:<br>\* all types of data;<br>\* all types of column features: regular column, primary key, indexed column, TTL column, ...<br><br>2. Insert some data.<br><br>3. Select data using [all the clauses of SELECT](https://ydb.tech/docs/en/yql/reference/syntax/select/\#clauses\-supported\-in\-select) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14991)](https://github.com/ydb-platform/ydb/issues/14991) | Pending |

### Import/Export via CLI
#### REQ-DS-001: Import/Export via CLI
**Description**: Provide robust CLI tools to facilitate data operations for users managing datashards.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-DS-001-1.1 | Import from CSV - 1GB | Validate import from CSV file. |  | Pending |
| REQ-DS-001-1.2 | Export to JSON - 10GB | Validate export to JSON data. |  | Pending |
| REQ-DS-001-1.3 | Import from Parquet - 100GB | Validate import from Parquet file. |  | Pending |
| REQ-DS-001-1.4 | Export to CSV - 1TB | Validate export to CSV data. |  | Pending |

