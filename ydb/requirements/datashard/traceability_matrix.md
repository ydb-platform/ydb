# Traceability Matrix

### Datashards
#### [ISSUE-14851](https://github.com/ydb-platform/ydb/issues/14851): Test Suite: datashard/types
[![PROGRESS](https://img.shields.io/badge/PROGRESS-1%2F12:8%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14851-test-suite-datashardtypes)

**Description**: * get all types of data;
* get a column feature: regular column, primary key, indexed column, TTL column, ...;
* perform all operations, including SELECT, INSERT, DELETE, TTL,  CDC, ASYNC REPLICATION, BACKUP/RESTORE, INDEX...

Надо не забыть тесты-кейсы с совместимостью версий (фреймворк с @maximyurchuk )

Есть мнение, что после реализации тест-кейсов нам очень бы помог coverage для анализа непокрытого кода

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14033 | Stress test for all supported data types | Write a stress test which performs the steps: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14033)](https://github.com/ydb-platform/ydb/issues/14033) | Pending |
| #14980 | Test for DML operations and all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14980)](https://github.com/ydb-platform/ydb/issues/14980) | Pending |
| #14981 | Test for TTL on all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14981)](https://github.com/ydb-platform/ydb/issues/14981) | Pending |
| #14983 | Test for async replication for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14983)](https://github.com/ydb-platform/ydb/issues/14983) | Pending |
| #14984 | Test for dump/restore to filesystem for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14984)](https://github.com/ydb-platform/ydb/issues/14984) | Pending |
| #14985 | Test for export/import to S3 for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14985)](https://github.com/ydb-platform/ydb/issues/14985) | Pending |
| #14986 | Test for secondary indexes for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14986)](https://github.com/ydb-platform/ydb/issues/14986) | Pending |
| #14987 | Test for vector index for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14987)](https://github.com/ydb-platform/ydb/issues/14987) | Pending |
| #14988 | Test for copy table for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14988)](https://github.com/ydb-platform/ydb/issues/14988) | Pending |
| #14989 | Test for split/merge for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14989)](https://github.com/ydb-platform/ydb/issues/14989) | Pending |
| #14990 | Test for parametrized queries for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14990)](https://github.com/ydb-platform/ydb/issues/14990) | Pending |
| #14991 | Test for all clauses of SELECT for all types | 1. Create a table with: | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14991)](https://github.com/ydb-platform/ydb/issues/14991) | Pending |

