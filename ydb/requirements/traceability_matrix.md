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

## Functional Requirements

### Bulk Upsert Data Insertion
#### [ISSUE-14639](https://github.com/ydb-platform/ydb/issues/14639): Test suite: cs/write data
[![PROGRESS](https://img.shields.io/badge/PROGRESS-2%2F8:25%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14639-test-suite-cswrite-data)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14640 | You can write all kinds of data via bulk_upsert with all kinds of transport: arrow, BoxedValue |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14640)](https://github.com/ydb-platform/ydb/issues/14640) | Pending |
| #14642 | After a successful bulk_upsert write, the latest data values are visible | There can be multiple entries in bulk\_upsert with the same key. We expect that only the last record is written. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14642)](https://github.com/ydb-platform/ydb/issues/14642) | Pending |
| #14643 | If there are multiple identical keys within a single bulk_upsert data bundle, the last one is written | Test bulk upsert into the table with overlapping keys. Data are inserted by overlapping chunks<br> | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14643)](https://github.com/ydb-platform/ydb/issues/14643) | Pending |
| #14644 | Writing data to bulk_upsert with data integrity violation works correctly |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14644)](https://github.com/ydb-platform/ydb/issues/14644) | Pending |
| #14645 | When bulk_upsert is executed in parallel, the data is written to one table without errors | Test bulk upsert into the table with overlapping keys. Data are inserted by overlapping chunks | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14645)](https://github.com/ydb-platform/ydb/issues/14645) | Pending |
| #14646 | Writing milliards of rows via bulk_upsert is faster than a similar number of rows using INSERT INTO |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14646)](https://github.com/ydb-platform/ydb/issues/14646) | Pending |
| #14647 | If the cluster is stopped during bulk_upsert execution, an error is returned to the user |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14647)](https://github.com/ydb-platform/ydb/issues/14647) | Pending |
| #14648 | When inserting a large amount of data ALTER TABLE ADD COLUMN, bulk_upsert should complete successfully |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14648)](https://github.com/ydb-platform/ydb/issues/14648) | Pending |

### INSERT INTO, UPSERT, and REPLACE Operations
#### [ISSUE-14668](https://github.com/ydb-platform/ydb/issues/14668): Test suite: CS/(INSERT INTO/UPSERT/REPLACE) support
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F9:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14668-test-suite-cs-insert-intoupsertreplace-support)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14669 | It is possible to write data types in all columns, including PK and data |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14669)](https://github.com/ydb-platform/ydb/issues/14669) | Pending |
| #14670 | If the data already exists in the table, INSERT INTO returns an error, REPLACE/UPSERT overwrites it |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14670)](https://github.com/ydb-platform/ydb/issues/14670) | Pending |
| #14671 | If there is no data in the table, INSERT INTO inserts the data, REPLACE does nothing, UPSERT inserts the data |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14671)](https://github.com/ydb-platform/ydb/issues/14671) | Pending |
| #14672 | It is possible to write 1 million (? batch size) strings in a single call |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14672)](https://github.com/ydb-platform/ydb/issues/14672) | Pending |
| #14673 | When working in a transaction, if a rollback occurs, the data before the modification is visible (data is not modified) |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14673)](https://github.com/ydb-platform/ydb/issues/14673) | Pending |
| #14674 | If the work comes from multiple transactions, writing to the same key, one transaction is rolled back and the second transaction is successfully completed |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14674)](https://github.com/ydb-platform/ydb/issues/14674) | Pending |
| #14675 | You can insert data into one table in parallel in N threads |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14675)](https://github.com/ydb-platform/ydb/issues/14675) | Pending |
| #14676 | Try to write data using INSERT INTO with data integrity violation. For example, 100 rows, write -1 to one of them in the UINT field, no data from INSERT INTO is applied |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14676)](https://github.com/ydb-platform/ydb/issues/14676) | Pending |
| #14678 | If the cluster is stopped during INSERT INTO execution, an error is returned to the user. Alternatively, INSERT is expected to be retried until the server is restarted |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14678)](https://github.com/ydb-platform/ydb/issues/14678) | Pending |

### Data Reading Operations
#### [ISSUE-14680](https://github.com/ydb-platform/ydb/issues/14680): Test Suite: Reading data
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F2:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14680-test-suite-reading-data)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14679 | LogBench - working with reads, aggregation, JSON types. The test is successful on a small amount of data |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14679)](https://github.com/ydb-platform/ydb/issues/14679) | Pending |
| #14681 | TPCH S100 is successful |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14681)](https://github.com/ydb-platform/ydb/issues/14681) | Pending |

#### [ISSUE-13527](https://github.com/ydb-platform/ydb/issues/13527): Test Suite: cs/read-write
[![PROGRESS](https://img.shields.io/badge/PROGRESS-3%2F9:33%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-13527-test-suite-csread-write)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #13528 | Test cs read-write. Check all column types work | Для каждого типа проверяем, что он может быть PK/не PK столбцом через create table \+ тривиальную операцию. Убеждаемся что операции чтения\-записи проходят корректно<br><br>Частично реализован в https://github.com/ydb\-platform/ydb/blob/main/ydb/tests/stress/olap\_workload/workload/\_\_init\_\_.py без проверки записи/чтения<br><br>Есть известные неподдерживаемые типы:<br>https://github.com/ydb\-platform/ydb/issues/13037<br>https://github.com/ydb\-platform/ydb/issues/13048<br>https://github.com/ydb\-platform/ydb/issues/13047<br>https://github.com/ydb\-platform/ydb/issues/13050<br><br> | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13528)](https://github.com/ydb-platform/ydb/issues/13528) | Pending |
| #13529 | Test cs read-write. Quota exhaustion | Пишем в таблицу пока квота не закончится. Далее удаляем таблицу, убеждаемся что база работоспособна (путем манипулирования с таблицами) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13529)](https://github.com/ydb-platform/ydb/issues/13529) | Pending |
| #13530 | Test cs read-write. Log scenario (write in the end) | Делаем табличку с PK типа Timestamp<br><br>Делаем симуляцию записи логов в примерно в текущее время с некоторым разбросом (ydb workload log), проверяем что запись идет<br><br>Параллельно делаем read запросы с агрегацией (например по часам)<br><br>Выжидаем N минут, проверяем что запросы успешные. (N для PR мало, для остальных запусков больше)<br><br>Сам тест нужно параметризовать типом вставки bulk upsert/insert/upsert либо же лить данные разным способом<br><br><br>Тест должен также выявлять проблему, приведщую к SPI\-125178 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13530)](https://github.com/ydb-platform/ydb/issues/13530) | Pending |
| #13531 | Test cs read-write. Log scenario (random writes) | То же самое что и \#13530 , но необходимо писать в случайные точки, а не в конец  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13531)](https://github.com/ydb-platform/ydb/issues/13531) | Pending |
| #13532 | Test cs read-write. Log scenario (sparsed + many columns) | Включить sparse \+ то же самое что и \#13530 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13532)](https://github.com/ydb-platform/ydb/issues/13532) | Pending |
| #13652 | Test cs writes. Written duplicates doesn't provoke disk exhaustion | Писать в таблицу много дубликатов первичного ключа. Отслеживать потреблямый объём стораджа. Проверять, что потребление стораджа растёт быстрее, чем кол\-во строк в базе.<br>После того, как упёрлись в квоту, перестать писать и подождать пока данные скомпактятся, потребление уменьшится и можно писать дальше | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13652)](https://github.com/ydb-platform/ydb/issues/13652) | Pending |
| #13653 | Data deletion doesn't lead to disk exhaustion | Писать данные до того, как упрёмся в квоту. После этого удялять данные большими кусками. <br>Проверять, что в начале удаления потребление стораджа растёт выше квоты. <br>Проверять, что мы можем подойти к исчерпанию стораджа(не квоты) и удаления начнут возвращать ошибку<br>Дождаться компашна, проверить что потребление стораджа опустилось ниже квоты | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13653)](https://github.com/ydb-platform/ydb/issues/13653) | Pending |
| #13848 | Test CS RW load. Write, modify, read TBs | Проверка записи/модификации/чтения на больших объёмах данных (единицы\-десятки. TB)<br>Сравнить производительность  записи/чтения до модификаций во время модификаций после модификаций<br><br>1. Пишем 100 gib в CS измеряем время<br>2. Пишем еще 100 gib в CS и читаем измеряем время<br>3. Пишем еще 100 gib, параллельно модифицируем случаные строчки измеряем время<br>4. Пишем оставшиеся 700 gib, параллельно модифицируя случайны строчки, читая данные и измеряя время<br> | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13848)](https://github.com/ydb-platform/ydb/issues/13848) | Pending |
| #15512 | Test cs read. Memory over-usage by scan lead to failed queries, not OOM | Если данные порций, необходимые для скана, не помещаются в память ноды, скан должен падать с понятной ошибкой и не должен случаться OOM.<br><br>1. Отключить компакшн<br>2. Много раз повторно записать один батч данных<br>3. Выполнить SELECT, по этим данным | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/15512)](https://github.com/ydb-platform/ydb/issues/15512) | Pending |

#### [ISSUE-14639](https://github.com/ydb-platform/ydb/issues/14639): Test suite: cs/write data
[![PROGRESS](https://img.shields.io/badge/PROGRESS-2%2F8:25%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14639-test-suite-cswrite-data)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14640 | You can write all kinds of data via bulk_upsert with all kinds of transport: arrow, BoxedValue |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14640)](https://github.com/ydb-platform/ydb/issues/14640) | Pending |
| #14642 | After a successful bulk_upsert write, the latest data values are visible | There can be multiple entries in bulk\_upsert with the same key. We expect that only the last record is written. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14642)](https://github.com/ydb-platform/ydb/issues/14642) | Pending |
| #14643 | If there are multiple identical keys within a single bulk_upsert data bundle, the last one is written | Test bulk upsert into the table with overlapping keys. Data are inserted by overlapping chunks<br> | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14643)](https://github.com/ydb-platform/ydb/issues/14643) | Pending |
| #14644 | Writing data to bulk_upsert with data integrity violation works correctly |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14644)](https://github.com/ydb-platform/ydb/issues/14644) | Pending |
| #14645 | When bulk_upsert is executed in parallel, the data is written to one table without errors | Test bulk upsert into the table with overlapping keys. Data are inserted by overlapping chunks | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14645)](https://github.com/ydb-platform/ydb/issues/14645) | Pending |
| #14646 | Writing milliards of rows via bulk_upsert is faster than a similar number of rows using INSERT INTO |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14646)](https://github.com/ydb-platform/ydb/issues/14646) | Pending |
| #14647 | If the cluster is stopped during bulk_upsert execution, an error is returned to the user |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14647)](https://github.com/ydb-platform/ydb/issues/14647) | Pending |
| #14648 | When inserting a large amount of data ALTER TABLE ADD COLUMN, bulk_upsert should complete successfully |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14648)](https://github.com/ydb-platform/ydb/issues/14648) | Pending |

#### [ISSUE-14693](https://github.com/ydb-platform/ydb/issues/14693): Test Suite: Deletion by command
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F6:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14693-test-suite-deletion-by-command)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14694 | Data can be deleted by explicit row identifiers |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14694)](https://github.com/ydb-platform/ydb/issues/14694) | Pending |
| #14695 | Data can be deleted by key range, including the range of 99% of the data (on TPC-H 1000 class queries) |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14695)](https://github.com/ydb-platform/ydb/issues/14695) | Pending |
| #14696 | If the disks are full, the data can be cleared and the system restored to operation |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14696)](https://github.com/ydb-platform/ydb/issues/14696) | Pending |
| #14697 |  Data can be deleted by a query of the form DELETE FROM T WHERE ID IN (SELECT ID FROM T) |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14697)](https://github.com/ydb-platform/ydb/issues/14697) | Pending |
| #14698 | You can delete a record that does not exist |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14698)](https://github.com/ydb-platform/ydb/issues/14698) | Pending |
| #14699 | When data is deleted in a transaction, the data remains in place when the transaction is rolled back |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14699)](https://github.com/ydb-platform/ydb/issues/14699) | Pending |

### Other
#### [ISSUE-13952](https://github.com/ydb-platform/ydb/issues/13952): Test Suite: cs/introspection
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F1:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-13952-test-suite-csintrospection)

**Description**: статистики по таблицам для UI, доступность информации через .sys

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #13955 | TBD |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13955)](https://github.com/ydb-platform/ydb/issues/13955) | Pending |

#### [ISSUE-13956](https://github.com/ydb-platform/ydb/issues/13956): Test suite: cs/schema
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F1:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-13956-test-suite-csschema)

**Description**: взаимодействие со ским-шардом, создание/удаление таблиц/сторов, представление/оптимизиация хранения схем, актуализация данных

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #13957 | TBD |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13957)](https://github.com/ydb-platform/ydb/issues/13957) | Pending |

#### [ISSUE-13959](https://github.com/ydb-platform/ydb/issues/13959): Test suite: cs/indexes
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F1:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-13959-test-suite-csindexes)

**Description**: индексы/статистики

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #13960 | TBD |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13960)](https://github.com/ydb-platform/ydb/issues/13960) | Pending |

#### [ISSUE-14601](https://github.com/ydb-platform/ydb/issues/14601): Test Suite: Workload Manager
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F1:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14601-test-suite-workload-manager)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14602 | Test WM. Classifiers move queires to right resource pool |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14602)](https://github.com/ydb-platform/ydb/issues/14602) | Pending |

#### [ISSUE-14682](https://github.com/ydb-platform/ydb/issues/14682): Test Suite: CS/Pushdown предикатов
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F2:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14682-test-suite-cspushdown-предикатов)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14683 | При выполнении запросов происходит pushdown нужных типов данных-вычислений (проверяется, что pushdown был выполнен). В векторном варианте |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14683)](https://github.com/ydb-platform/ydb/issues/14683) | Pending |
| #14684 | When queries are executed, a pushdown of the desired data-calculus types is performed (check that the pushdown has been executed). In the scalar variant |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14684)](https://github.com/ydb-platform/ydb/issues/14684) | Pending |

### Federated Queries Support
#### [ISSUE-14700](https://github.com/ydb-platform/ydb/issues/14700): Test Suite: Federated Queries
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F2:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14700-test-suite-federated-queries)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14701 | federated source cross-requests |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14701)](https://github.com/ydb-platform/ydb/issues/14701) | Pending |
| #14702 | inserts from a federated source |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14702)](https://github.com/ydb-platform/ydb/issues/14702) | Pending |

## 

### TTL
#### [ISSUE-13526](https://github.com/ydb-platform/ydb/issues/13526): Test Suite: cs/tiering+ttl
[![PROGRESS](https://img.shields.io/badge/PROGRESS-5%2F12:41%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-13526-test-suite-cstieringttl)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #13468 | Test tiering. Functional. Data deleted by DELETE statement are deleted from S3 | При явном удалении данные с помощью DELETE связанные с ним данные удаляются из S3 (тест дожидается пропажи данных в S3)<br><br>Сценарий:<br>1 создаются нужные ресурсы:<br>1.1 таблица<br><br>ts Timestamp,<br>s String,<br>val Int64<br><br>1.2 два бакета в s3 (или очищаются ранее созданные)<br><br>aws \-\-profile=default \-\-endpoint\-url=http://127.0.0.1:12012 \-\-region=us\-east\-1 s3 mb s3://s3\_cold<br>aws \-\-profile=default \-\-endpoint\-url=http://127.0.0.1:12012 \-\-region=us\-east\-1 s3 mb s3://s3\_frozen<br><br>1.3 два EXTERNAL DATA SOURCE:<br>s3\_cold<br>s3\_frozen<br><br>CREATE OBJECT access\_key (TYPE SECRET) WITH (value="")<br>CREATE OBJECT secret\_key (TYPE SECRET) WITH (value="")<br><br>create external data source s3\_cold WITH(source\_type="ObjectStorage", LOCATION="http://127.0.0.1:12012", AUTH\_METHOD="AWS", AWS\_ACCESS\_KEY\_ID\_SECRET\_NAME="access\_key", AWS\_REGION="ru\-central1", AWS\_SECRET\_ACCESS\_KEY\_SECRET\_NAME="secret\_key")<br>повторить для s3\_frozen вместо s3\_cold<br><br>    Таблица заполняется данными равномерно распределёнными по ts в интервале 2010\-2030 год<br>    смотрим объём данный в .sys<br>    Настраивается тиринг в два тира:<br><br>ALTER TABLE mytable SET (<br>     TTL =<br>     Interval("P100D") TO EXTERNAL DATA SOURCE \`/Root/test/s3\_cold\`,<br>     Interval("P3000D") TO EXTERNAL DATA SOURCE \`/Root/test/s3\_frozen\`<br>     ON ts<br>     );<br><br><br>Контролируем по .sys что данные разъехались по тирам пропорционально длительности интервалов<br>Контролируем объём данных в бакетах s3<br><br>Выполняем команду DELETE FROM \`mytable\`<br>Проверяем по .sys, что данные удалились из всех тиров<br>Проверяем, что все данные удалились из обоих бакетов<br> | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13468)](https://github.com/ydb-platform/ydb/issues/13468) | Pending |
| #13467 | Test tiering. Functional. When configuring DELETE tier, data evaporates from S3 | Изменение настроек тиринга в части удаления данных из S3 приводит к полной очистке бакета <br><br>Сценарий:<br>1 создаются нужные ресурсы:<br>1.1 таблица<br>\`\`\`<br>ts Timestamp,<br>s String,<br>val Int64<br>\`\`\`<br><br>1.2 два бакета в s3 (или очищаются ранее созданные)<br>1.3 два EXTERNAL DATA SOURCE:<br>s3\_cold<br>s3\_frozen<br><br>2. Таблица заполняется данными равномерно распределёнными по ts в интервале 2010\-2030 год<br>смотрим объём данный в .sys<br>3. Настраивается тиринг в два тира:<br>\`\`\`<br>ALTER TABLE \`mytable\` SET (<br>    TTL =<br>        DateTime::IntervalFromDays(1000) TO EXTERNAL DATA SOURCE \`/Root/s3\_cold\`,<br>        DateTime::IntervalFromDays(3000) TO EXTERNAL DATA SOURCE \`/Root/s3\_frozen\`<br>    ON ts<br>);<br>\`\`\`<br>Контролируем по .sys что данные разъехались по тирам пропорционально длительности интервалов<br>Контролируем объём данных в бакетах s3<br><br>4. Меняем правила тиринга<br>\`\`\`<br>ALTER TABLE \`mytable\` SET (<br>    TTL =<br>        DateTime::IntervalFromDays(1000)<br>    ON ts<br>);<br><br>Проверяем по .sys, что данные удалились из всех тиров, кроме \_\_DEFAULT<br>Проверяем, что все данные удалились из обоих бакетов<br> <br> | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13467)](https://github.com/ydb-platform/ydb/issues/13467) | Pending |
| #13466 | Test tiering. Functional. Check data migration when altering tiering settings | Изменение настроек тиринга приводит к ожидаемому перемещению данных из одного тира в другой <br>Подробное описание:<br>1. Создать S3 и создать два \`EXTERNAL DATA SOURCE\` для двух разных бакета (\`bucket1\` и \`bucket2\`)<br>2. Создать табличку.<br>\`\`\`<br>CREATE TABLE \`table\_name\` (<br>    timestamp Timestamp,<br>    value Uint64,<br>    data String,<br>    PRIMARY KEY(timestamp)<br>)<br>WITH (<br>    STORE = COLUMN,<br>);<br>\`\`\`<br>3. Заполнить данные в табличку.<br>4. Изменить \`TTL\`, так чтобы данные вытиснялись во второй бакет<br>\`\`\`<br>ALTER TABLE \`table\_name\` SET (<br>    TTL = INTERVAL("PT2M") TO EXTERNAL DATA SOURCE \`/Root/bucket1\`,<br>    ON timestamp<br>);<br>\`\`\`<br>5. Дождаться вытиснения и проверить размер первого бакета (размер 1\-го бакета должен совпадать с количеством вставленных данных, а 2\-й бакет должен быть пустым)<br>6. Изменить \`TTL\`, так чтобы данные вытиснялись во второй бакет.<br>\`\`\`<br>ALTER TABLE \`table\_name\` SET (<br>    TTL = INTERVAL("PT5M") TO EXTERNAL DATA SOURCE \`/Root/bucket2\`,<br>    ON timestamp<br>);<br>\`\`\`<br>7. Дождаться вытиснения и проверить размеры бакетов (размер 2\-го бакета должен совпадать с количеством вставленных данных, а 1\-й бакет должен быть пустым)<br>8. Изменить \`TTL\`, так чтобы данные вернулись в \`BlobStorage\`<br>\`\`\`<br>ALTER TABLE \`table\_name\` SET (<br>    TTL = INTERVAL("P10000D") TO EXTERNAL DATA SOURCE \`/Root/bucket1\`,<br>    ON timestamp<br>);<br>\`\`\`<br>9. Дождаться возвращения данных в \`BlobStorage\` и проверить размеры бакетов (размер 1\-го и 2\-го бакета должны быть пустым) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13466)](https://github.com/ydb-platform/ydb/issues/13466) | Pending |
| #13465 | Test tiering. Functional. Check data correctness | Выполняется большое число записи, удаления, модификации большого числа данных с тем, чтобы все данные были вытеснены. Сравниваются прочитанные данные и ожидаемые<br>TBD Подробное описание теста | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13465)](https://github.com/ydb-platform/ydb/issues/13465) | Pending |
| #13542 | Test tiering. Functional. Check data availability and correctness while changing ttl settings | Таблица наполняется данными, настройки тиринга меняются постоянно, проверяется, что все время считываются корректные данные приоритет 1<br>3 тира и данные записаны в соответсвии с паттерном логов, т.е. каждая запись содержит строки локализованные по времени | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13542)](https://github.com/ydb-platform/ydb/issues/13542) | Pending |
| #13543 | Test. sys reflects data distribution across tiers while modifying data | Выполняется большое число точечных модификаций данных. В sysview отражается статус отставания вытеснения данных | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13543)](https://github.com/ydb-platform/ydb/issues/13543) | Pending |
| #13544 | Test tiering. Stress. Ustable network connection | Протетсировать работу тиринга с нестабильным доступом до s3 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13544)](https://github.com/ydb-platform/ydb/issues/13544) | Pending |
| #13545 | Test tiering. Stability. Temporary unavailable s3 | Временно потеряно соединение с s3. Ожидаемое поведение \- после возобновления связи (через какое время?) перенос данных возобновляется. На время ошибок в sysview отражается статус ошибки | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13545)](https://github.com/ydb-platform/ydb/issues/13545) | Pending |
| #13546 | Test tiering. Stability. Writing when blobstorage is full | Постоянно потеряно соединение с S3, места на диске не хватает. Ожидаемое поведение \- сообщение об ошибке записи пользователю. . На время ошибок в sysview отражается статус ошибки | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13546)](https://github.com/ydb-platform/ydb/issues/13546) | Pending |
| #13619 | Test tiering. Add column works for offloaded data | Во время вытеснения данных в S3 производится смена схемы таблицы, добавляются новые поля. Ожидаемое поведение \- система на всей глубине хранения отображает новые поля | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13619)](https://github.com/ydb-platform/ydb/issues/13619) | Pending |
| #13620 | Test teiring. Drop Column works well for offloaded data | Во время вытеснения данных в S3 производится смена схемы таблицы, удаляются существующие not null поля. Ожидаемое поведение \- система на всей глубине хранения выполняет запросы  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13620)](https://github.com/ydb-platform/ydb/issues/13620) | Pending |
| #13621 | Test tiering. Alter column works well for offloaded data |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13621)](https://github.com/ydb-platform/ydb/issues/13621) | Pending |

#### [ISSUE-14685](https://github.com/ydb-platform/ydb/issues/14685): Test Suite: CS/TTL deletion
[![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F7:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-14685-test-suite-csttl-deletion)

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #14686 | Data is deleted according to the specified TTL |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14686)](https://github.com/ydb-platform/ydb/issues/14686) | Pending |
| #14687 | Data is deleted starting with the oldest records |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14687)](https://github.com/ydb-platform/ydb/issues/14687) | Pending |
| #14688 | If a valid TTL is specified, the data is deleted |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14688)](https://github.com/ydb-platform/ydb/issues/14688) | Pending |
| #14689 | The invalid TTL is handled correctly |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14689)](https://github.com/ydb-platform/ydb/issues/14689) | Pending |
| #14690 | You can change the previously specified TTL, deletion will occur according to the new TTL |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14690)](https://github.com/ydb-platform/ydb/issues/14690) | Pending |
| #14691 | Columns with types Timestamp, Datetime, Date can be specified as TTL (only the first column of PK) |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14691)](https://github.com/ydb-platform/ydb/issues/14691) | Pending |
| #14692 | TTL deletes data at a sufficient rate (must exceed the insertion rate) |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14692)](https://github.com/ydb-platform/ydb/issues/14692) | Pending |

### Compression
#### [ISSUE-13626](https://github.com/ydb-platform/ydb/issues/13626): Test Suite: cs/compression
[![PROGRESS](https://img.shields.io/badge/PROGRESS-2%2F11:18%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)](./summary.md#issue-13626-test-suite-cscompression)

**Description**: Сжатие (в широком смысле, напр., dictionary encoding), sparse, column_family

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| #13627 | Test cs column family. Create multiple/maximum column family for one table | Создать несколько/максимальное количество \`Column Family\` в одной таблице. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13627)](https://github.com/ydb-platform/ydb/issues/13627) | Pending |
| #13640 | Test cs column family. Check all supported compression | Проверить включения/измнения всех алгоримов сжатия и проверить размеры данных через sys после включения сжатия через \`Column Family\` | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13640)](https://github.com/ydb-platform/ydb/issues/13640) | Pending |
| #13642 | Test cs column family. Check all supported compression with S3 | Проверить включения/измнения всех алгоримов сжатия c вытеснением в S3 и проверять, что сжатие применялось | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13642)](https://github.com/ydb-platform/ydb/issues/13642) | Pending |
| #13643 | Test cs column family. Check availability of all data after alter family | При записи данных в таблицу задавать другие \`Column family\` у столбца с контролем данных | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13643)](https://github.com/ydb-platform/ydb/issues/13643) | Pending |
| #13644 | Test cs column family. Check availability of all data after alter compression in Column family | При записи данных в таблицу, изменять свойства сжатия у \`Column Family\` и проверять доступность старых и новых данных в столбцах, которые принадлежат измененному \`Column Family\`. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13644)](https://github.com/ydb-platform/ydb/issues/13644) | Pending |
| #13645 | Test cs column family. Check supported data type for column family | Проверить работоспособность column family на столбцах со всеми типами данных (Лучше сделать, чтобы все существующие тесты работали со всеми поддерживаемыми типами данных) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13645)](https://github.com/ydb-platform/ydb/issues/13645) | Pending |
| #13646 | Test cs column family. Check create table with PK column from columns in different column families | Проверяем, что можно создать первичный ключ из колонок разных column family  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13646)](https://github.com/ydb-platform/ydb/issues/13646) | Pending |
| #13647 | Test cs column family. Test column with NULL in column family | Проверить работоспоность column family с NULL столбцами | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13647)](https://github.com/ydb-platform/ydb/issues/13647) | Pending |
| #13648 | Test cs column family. Column family with data types: text, string, json, jsondocument | Проверяем, что поддерживаются типы данных максимальной длины (text, string, json, jsondocument), условная запись 1 MB данных в ячейку | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13648)](https://github.com/ydb-platform/ydb/issues/13648) | Pending |
| #13650 | Test cs column family. Zip-bomba | Выполняем запись в колонку 1 млн строк одной длинной, но одинаковой строки (в пределе из одного символа) (zip\-бомба), проверяем, что запись и чтение выполняется | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13650)](https://github.com/ydb-platform/ydb/issues/13650) | Pending |
| #13651 | Test cs column family. Write highly randomized data | Выполняем запись сильнорандомизированных данных (после сжатия размер должен вырасти), проверяем, что запись и чтение выполняется  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13651)](https://github.com/ydb-platform/ydb/issues/13651) | Pending |

## Non-functional Requirements

### Performance
#### REQ-PERF-001: Ensure the system handles aggregate functions efficiently across various data sizes.
**Description**: Verify that aggregate functions maintain performance standards at increasing data volumes, ensuring response times are within acceptable limits.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-001-1.1 | COUNT Function Performance - 1GB | Validate performance with a dataset of 1GB. |  | Pending |
| REQ-PERF-001-1.2 | COUNT Function Performance - 10GB | Validate performance with a dataset of 10GB. |  | Pending |
| REQ-PERF-001-1.3 | COUNT Function Performance - 100GB | Validate performance with a dataset of 100GB. |  | Pending |
| REQ-PERF-001-1.4 | COUNT Function Performance - 1TB | Validate performance with a dataset of 1TB. |  | Pending |
| REQ-PERF-001-1.5 | COUNT Function Performance - 10TB | Validate performance with a dataset of 10TB. |  | Pending |

#### REQ-PERF-002: Ensure system can efficiently compute distinct counts at scale.
**Description**: Evaluate the ability to perform COUNT(DISTINCT) operations with acceptable overhead across increasing data volumes.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-002-2.1 | COUNT DISTINCT Performance - 1GB | Measure distinct count efficiency at 1GB. |  | Pending |
| REQ-PERF-002-2.2 | COUNT DISTINCT Performance - 10GB | Measure distinct count efficiency at 10GB. |  | Pending |

#### REQ-PERF-003: Validate efficiency of SUM operations over large datasets.
**Description**: Ensure SUM functions execute with optimal performance metrics at different data scales.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-003-3.1 | SUM Function Performance - 1GB | Validate SUM operation efficiency with 1GB of data. |  | Pending |
| REQ-PERF-003-3.2 | SUM Function Performance - 10GB | Validate SUM operation efficiency with 10GB of data. |  | Pending |

#### REQ-PERF-004: Ensure system maintains average calculation efficiency.
**Description**: Verify AVG functions sustain performance as data sizes increase.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-004-4.1 | AVG Function Performance - 1GB | Performance metrics for AVG operation on 1GB of data. |  | Pending |

#### REQ-PERF-005: Efficient computation of MIN/MAX operations.
**Description**: Confirm that minimum and maximum functions perform within the expected time frames across various datasets.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-005-5.1 | MIN/MAX Performance - 1GB | Validate performance of MIN/MAX operations with 1GB. |  | Pending |

#### REQ-PERF-006: TPC-H benchmark testing on scalability.
**Description**: Evaluate system performance using TPC-H benchmark tests at different dataset volumes.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-006-6.1 | TPC-H Performance - 10GB | Validate TPC\-H benchmark performance with 10GB. |  | Pending |

#### REQ-PERF-007: ClickBench benchmark to test efficiency under different conditions.
**Description**: Assess system capabilities using ClickBench, targeting different data sizes.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-007-7.1 | ClickBench Performance - 1GB | Evaluate with ClickBench on 1GB of data. |  | Pending |

#### REQ-PERF-001: Ensure system operations meet expected performance benchmarks.
**Description**: Maintain performance standards across operations, ensuring response times and throughput align with requirements.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-PERF-001-7.1 | Bulk Upsert Performance | Measure and analyze the efficiency of bulk upsert operations. |  | Pending |
| REQ-PERF-001-7.2 | DELETE Operation Speed | Validate performance expectations during DELETE operations. |  | Pending |
| REQ-PERF-001-7.3 | TTL Deletion Speed | Measure and confirm deletion speed exceeds insertion speed. |  | Pending |

### Disk Space Management
#### REQ-DISK-001: Effectively manage disk space to avoid system failures.
**Description**: Utilize efficient disk space management strategies, particularly under scenarios with constrained disk resources.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-DISK-001-8.1 | Disk Space Recovery | Test system behavior and recovery strategies for low disk space conditions. |  | Pending |
| REQ-DISK-001-8.2 | Resilience to Disk Saturation | Ensure database resilience and behavior expectations under full disk conditions. |  | Pending |

### Documentation
#### REQ-DOC-001: Maintain and update comprehensive documentation.
**Description**: Ensure all functionalities are clearly documented, aiding user orientation and system understanding.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-DOC-001-9.1 | Documentation Completeness | Review documentation for completeness and clarity. |  | Pending |

### Load Testing
#### REQ-LOAD-001: Validate system performance under workload log scenarios.
**Description**: Ensure the system can handle select queries and bulk upsert operations efficiently, with specified data rates.

Issues:
- 14493:  Nodes crush under write+select: https

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-LOAD-001-1.1 | Bulk Upsert - 25MB/s | Measure bulk upsert performance at 25MB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14493)](https://github.com/ydb-platform/ydb/issues/14493) | Pending |
| REQ-LOAD-001-1.2 | Bulk Upsert - 1GB/s | Measure bulk upsert performance at 1GB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14493)](https://github.com/ydb-platform/ydb/issues/14493) | Pending |
| REQ-LOAD-001-1.3 | SELECT | Measure bulk upsert performance at 1GB/s. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14493)](https://github.com/ydb-platform/ydb/issues/14493) | Pending |

#### REQ-LOAD-002: Evaluate system performance under simple queue scenarios.
**Description**: Test the ability of the system to efficiently process tasks in simple queue scenarios.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-LOAD-002-2.1 | Simple Queue Load | Validate performance under simple queue load conditions. |  | Pending |

#### REQ-LOAD-003: Assess system capabilities with an OLAP workload.
**Description**: Verify OLAP queries perform efficiently under varied load.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-LOAD-003-3.1 | OLAP Workload Performance | Test OLAP workload performance metrics. |  | Pending |

### Stability
#### REQ-STAB-001: Ensure system stability under load scenarios with Nemesis.
**Description**: Validate stability by running load tests with a one-minute reload interval using Nemesis.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-STAB-001-4.1 | Nemesis Stability Test | Measure stability performance during Nemesis events. |  | Pending |

### Compatibility
#### REQ-COMP-001: Validate compatibility during system upgrades.
**Description**: Ensure seamless transition and compatibility when upgrading the system from one version to another.

| Case ID | Name | Description | Issues |  Status |
|---------|------|-------------|--------|:--------|
| REQ-COMP-001-5.1 | Upgrade 24-3 to 24-4 | Test upgrade compatibility from version 24\-3 to 24\-4. |  | Pending |
| REQ-COMP-001-5.2 | Upgrade 24-4 to 25-1 | Test upgrade compatibility from version 24\-4 to 25\-1. |  | Pending |

