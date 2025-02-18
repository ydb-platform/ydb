# Traceability Matrix

## Functional Requirements

### Bulk Upsert Data Insertion
#### REQ-BULK-001: The system should support data insertion using `bulk_upsert` across all data types and transports.
**Description**: Enable the insertion of all data types (e.g., Int32, Uint64, String) using transports like arrow and BoxedValue, ensuring data visibility and integrity.

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
#### [ISSUE-14668](https://github.com/ydb-platform/ydb/issues/14668): Test suite: CS/(INSERT INTO/UPSERT/REPLACE) support
![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F9:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
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
![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F2:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #14679 | LogBench - working with reads, aggregation, JSON types. The test is successful on a small amount of data |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14679)](https://github.com/ydb-platform/ydb/issues/14679) | Pending |
| #14681 | TPCH S100 is successful |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14681)](https://github.com/ydb-platform/ydb/issues/14681) | Pending |

#### [ISSUE-13527](https://github.com/ydb-platform/ydb/issues/13527): Test Suite: cs/read-write
![PROGRESS](https://img.shields.io/badge/PROGRESS-1%2F8:12%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #13528 | Test cs read-write. Check all column types work | Для каждого типа проверяем, что он может быть PK/не PK столбцом через create table + тривиальную операцию. Убеждаемся что операции чтения-записи проходят корректно | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13528)](https://github.com/ydb-platform/ydb/issues/13528) | Pending |
| #13529 | Test cs read-write. Quota exhaustion | Пишем в таблицу пока квота не закончится. Далее удаляем таблицу, убеждаемся что база работоспособна (путем манипулирования с таблицами) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13529)](https://github.com/ydb-platform/ydb/issues/13529) | Pending |
| #13530 | Test cs read-write. Log scenario (write in the end) | Делаем табличку с PK типа Timestamp | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13530)](https://github.com/ydb-platform/ydb/issues/13530) | Pending |
| #13531 | Test cs read-write. Log scenario (random writes) | То же самое что и #13530 , но необходимо писать в случайные точки, а не в конец  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13531)](https://github.com/ydb-platform/ydb/issues/13531) | Pending |
| #13532 | Test cs read-write. Log scenario (sparsed + many columns) | Включить sparse + то же самое что и #13530 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13532)](https://github.com/ydb-platform/ydb/issues/13532) | Pending |
| #13652 | Test cs writes. Written duplicates doesn't provoke disk exhaustion | Писать в таблицу много дубликатов первичного ключа. Отслеживать потреблямый объём стораджа. Проверять, что потребление стораджа растёт быстрее, чем кол-во строк в базе. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13652)](https://github.com/ydb-platform/ydb/issues/13652) | Pending |
| #13653 | Data deletion doesn't lead to disk exhaustion | Писать данные до того, как упрёмся в квоту. После этого удялять данные большими кусками.  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13653)](https://github.com/ydb-platform/ydb/issues/13653) | Pending |
| #13848 | Test CS RW load. Write, modify, read TBs | Проверка записи/модификации/чтения на больших объёмах данных (единицы-десятки. TB) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13848)](https://github.com/ydb-platform/ydb/issues/13848) | Pending |

#### [ISSUE-14639](https://github.com/ydb-platform/ydb/issues/14639): Test suite: cs/write data
![PROGRESS](https://img.shields.io/badge/PROGRESS-2%2F8:25%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #14640 | You can write all kinds of data via bulk_upsert with all kinds of transport: arrow, BoxedValue |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14640)](https://github.com/ydb-platform/ydb/issues/14640) | Pending |
| #14642 | After a successful bulk_upsert write, the latest data values are visible | There can be multiple entries in bulk_upsert with the same key. We expect that only the last record is written. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14642)](https://github.com/ydb-platform/ydb/issues/14642) | Pending |
| #14643 | If there are multiple identical keys within a single bulk_upsert data bundle, the last one is written | Test bulk upsert into the table with overlapping keys. Data are inserted by overlapping chunks | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14643)](https://github.com/ydb-platform/ydb/issues/14643) | Pending |
| #14644 | Writing data to bulk_upsert with data integrity violation works correctly |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14644)](https://github.com/ydb-platform/ydb/issues/14644) | Pending |
| #14645 | When bulk_upsert is executed in parallel, the data is written to one table without errors | Test bulk upsert into the table with overlapping keys. Data are inserted by overlapping chunks | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14645)](https://github.com/ydb-platform/ydb/issues/14645) | Pending |
| #14646 | Writing milliards of rows via bulk_upsert is faster than a similar number of rows using INSERT INTO |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14646)](https://github.com/ydb-platform/ydb/issues/14646) | Pending |
| #14647 | If the cluster is stopped during bulk_upsert execution, an error is returned to the user |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14647)](https://github.com/ydb-platform/ydb/issues/14647) | Pending |
| #14648 | When inserting a large amount of data ALTER TABLE ADD COLUMN, bulk_upsert should complete successfully |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14648)](https://github.com/ydb-platform/ydb/issues/14648) | Pending |

### Other
#### [ISSUE-13952](https://github.com/ydb-platform/ydb/issues/13952): Test Suite: cs/introspection
![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F1:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)

**Description**: статистики по таблицам для UI, доступность информации через .sys

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #13955 | TBD |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13955)](https://github.com/ydb-platform/ydb/issues/13955) | Pending |

#### [ISSUE-13956](https://github.com/ydb-platform/ydb/issues/13956): Test suite: cs/schema
![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F1:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)

**Description**: взаимодействие со ским-шардом, создание/удаление таблиц/сторов, представление/оптимизиация хранения схем, актуализация данных

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #13957 | TBD |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13957)](https://github.com/ydb-platform/ydb/issues/13957) | Pending |

#### [ISSUE-13959](https://github.com/ydb-platform/ydb/issues/13959): Test suite: cs/indexes
![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F1:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)

**Description**: индексы/статистики

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #13960 | TBD |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13960)](https://github.com/ydb-platform/ydb/issues/13960) | Pending |

#### [ISSUE-14601](https://github.com/ydb-platform/ydb/issues/14601): Test Suite: Workload Manager
![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F1:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #14602 | Test WM. Classifiers move queires to right resource pool |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/14602)](https://github.com/ydb-platform/ydb/issues/14602) | Pending |

### INSERT INTO, UPSERT, and REPLACE Operations
#### REQ-INS-001: Support INSERT INTO, UPSERT, and REPLACE for data modifications with expected behaviors.
**Description**: Ensure data can be inserted or updated with proper handling of duplicates and expected transactional behaviors.

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
#### REQ-READ-001: Provide robust and efficient data reading capabilities.
**Description**: Execute and validate operations concerning reading data, including aggregation and supported data types.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-READ-001-3.1 | LogBench Operations | Validate reading, aggregating, and processing JSON types through LogBench tests, focused on smaller datasets. |  | Pending |
| REQ-READ-001-3.2 | TPC-H S1 Execution | Confirm successful execution and performance of TPC-H S1 queries. |  | Pending |

## Federated Queries Support

#### REQ-FEDQ-001: Allow and manage federated query execution.
**Description**: Enable and validate operations involving federated query execution, taking advantage of diverse data sources.

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| REQ-FEDQ-001-10.1 | Cross-Source Federated Queries | Test execution of queries spanning multiple federated data sources. |  | Pending |
| REQ-FEDQ-001-10.2 | Federated Source Data Insertions | Validate data insertions deriving from federated sources. |  | Pending |

## 

### Compression
#### [ISSUE-13626](https://github.com/ydb-platform/ydb/issues/13626): Test Suite: cs/compression
![TO%20DO](https://img.shields.io/badge/TO%20DO-0%2F11:0%25-rgb(224%2C%20250%2C%20227%2C1)?style=for-the-badge&logo=database&labelColor=grey)

**Description**: Сжатие (в широком смысле, напр., dictionary encoding), sparse, column_family

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #13627 | Test cs column family. Create multiple/maximum column family for one table | Создать несколько/максимальное количество `Column Family` в одной таблице. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13627)](https://github.com/ydb-platform/ydb/issues/13627) | Pending |
| #13640 | Test cs column family. Check all supported compression | Проверить включения/измнения всех алгоримов сжатия и проверить размеры данных через sys после включения/измнения сжатия для `Column Family` | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13640)](https://github.com/ydb-platform/ydb/issues/13640) | Pending |
| #13642 | Test cs column family. Check all supported compression with S3 | Проверить включения/измнения всех алгоримов сжатия c вытеснением в S3 и проверять, что сжатие применялось | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13642)](https://github.com/ydb-platform/ydb/issues/13642) | Pending |
| #13643 | Test cs column family. Check availability of all data after alter family | При записи данных в таблицу, создавать новые или задавать существующие `Column family` у столбца с контролем данных | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13643)](https://github.com/ydb-platform/ydb/issues/13643) | Pending |
| #13644 | Test cs column family. Check availability of all data after alter compression in Column family | При записи данных в таблицу, изменять свойства сжатия у `Column Family` и проверять доступность старых и новых данных в столбцах, которые принадлежат измененному `Column Family`. | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13644)](https://github.com/ydb-platform/ydb/issues/13644) | Pending |
| #13645 | Test cs column family. Check supported data type for column family | Проверить работоспособность column family на столбцах со всеми типами данных (Лучше сделать, чтобы все существующие тесты работали со всеми поддерживаемыми типами данных) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13645)](https://github.com/ydb-platform/ydb/issues/13645) | Pending |
| #13646 | Test cs column family. Check create table with PK column from columns in different column families | Проверяем, что можно создать первичный ключ из колонок разных column family  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13646)](https://github.com/ydb-platform/ydb/issues/13646) | Pending |
| #13647 | Test cs column family. Test column with NULL in column family | Проверить работоспоность column family с NULL столбцами | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13647)](https://github.com/ydb-platform/ydb/issues/13647) | Pending |
| #13648 | Test cs column family. Column family with data types: text, string, json, jsondocument | Проверяем, что поддерживаются типы данных максимальной длины (text, string, json, jsondocument), условная запись 1 MB данных в ячейку | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13648)](https://github.com/ydb-platform/ydb/issues/13648) | Pending |
| #13650 | Test cs column family. Zip-bomba | Выполняем запись в колонку 1 млн строк одной длинной, но одинаковой строки (в пределе из одного символа) (zip-бомба), проверяем, что запись и чтение выполняется | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13650)](https://github.com/ydb-platform/ydb/issues/13650) | Pending |
| #13651 | Test cs column family. Write highly randomized data | Выполняем запись сильнорандомизированных данных (после сжатия размер должен вырасти), проверяем, что запись и чтение выполняется  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13651)](https://github.com/ydb-platform/ydb/issues/13651) | Pending |

### TTL
#### [ISSUE-13526](https://github.com/ydb-platform/ydb/issues/13526): Test Suite: cs/tiering+ttl
![PROGRESS](https://img.shields.io/badge/PROGRESS-4%2F12:33%25-rgb(254%2C%20248%2C%20202%2C1)?style=for-the-badge&logo=database&labelColor=grey)

| Case ID | Name | Description | Issues | Test Case Status |
|---------|------|-------------|--------|------------------|
| #13468 | Test tiering. Functional. Data deleted by DELETE statement are deleted from S3 | При явном удалении данные с помощью DELETE связанные с ним данные удаляются из S3 (тест дожидается пропажи данных в S3) | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13468)](https://github.com/ydb-platform/ydb/issues/13468) | Pending |
| #13467 | Test tiering. Functional. When configuring DELETE tier, data evaporates from S3 | Изменение настроек тиринга в части удаления данных из S3 приводит к полной очистке бакета  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13467)](https://github.com/ydb-platform/ydb/issues/13467) | Pending |
| #13466 | Test tiering. Functional. Check data migration when altering tiering settings | Изменение настроек тиринга приводит к ожидаемому перемещению данных из одного тира в другой  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13466)](https://github.com/ydb-platform/ydb/issues/13466) | Pending |
| #13465 | Test tiering. Functional. Check data correctness | Выполняется большое число записи, удаления, модификации большого числа данных с тем, чтобы все данные были вытеснены. Сравниваются прочитанные данные и ожидаемые | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13465)](https://github.com/ydb-platform/ydb/issues/13465) | Pending |
| #13542 | Test tiering. Functional. Check data availability and correctness while changing ttl settings | Таблица наполняется данными, настройки тиринга меняются постоянно, проверяется, что все время считываются корректные данные приоритет 1 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13542)](https://github.com/ydb-platform/ydb/issues/13542) | Pending |
| #13543 | Test. sys reflects data distribution across tiers while modifying data | Выполняется большое число точечных модификаций данных. В sysview отражается статус отставания вытеснения данных | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13543)](https://github.com/ydb-platform/ydb/issues/13543) | Pending |
| #13544 | Test tiering. Stress. Ustable network connection | Протетсировать работу тиринга с нестабильным доступом до s3 | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13544)](https://github.com/ydb-platform/ydb/issues/13544) | Pending |
| #13545 | Test tiering. Stability. Temporary unavailable s3 | Временно потеряно соединение с s3. Ожидаемое поведение - после возобновления связи (через какое время?) перенос данных возобновляется. На время ошибок в sysview отражается статус ошибки | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13545)](https://github.com/ydb-platform/ydb/issues/13545) | Pending |
| #13546 | Test tiering. Stability. Writing when blobstorage is full | Постоянно потеряно соединение с S3, места на диске не хватает. Ожидаемое поведение - сообщение об ошибке записи пользователю. . На время ошибок в sysview отражается статус ошибки | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13546)](https://github.com/ydb-platform/ydb/issues/13546) | Pending |
| #13619 | Test tiering. Add column works for offloaded data | Во время вытеснения данных в S3 производится смена схемы таблицы, добавляются новые поля. Ожидаемое поведение - система на всей глубине хранения отображает новые поля | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13619)](https://github.com/ydb-platform/ydb/issues/13619) | Pending |
| #13620 | Test teiring. Drop Column works well for offloaded data | Во время вытеснения данных в S3 производится смена схемы таблицы, удаляются существующие not null поля. Ожидаемое поведение - система на всей глубине хранения выполняет запросы  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13620)](https://github.com/ydb-platform/ydb/issues/13620) | Pending |
| #13621 | Test tiering. Alter column works well for offloaded data |  | [![GitHub issue/pull request detail](https://img.shields.io/github/issues/detail/state/ydb-platform/ydb/13621)](https://github.com/ydb-platform/ydb/issues/13621) | Pending |

