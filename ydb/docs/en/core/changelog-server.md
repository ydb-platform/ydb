# {{ ydb-short-name }} Server changelog

## Version 23.1 {#23-1}

Released on May 5, 2023. To update to version 23.1, select the [Downloads](downloads/index.md#ydb-server) section.

**Features:**

* [CDC initial scan](concepts/cdc.md#initial-scan). By default, records are emitted to the CDC changefeed only for table rows that were changed after the stream was created. With the optional initial scan of the table, CDC is now able to emit to the changefeed all the rows that existed at the time of the changefeed creation.
* [Atomic index replacement](best_practices/secondary_indexes.md#atomic-index-replacement). Allows to replace one index with another atomically and transparently to the application. The ability to replace the index with its improved version without any downtime is important for the many YDB applications, which normally embed secondary index names into YQL queries.
* [Audit Logs](cluster/audit-log.md). An event stream that includes data about all the attempts to change {{ ydb-short-name }} objects and permissions, both successfully or unsuccessfully. Audit Logs support is important for ensuring the secure operation of {{ ydb-short-name }} databases.

**Performance:**

* [Automatic configuration](deploy/configuration/config.md#autoconfig) of thread pool sizes depending on their load. This function allows {{ ydb-short-name }} to adapt to the changes in the workload, and improves the performance by better CPU resource sharing.
* Predicate extraction. More accurate predicate pushdown logic: support for OR and IN expressions with parameters for DataShard pushdown.
* Point lookups support for scan queries. Single-row lookups using primary key or secondary indexes are now supported in scan queries, leading to the improved performance in many cases. As with regular data queries, to actually use the secondary index lookups, index name has to be explicitly specified in the query text with the `VIEW` keyword.
* Significant improvements in data transfer formats between request execution stages. CPU consumption decreases and throughput increases, for example, for SELECTs  by about 10% on queries with parameters, and for write operations up to 30%.
* Caching of the calculation graph when executing queries. Reduces CPU consumption for its construction.

**Bug Fix:**

* Fixed a number of errors in the implementation of distributed data storage. We strongly recommend that all users upgrade to the current version.
* Fixed index building on not null columns.
* Fixed statistics calculation with MVCC enabled.
* Fixed backup issues.
* Fixed race while splitting and deleting table with CDC.

## Version 22.5 {#22-5}

Released on March 7, 2023. To update to version **22.5**, select the [Downloads](downloads/index.md#ydb-server) section.

**What's new:**

* Added [changefeed configuration parameters](yql/reference/syntax/alter_table.md#changefeed-options) to transfer additional information about changes to a topic.
* You can now [rename tables](concepts/datamodel/table.md#rename) that have TTL enabled.
* You can now [manage the record retention period](concepts/cdc.md#retention-period).

**Bug fixes and improvements:**

* Fixed an error inserting 0 rows with a BulkUpsert.
* Fixed an error importing Date/DateTime columns from CSV.
* Fixed an error importing CSV data with line breaks.
* Fixed an error importing CSV data with NULL values.
* Improved Query Processing performance (by replacing WorkerActor with SessionActor).
* DataShard compaction now starts immediately after a split or merge.

## Version 22.4 {#22-4}

Released on October 12, 2023. To update to version **22.4**, select the [Downloads](downloads/index.md#ydb-server) section.

**What's new:**

* {{ ydb-short-name }} Topics and Change Data Capture (CDC):
   * Introduced the new Topic API. {{ ydb-short-name }} [Topic](concepts/topic.md) is an entity for storing unstructured messages and delivering them to various subscribers.
   * Added support for the Topic API to the [{{ ydb-short-name }} CLI](reference/ydb-cli/topic-overview.md) and [SDK](reference/ydb-sdk/topic.md). The Topic API provides methods for message streaming writes and reads as well as topic management.
   * Added the ability to [capture table updates](concepts/cdc.md) and send change messages to a topic.

* SDK:
   * Added the ability to handle topics in the {{ ydb-short-name }} SDK.
   * Added official support for the database/sql driver for working with {{ ydb-short-name }} in Golang.

* Embedded UI:
   * The CDC change stream and the secondary indexes are now displayed in the database schema hierarchy as separate objects.
   * Improved the visualization of query explain plan graphics.
   * Problem storage groups have more visibility now.
   * Various improvements based on UX research.

* Query Processing:
   * Added Query Processor 2.0, a new subsystem to execute OLTP queries with significant improvements compared to the previous version.
   * Improved write performance by up to 60%, and by up to 10% for reads.
   * Added the ability to include a NOT NULL restriction for YDB primary keys when creating tables.
   * Added support for renaming a secondary index online without shutting the service down.
   * Improved the query explain view that now also includes fields for the physical operators.

* Core:
   * For read only transactions, added consistent snapshot support that does not conflict with write transactions.
   * Added BulkUpsert support for tables with asynchronous secondary indexes.
   * Added TTL support for tables with asynchronous secondary indexes.
   * Added compression support for data export to S3.
   * Added an audit log for DDL statements.
   * Added support for authentication with static credentials.
   * Added system tables for query performance troubleshooting.
