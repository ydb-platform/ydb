# {{ ydb-short-name }} Server changelog

## Version 22.5 {#22-5}

Released on 07/03/2023. To update to version **22.5**, select the [Downloads](downloads/index.md#ydb-server) section.

**What's new:**

* Added [changefeed configuration parameters](yql/reference/syntax/alter_table#changefeed-options) to transfer additional information about changes to a topic.
* You can now [rename tables](concepts/datamodel/table.md#rename) that have TTL enabled.
* You can now [manage the record retention period](concepts/cdc#retention-period).

**Bug fixes and improvements:**

* Fixed an error inserting 0 rows with a BulkUpsert.
* Fixed an error importing Date/DateTime columns from CSV.
* Fixed an error importing CSV data with line breaks.
* Fixed an error importing CSV data with NULL values.
* Improved Query Processing performance (by replacing WorkerActor with SessionActor).
* DataShard compaction now starts immediately after a split or merge.

## Version 22.4 {#22-4}

Released on 12/10/2022. To update to version **22.4**, select the [Downloads](downloads/index.md#ydb-server) section.

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
