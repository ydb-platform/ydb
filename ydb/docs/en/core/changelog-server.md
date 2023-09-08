# {{ ydb-short-name }} Server changelog

## Version 23.2 {#23-2}

Release date: August 14, 2023.

**Functionality:**

* **_(Experimental)_** Implemented visibility of own changes. With this feature enabled you can read changed values from the current transaction, which has not been committed yet. This functionality also allows multiple modifying operations in one transaction on a table with secondary indexes. To enable this feature add `enable_kqp_immediate_effects: true` under `table_service_config` section into [configuration file](deploy/configuration/config.md).
* **_(Experimental)_** Implemented read iterators. This feature allows to separate reads and computations. Read iterators allow datashards to increase read queries throughput. To enable this feature add `enable_kqp_data_query_source_read: true` under `table_service_config` section into [configuration file](deploy/configuration/config.md).

**Embedded UI:**

* Navigation improvements:
  * Diagnostics and Development mode switches are moved to the left panel.
  * Every page has breadcrumbs.
  * Storage groups and nodes info are moved from left buttons to tabs on the database page.
* Query history and saved queries are moved to tabs over the text editor area in query editor.
* Info tab for scheme objects displays parameters using terms from `CREATE` or `ALTER` statements.
* Added [column tables](concepts/datamodel/table.md#column-tables) support.

**Performance:**

* For scan queries, you can now effectively search for individual rows using a primary key or secondary indexes. This can bring you a substantial performance gain in many cases. Similarly to regular queries, you need to explicitly specify its name in the query text using the `VIEW` keyword to use a secondary index.

* **_(Experimental)_** Added an option to give control of the system tablets of the database (SchemeShard, Coordinators, Mediators, SysViewProcessor) to its own Hive instead of the root Hive, and do so immediately upon creating a new database. Without this flag, the system tablets of the new database are created in the root Hive, which can negatively impact its load. Enabling this flag makes databases completely isolated in terms of load, that may be particularly relevant for installations, consisting from a roughly hundred or more databases. To enable this feature add `alter_database_create_hive_first: true` under `feature_flags` section into [configuration file](deploy/configuration/config.md).

**Bug fixes:**

* Fixed a bug in the autoconfiguration of the actor system, resulting in all the load being placed on the system pool.
* Fixed a bug that caused full scanning when searching by prefix of the primary key using `LIKE`.
* Fixed bugs when interacting with datashard followers.
* Fixed bugs when working with memory in column tables.
* Fixed a bug in processing conditions for immediate transactions.
* Fixed a bug in the operation of iterator-based reads on datasharrd followers.
* Fixed a bug that caused cascading reinstallation of data delivery sessions to asynchronous indexes.
* Fixed bugs in the optimizer for scanning queries.
* Fixed a bug in the incorrect calculation of storage consumption by Hive after expanding the database.
* Fixed a bug that caused operations to hang on non-existent iterators.
* Fixed bugs when reading a range on a `NOT NULL` column.
* Fixed a bug in the replication of VDisks.
* Fixed a bug in the work of the `run_interval` option in TTL.

## Version 23.1 {#23-1}

Release date: May 5, 2023. To update to version 23.1, select the [Downloads](downloads/index.md#ydb-server) section.

**Functionality:**

* Added [initial table scan](concepts/cdc.md#initial-scan) when creating a CDC changefeed. Now, you can export all the data existing at the time of changefeed creation.
* Added [atomic index replacement](best_practices/secondary_indexes.md#atomic-index-replacement). Now, you can atomically replace one pre-defined index with another. This operation is absolutely transparent for your application. Indexes are replaced seamlessly, with no downtime.
* Added the [audit log](cluster/audit-log.md): Event stream including data about all the operations on {{ ydb-short-name }} objects.

**Performance:**

* Improved formats of data exchanged between query stages. As a result, we accelerated SELECTs by 10% on parameterized queries and by up to 30% on write operations.
* Added [autoconfiguring](deploy/configuration/config.md#autoconfig) for the actor system pools based on the workload against them. This improves performance through more effective CPU sharing.
* Optimized the predicate logic: Processing of parameterized OR or IN constraints is automatically delegated to DataShard.
* (Experimental) For scan queries, you can now effectively search for individual rows using a primary key or secondary indexes. This can bring you a substantial gain in performance in many cases. Similarly to regular queries, to use a secondary index, you need to explicitly specify its name in the query text using the `VIEW` keyword.
* The query's computational graph is now cached at query runtime, reducing the CPU resources needed to build the graph.

**Bug fixes:**

* Fixed bugs in the distributed data warehouse implementation. We strongly recommend all our users to upgrade to the latest version.
* Fixed the error that occurred on building an index on NOT NULL columns.
* Fixed statistics calculation with MVCC enabled.
* Fixed errors with backups.
* Fixed the race condition that occurred at splitting and deleting a table with SDC.

## Version 22.5 {#22-5}

Release date: March 7, 2023. To update to version **22.5**, select the [Downloads](downloads/index.md#ydb-server) section.

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

Release date: October 12, 2022. To update to version **22.4**, select the [Downloads](downloads/index.md#ydb-server) section.

**What's new:**

* {{ ydb-short-name }} Topics and Change Data Capture (CDC):
   * Introduced the new Topic API. {{ ydb-short-name }} [Topic](concepts/topic.md) is an entity for storing unstructured messages and delivering them to various subscribers.
   * Added support for the Topic API to the [{{ ydb-short-name }} CLI](reference/ydb-cli/topic-overview.md) and [SDK](reference/ydb-sdk/topic.md). The Topic API provides methods for message streaming writes and reads as well as topic management.
   * Added the ability to [capture table updates](concepts/cdc.md) and send change messages to a topic.

* SDK:
   * Added the ability to handle topics in the {{ ydb-short-name }} SDK.
   * Added official support for the database/sql driver for working with {{ ydb-short-name }} in Golang.

* Embedded UI:
   * The CDC changefeed and the secondary indexes are now displayed in the database schema hierarchy as separate objects.
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
