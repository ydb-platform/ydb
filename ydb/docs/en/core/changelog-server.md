# {{ ydb-short-name }} Server changelog

## Version 23.4 {#23-4}

Release date: May 14, 2024.

**Performance:**

* [Fixed](https://github.com/ydb-platform/ydb/pull/3638) an issue of increased CPU consumption by a topic actor `PERSQUEUE_PARTITION_ACTOR`.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2083) resource usage by SchemeBoard replicas. The greatest effect is noticeable when modifying the metadata of tables with a large number of partitions.

**Bug fixes:**

* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2169) of possible partial commit of accumulated changes when using persistent distributed transactions. This error occurs in an extremely rare combination of events, including restarting tablets that service the table partitions involved in the transaction.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/3165) involving a race condition between the table merge and garbage collection processes, which could result in garbage collection ending with an invariant violation error, leading to an abnormal termination of the `ydbd` server process.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2696) in Blob Storage, where information about changes to the composition of a storage group might not be received in a timely manner by individual cluster nodes. As a result, reads and writes of data stored in the affected group could become blocked in rare cases, requiring manual intervention.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/3002) in Blob Storage, where data storage nodes might not start despite the correct configuration. The error occurred on systems with the experimental "blob depot" feature explicitly enabled (this feature is disabled by default).
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2475) that sometimes occurred when writing to a topic with an empty `producer_id` with turned off deduplication. It could lead to abnormal termination of the `ydbd` server process.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2651) that caused the `ydbd` process to crash due to an incorrect session state when writing to a topic.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/3587) in displaying the metric of number of partitions in a topic, where it previously displayed an incorrect value.
* [Fixed a bug](https://github.com/ydb-platform/ydb/pull/2126) causing memory leaks that appeared when copying topic data between clusters. These could cause `ydbd` server processes to terminate due to out-of-memory issues.

## Version 23.3 {#23-3}

Release date: October 12, 2023.

**Functionality:**

* Implemented visibility of own changes. With this feature enabled you can read changed values from the current transaction, which has not been committed yet. This functionality also allows multiple modifying operations in one transaction on a table with secondary indexes.
* Added support for [column tables](concepts/datamodel/table.md#column-tables). It is now possible to create analytical reports based on stored data in YDB with performance comparable to specialized analytical DBMS.
* Added support for Kafka API for topics. YDB topics can now be accessed via a Kafka-compatible API designed for migrating existing applications. Support for Kafka protocol version 3.4.0 is provided.
* Added the ability to [write to a topic without deduplication](concepts/topic.md#no-dedup). This is important in cases where message processing order is not critical.
* YQL has added the capabilities to [create](yql/reference/syntax/create_topic.md), [modify](yql/reference/syntax/alter_topic.md), and [delete](yql/reference/syntax/delete.md) topics.
* Added support of assigning and revoking access rights using the YQL `GRANT` and `REVOKE` commands.
* Added support of DML-operations logging in the audit log.
* **_(Experimental)_** When writing messages to a topic, it is now possible to pass metadata. To enable this functionality, add `enable_topic_message_meta: true` to the [configuration file](deploy/configuration/config.md).
* **_(Experimental)_** Added support for [reading from topics in a transaction](reference/ydb-sdk/topic.md#read-tx). It is now possible to read from topics and write to tables within a transaction, simplifying the data transfer scenario from a topic to a table. To enable this functionality, add `enable_topic_service_tx: true` to the [configuration file](deploy/configuration/config.md).
* **_(Experimental)_** Added support for PostgreSQL compatibility. This involves executing SQL queries in PostgreSQL dialect on the YDB infrastructure using the PostgreSQL network protocol. With this capability, familiar PostgreSQL tools such as psql and drivers (e.g., pq for Golang and psycopg2 for Python) can be used. Queries can be developed using the familiar PostgreSQL syntax and take advantage of YDB's benefits such as horizontal scalability and fault tolerance.
* **_(Experimental)_** Added support for federated queries. This enables retrieving information from various data sources without the need to move the data into YDB. Federated queries support interaction with ClickHouse and PostgreSQL databases, as well as S3 class data stores (Object Storage). YQL queries can be used to access these databases without duplicating data between systems.

**Embedded UI:**

* A new option `PostgreSQL` has been added to the query type selector settings, which is available when the `Enable additional query modes` parameter is enabled. Also, the query history now takes into account the syntax used when executing the query.
* The YQL query template for creating a table has been updated. Added a description of the available parameters.
* Now sorting and filtering for Storage and Nodes tables takes place on the server. To use this functionality, you need to enable the parameter `Offload tables filters and sorting to backend` in the experiments section.
* Buttons for creating, changing and deleting [topics](concepts/topic.md) have been added to the context menu.
* Added sorting by criticality for all issues in the tree in `Healthcheck`.

**Performance:**

* Implemented read iterators. This feature allows to separate reads and computations. Read iterators allow datashards to increase read queries throughput.
* The performance of writing to YDB topics has been optimized.
* Improved tablet balancing during node overload.

**Bug fixes:**

* Fixed an error regarding potential blocking of reading iterators of snapshots, of which the coordinators were unaware.
* Memory leak when closing the connection in Kafka proxy has been fixed.
* Fixed an issue where snapshots taken through reading iterators may fail to recover on restarts.
* Fixed an issue with an incorrect residual predicate for the `IS NULL` condition on a column.
* Fixed an occurring verification error: `VERIFY failed: SendResult(): requirement ChunksLimiter.Take(sendBytes) failed`.
* Fixed `ALTER TABLE` for TTL on column-based tables.
* Implemented a `FeatureFlag` that allows enabling/disabling work with `CS` and `DS`.
* Fixed a 50ms time difference between coordinator time in 23-2 and 23-3.
* Fixed an error where the storage endpoint was returning extra groups when the `viewer backend` had the `node_id` parameter in the request.
* Added a usage filter to the `/storage` endpoint in the `viewer backend`.
* Fixed an issue in Storage v2 where an incorrect number was returned in the `Degraded field`.
* Fixed an issue with cancelling subscriptions from sessions during tablet restarts.
* Fixed an error where `healthcheck alerts` for storage were flickering during rolling restarts when going through a load balancer.
* Updated `CPU usage metrics` in YDB.
* Fixed an issue where `NULL` was being ignored when specifying `NOT NULL` in the table schema.
* Implemented logging of `DDL` operations in the common log.
* Implemented restriction for the YDB table attribute `add/drop` command to only work with tables and not with any other objects.
* Disabled `CloseOnIdle` for interconnect.
* Fixed the doubling of read speed in the UI.
* Fixed an issue where data could be lost on block-4-2.
* Added a check for topic name validity.
* Fixed a possible deadlock in the actor system.
* Fixed the `KqpScanArrowInChanels::AllTypesColumns` test.
* Fixed the `KqpScan::SqlInParameter` test.
* Fixed parallelism issues for OLAP queries.
* Fixed the insertion of `ClickBench` parquet files.
* Added a missing call to `CheckChangesQueueOverflow` in the general `CheckDataTxReject`.
* Fixed an error that returned an empty status in `ReadRows` API calls.
* Fixed incorrect retry behavior in the final stage of export.
* Fixed an issue with infinite quota for the number of records in a `CDC topic`.
* Fixed the import error of `string` and `parquet` columns into an `OLAP string column`.
* Fixed a crash in `KqpOlapTypes.Timestamp` under `tsan`.
* Fixed a `viewer backend` crash when attempting to execute a query against the database due to version incompatibility.
* Fixed an error where the viewer did not return a response from the `healthcheck` due to a timeout.
* Fixed an error where incorrect `ExpectedSerial` values could be saved in `Pdisks`.
* Fixed an error where database nodes were crashing due to segfault in the S3 actor.
* Fixed a race condition in `ThreadSanitizer: data race KqpService::ToDictCache-UseCache`.
* Fixed a race condition in `GetNextReadId`.
* Fixed an issue with an inflated result in `SELECT COUNT(*)` immediately after import.
* Fixed an error where `TEvScan` could return an empty dataset in the case of shard splitting.
* Added a separate `issue/error` code in case of available space exhaustion.
* Fixed a `GRPC_LIBRARY Assertion` failed error.
* Fixed an error where scanning queries on secondary indexes returned an empty result.
* Fixed validation of `CommitOffset` in `TopicAPI`.
* Reduced shared cache consumption when approaching OOM.
* Merged scheduler logic from data executer and scan executer into one class.
* Added discovery and `proxy` handlers to the query execution process in the `viewer backend`.
* Fixed an error where the `/cluster` endpoint returned the root domain name, such as `/ru`, in the `viewer backend`.
* Implemented a seamless table update scheme for `QueryService`.
* Fixed an issue where `DELETE` returned data and did not delete it.
* Fixed an error in `DELETE ON` operation in query service.
* Fixed an unexpected batching disablement in `default` schema settings.
* Fixed a triggering check `VERIFY failed: MoveUserTable(): requirement move.ReMapIndexesSize() == newTableInfo->Indexes.size()`.
* Increased the `default` timeout for `grpc-streaming`.
* Excluded unused messages and methods from `QueryService`.
* Added sorting by `Rack` in /nodes in the `viewer backend`.
* Fixed an error where sorting queries returned an error in descending order.
* Improved interaction between `KQP` and `NodeWhiteboard`.
* Removed support for old parameter formats.
* Fixed an error where `DefineBox` was not being applied to disks with a static group.
* Fixed a `SIGSEGV` error in the dinnode during `CSV` import via `YDB CLI`.
* Fixed an error that caused a crash when processing `NGRpcService::TRefreshTokenImpl`.
* Implemented a `gossip protocol` for exchanging cluster resource information.
* Fixed an error in `DeserializeValuePickleV1(): requirement data.GetTransportVersion() == (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0 failed`.
* Implemented `auto-increment` columns.
* Use `UNAVAILABLE` status instead of `GENERIC_ERROR` when shard identification fails.
* Added support for rope payload in `TEvVGet`.
* Added ignoring of deprecated events.
* Fixed a crash of write sessions on an invalid topic name.
* Fixed an error in `CheckExpected(): requirement newConstr failed, message: Rewrite error, missing Distinct((id)) constraint in node FlatMap`.
* Enabled `self-heal` by default.

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
* Added [atomic index replacement](dev/secondary-indexes.md#atomic-index-replacement). Now, you can atomically replace one pre-defined index with another. This operation is absolutely transparent for your application. Indexes are replaced seamlessly, with no downtime.
* Added the [audit log](security/audit-log.md): Event stream including data about all the operations on {{ ydb-short-name }} objects.

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
