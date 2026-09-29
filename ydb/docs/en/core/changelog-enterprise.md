# Yandex Enterprise Database changelog

## Version 26.1 {#26-1}

### Version 26.1.1.ent.3 {#26-1-1-ent-3}

Release date: August 3, 2026.

The version includes all improvements contained in build {{ ydb-short-name }} 26.1.1.22, see [change description](./changelog-server.md#26-1-1-22). In addition, the version includes the [improvements specific to Yandex Enterprise Database](#26-1-1-ent-3-extras) listed below.

#### Improvements specific to Yandex Enterprise Database {#26-1-1-ent-3-extras}

The following changes are available in Yandex Enterprise Database in addition to the corresponding build {{ ydb-short-name }}:

* Added an optimization that allows filtering rows by index columns before querying the main table, reducing the number of accesses to the main table when executing certain types of queries.
* Implemented a set of fixes in index access (StreamIndexLookup) that eliminates the possibility of rare "hang" situations in running queries and reduces memory consumption when executing queries.
* Invalid views can now be restored from a backup. This allows restoring backups created from a database containing such views without additional administrator actions.
* Added support for mutual certificate authentication (mTLS) in [Kafka API](./reference/kafka-api/index.md).
* In system views `.sys/top_queries_*` and `.sys/query_sessions` added a column `TraceId` with the query trace ID.

## Version 25.4 {#25-4}

### Version 25.4.1.ent.2 {#25-4-1-ent-2}

Release date: June 17, 2026.

The version includes all improvements contained in build {{ ydb-short-name }} 25.4.1.15, see [change description](./changelog-server.md#25-4-1-15). In addition, all [additional fixes](#25-2-1-ent-13-extras) listed below for version 25.2.1.ent.13 are included.

## Version 25.3 {#25-3}

### Version 25.3.1.ent.3 {#25-3-1-ent-3}

Release date: June 11, 2026.

The version includes all improvements contained in build {{ ydb-short-name }} 25.3.1.27, see [change description](./changelog-server.md#25-3-1-27). In addition, all [additional fixes](#25-2-1-ent-13-extras) listed below for version 25.2.1.ent.13 are included.

## Version 25.2 {#25-2}

### Version 25.2.1.ent.13 {#25-2-1-ent-13}

Release date: June 11, 2026.

The version includes all improvements contained in build {{ ydb-short-name }} 25.2.1.26, see [change description](./changelog-server.md#25-2-1-26). In addition, the version includes a number of additional improvements ported from the current version 26.1.

#### Additional fixes {#25-2-1-ent-13-extras}

The following changes were ported from version 26.1 to the supported stable versions of Yandex Enterprise Database:

* Fixed a bug that violated the sort order specified in the query when accessing system tables.
* Fixed a bug in the internal state integrity check logic that, in rare cases, could lead to a single (not mass) restart of storage nodes.
* Added an optimization that allows filtering rows by index columns before querying the main table, reducing the number of accesses to the main table when executing certain types of queries.
* Implemented a set of fixes in index access (StreamIndexLookup) that eliminates the possibility of rare "hang" situations for running queries and reduces RAM consumption when executing queries.
* Added an optimization that reduces memory consumption when processing queries with the TopSort operation (`SELECT ... ORDER BY x LIMIT n`).
* Added support for materializing indexes during backup and restore.
* TLI (Transaction Locks Invalidated) error messages are now always accompanied by the identifier or path of the affected table.
* Query statistics provided through system tables `.sys/query_metrics_*` now include lock metrics.
* Invalid views can now be restored from a backup. This allows restoring backups created from a database containing such views without additional administrator actions.

### Version 25.2.1.ent.4 {#25-2-1-ent-4}

Release date: February 12, 2026.

#### Functionality

* [Analytical capabilities](./concepts/analytics/index.md) are available by default: [columnar tables](./concepts/datamodel/table.md#column-oriented-tables) can be created without enabling special flags, using LZ4 compression and hash partitioning. Supported operations include a wide range of DML (UPDATE, DELETE, UPSERT, INSERT INTO ... SELECT) and CREATE TABLE AS SELECT. Integration with dbt, Apache Airflow, Jupyter, Superset, and federated queries to S3 allow building end-to-end analytical pipelines in YDB.
* [Cost-based optimizer](./concepts/query_execution/optimizer.md) works by default for queries that use at least one columnar table, but can be forced on for other queries as well. The cost-based optimizer improves query performance by computing the optimal join order and type based on table statistics; supported [hints](./dev/optimization/hints.md) allow fine-tuning execution plans for complex analytical queries.
* Implemented [data transfer](./concepts/transfer.md) – an asynchronous mechanism for moving data from a topic to a table. [Creating](./yql/reference/syntax/create-transfer.md) a transfer instance, [modifying](./yql/reference/syntax/alter-transfer.md) it, and [deleting](./yql/reference/syntax/drop-transfer.md) it is done using YQL. For a quick start, use the [step-by-step guide with an example](./recipes/transfer/quickstart.md).
* Added [spilling](./concepts/query_execution/spilling.md), a memory management mechanism in which intermediate data generated by query execution and exceeding the available RAM of a node is temporarily offloaded to external storage. Spilling ensures that user queries requiring processing of large data volumes exceeding the node's available memory can be executed.
* Increased [maximum query execution time](./concepts/limits-ydb) from 30 minutes to 2 hours.
* Added support for Certificate Authority (CA) and [Yandex Cloud Identity and Access Management (IAM)](https://yandex.cloud/ru/docs/iam) authentication in [asynchronous replication](./yql/reference/syntax/create-async-replication.md).
* Enabled by default:

  * [vector index](./dev/vector-indexes.md) for approximate vector search;
  * support in [YDB Topics Kafka API](./reference/kafka-api/index.md) for [client-side read balancing](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb), [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html), and [transactions](https://www.confluent.io/blog/transactions-apache-kafka);
  * support for [automatic topic partitioning](./concepts/cdc.md#topic-partitions) in CDC for row-based tables;
  * support for automatic topic partitioning for asynchronous replication;
  * support for the parameterized [Decimal type](./yql/reference/types/primitive.md#numeric);
  * support for the [DateTime64 type](./yql/reference/types/primitive.md#datetime);
  * automatic deletion of temporary directories and tables when exporting to S3;
  * support for [change feed](./concepts/cdc.md) in backup and restore operations;
  * ability to [specify the number of replicas](./yql/reference/syntax/alter_table/indexes.md) for a secondary index;
  * system views with [history of overloaded partitions](./dev/system-views#top-overload-partitions).

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/24265) an error in [Workload Manager](./dev/resource-consumption-management.md) that could cause CPU consumption by column-oriented tables to exceed set limits.
* [Fixed](https://github.com/ydb-platform/ydb/pull/25112) a [problem](https://github.com/ydb-platform/ydb/issues/23858) that could cause deletion of a [tablet](./concepts/glossary.md#tablet) to hang
* [Fixed](https://github.com/ydb-platform/ydb/pull/25145) an [error](https://github.com/ydb-platform/ydb/issues/20866) that caused an error when changing a table's follower
* Fixed a number of errors related to [changefeed](./concepts/glossary.md#changefeed):
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25689) an [error](https://github.com/ydb-platform/ydb/issues/25524) that could cause importing a table with a Utf8 key and enabled changefeed to fail
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25453) an [error](https://github.com/ydb-platform/ydb/issues/25454) where importing a table without change feeds could fail due to incorrect search for changefeed files
* [Fixed](https://github.com/ydb-platform/ydb/pull/26069) an [error](https://github.com/ydb-platform/ydb/issues/25869) that could lead to failures during UPSERT operations in column-oriented tables
* [Fixed](https://github.com/ydb-platform/ydb/pull/26504) an [error](https://github.com/ydb-platform/ydb/issues/26225) that caused a crash due to accessing already freed memory
* [Fixed](https://github.com/ydb-platform/ydb/pull/26657) an [error](https://github.com/ydb-platform/ydb/issues/23122) with duplicates in unique secondary indexes
* [Fixed](https://github.com/ydb-platform/ydb/pull/26879) an [error](https://github.com/ydb-platform/ydb/issues/26565) of checksum mismatch when restoring compressed backups from S3
* [Fixed](https://github.com/ydb-platform/ydb/pull/27528) [an error](https://github.com/ydb-platform/ydb/issues/27193) that caused some TPC-H 1000 benchmark queries to fail with an error
* Fixed a number of issues related to cluster initialization:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25678) [an error](https://github.com/ydb-platform/ydb/issues/25023) that caused cluster initialization to hang when mandatory authorization was enabled
  * [Fixed](https://github.com/ydb-platform/ydb/pull/28886) [an issue](https://github.com/ydb-platform/ydb/issues/27228) that made it impossible to create new databases for several minutes immediately after cluster deployment
* [Fixed](https://github.com/ydb-platform/ydb/pull/28655) [an error](https://github.com/ydb-platform/ydb/issues/28510) that could cause a race condition and clients received an error `Could not find correct token validator` if recently issued tokens were used before the state was updated `LoginProvider`

## Version 25.1 {#25-1}

### Version 25.1.4.ent.8 {#25-1-4-ent-8}

Release date: February 12, 2026.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/29940) [an error](https://github.com/ydb-platform/ydb/issues/29903) where a named expression containing another named expression led to an incorrect backup `VIEW`.
* [Fixed](https://github.com/ydb-platform/ydb/commit/c3b025603a6ba71d27ef0f1f66b9f643407643b3) an error where descending sorting did not work correctly in queries to system views.

### Version 25.1.4.ent.3 {#25-1-4-ent-3}

Release date: November 25, 2025.

#### Functionality

* [Implemented](https://github.com/ydb-platform/ydb/pull/19504) [a vector index](./dev/vector-indexes.md?version=v25.1) for approximate vector search. Recipes for [YDB CLI and YQL](./recipes/vector-search?version=v25.1) have been published for vector search, as well as examples of working [in C++ and Python](./recipes/ydb-sdk/vector-search?version=v25.1).
* [Added](https://github.com/ydb-platform/ydb/issues/11454) support for [consistent asynchronous replication](./concepts/async-replication.md?version=v25.1).
* Added a [V2 configuration mechanism](./devops/configuration-management/configuration-v2/config-overview?version=v25.1) that simplifies deployment of new clusters {{ ydb-short-name }} and further work with them. [Comparison](./devops/configuration-management/compare-configs?version=v25.1) of V1 and V2 configuration mechanisms.
* Added support for the parameterized [Decimal type](./yql/reference/types/primitive.md?version=v25.1#numeric).
* Implemented client-side partition balancing when reading via the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) (like Kafka itself). Previously, balancing occurred on the server. It is enabled by setting the flag `enable_kafka_native_balancing` in the cluster configuration.
* Added support for [topic auto-partitioning](./concepts/cdc.md?version=v25.1#topic-partitions) in CDC for string tables. It is enabled by setting the flag `enable_topic_autopartitioning_for_cdc` in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/8264) the ability to [change the data retention period](./concepts/cdc.md?version=v25.1#topic-options) in a CDC topic using the expression `ALTER TOPIC`.
* [Supported](https://github.com/ydb-platform/ydb/pull/7052) [the format DEBEZIUM_JSON](./concepts/cdc.md?version=v25.1#debezium-json-record-structure) for change feeds (changefeed).
* [Added](https://github.com/ydb-platform/ydb/pull/19507) the ability to create change feeds for index tables.
* Added the ability to [specify the number of replicas](./yql/reference/syntax/alter_table/indexes.md?version=v25.1) for a secondary index. It is enabled by setting the flag `enable_access_to_index_impl_tables` in the cluster configuration.
* In backup and restore operations, [change streams are supported](https://github.com/ydb-platform/ydb/issues/7054). To use this functionality, you need to set the flags `enable_changefeeds_export` and `enable_changefeeds_export` in the `feature_flags` section of the [database](./devops/configuration-management/configuration-v1/dynamic-config.md) or [cluster](./devops/configuration-management/configuration-v1/static-config.md) configuration.
* Added automatic deletion of temporary directories and tables when exporting to S3. It is enabled by setting the flag `enable_export_auto_dropping` in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/12909) automatic integrity check of backups during import, preventing restoration from corrupted backups and protecting against data loss.
* [Added](https://github.com/ydb-platform/ydb/pull/15570) the ability to create views that use [UDF](./yql/reference/builtins/basic.md?version=v25.1#udf) in queries.
* Added system views with information about [access permission settings](./dev/system-views?version=v25.1#top-tli-partitions), [history of overloaded partitions](./dev/system-views?version=v25.1#top-tli-partitions) - enabled by setting the flag `enable_followers_stats` in the cluster configuration, [history of partitions of row-oriented tables with broken locks (TLI)](./dev/system-views?version=v25.1#top-tli-partitions).
* Added new parameters to the [CREATE USER](./yql/reference/syntax/create-user.md?version=v25.1) and [ALTER USER](./yql/reference/syntax/alter-user.md?version=v25.1) statements:
  * `HASH` — the ability to specify a password in encrypted form;
  * `LOGIN` and `NOLOGIN` — unlocking and locking a user.
* Improved account security:
  * [Added](https://github.com/ydb-platform/ydb/pull/11963) [password complexity check](./reference/configuration/?version=v25.1#password-complexity) for users;
  * [Implemented](https://github.com/ydb-platform/ydb/pull/12578) [automatic user locking](./reference/configuration/?version=v25.1#account-lockout) when the password attempt limit is exhausted;
  * [Added](https://github.com/ydb-platform/ydb/pull/12983) the ability for users to change their password themselves.
* [Implemented](https://github.com/ydb-platform/ydb/issues/9748) the ability to switch functional flags while the server is running {{ ydb-short-name }}. Flags for which the [proto-file](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/feature_flags.proto#L60) does not specify the parameter `(RequireRestart) = true` will be applied without restarting the cluster.
* Now the oldest (rather than new) locks [are changed to full-shard locks](https://github.com/ydb-platform/ydb/pull/11329) when the number of locks on shards is exceeded.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12567) preservation of optimistic locks in memory during a smooth restart of datashards, which should reduce the number of ABORTED errors due to lock loss when balancing tables between nodes.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12689) cancellation of volatile transactions with ABORTED status during a smooth restart of datashards.
* [Added](https://github.com/ydb-platform/ydb/pull/6342) the ability to remove `NOT NULL`-constraints on a column in a table using the query `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL`.
* [Added](https://github.com/ydb-platform/ydb/pull/9168) a limit of 100,000 on the number of concurrent session creation requests in the coordination service.
* [Increased](https://github.com/ydb-platform/ydb/pull/14219) the maximum [number of columns in the primary key](./concepts/limits-ydb.md?version=v25.1#schema-object) from 20 to 30.
* Improved diagnostics and introspection of memory-related errors ([#10419](https://github.com/ydb-platform/ydb/pull/10419), [#11968](https://github.com/ydb-platform/ydb/pull/11968)).
* **_(Experimental)_** [Added](https://github.com/ydb-platform/ydb/pull/14075) an experimental mode with stricter access permission checks. It is enabled by setting the following flags:
  * `enable_strict_acl_check` – do not allow granting permissions to non-existent users and deleting users if they have been granted permissions;
  * `enable_strict_user_management` — enables strict rules for administering local users (i.e., only a cluster or database administrator can administer local users);
  * `enable_database_admin` — adds the database administrator role.
* [Added](https://github.com/ydb-platform/ydb/pull/21119) the ability to use familiar data streaming tools – Kafka Connect, Confluent Schema Registry, Kafka Streams, Apache Flink, AKH via the [Kafka API](./reference/kafka-api/index.md) when working with YDB Topics. Now the YDB Topics Kafka API supports:
  * client-side reader balancing – enabled by setting the flag `enable_kafka_native_balancing` in the [cluster configuration](./reference/configuration/feature_flags.md). [How reader balancing works in Apache Kafka](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb). Now reader balancing in the YDB Topics Kafka API will work exactly the same way,
  * [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) – enabled by setting the flag `enable_topic_compactification_by_key`,
  * [transactions](https://www.confluent.io/blog/transactions-apache-kafka) – enabled by setting the flag `enable_kafka_transactions`.
* [Added](https://github.com/ydb-platform/ydb/pull/20982) a [new protocol](https://github.com/ydb-platform/ydb/issues/11064) in [Node Broker](./concepts/glossary.md#node-broker), eliminating network traffic spikes on large clusters (more than 1000 servers) associated with broadcasting node information.

#### Breaking changes

* If you use queries that access named expressions as tables using [AS_TABLE](./yql/reference/syntax/select/from_as_table?version=v25.1), update [temporal over YDB](https://github.com/yandex/temporal-over-ydb) to version [v1.23.0-ydb-compat](https://github.com/yandex/temporal-over-ydb/releases/tag/v1.23.0-ydb-compat) before updating YDB to the current version to avoid errors in executing such queries.

#### YDB UI

* The query editor [now supports](https://github.com/ydb-platform/ydb-embedded-ui/pull/1974) partial result loading — display starts immediately upon receiving the first fragment from the server without waiting for the query to complete. This allows you to get results faster.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/pull/1967) security: controls that are unavailable to the user are no longer displayed in the interface. Users will not encounter "Access denied" errors.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1981) search by tablet ID on the "Tablets" tab.
* Added a hotkey hint that opens with the combination `⌘+K`.
* Added an "Operations" tab to the database page that allows viewing the list of operations and canceling them.
* Updated the cluster monitoring panel, added the ability to collapse it.
* Implemented support for case-sensitive search in the JSON hierarchical display tool.
* After selecting a database, code examples for connecting in YDB SDK were added to the top panel, which speeds up the development process.
* Fixed sorting of strings on the Queries tab.
* Removed unnecessary confirmation prompts when closing the browser page in the query editor — confirmation is now requested only when necessary.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17839) [an error](https://github.com/ydb-platform/ydb/issues/15230) due to which not all tablets were displayed on the Tablets tab in the diagnostics section.
* Fixed [an error](https://github.com/ydb-platform/ydb/issues/18735) due to which the Storage tab in the database diagnostics section displayed not only storage nodes.
* Fixed [a serialization error](https://github.com/ydb-platform/ydb-embedded-ui/issues/2164) that could cause a crash when opening query execution statistics.
* Changed the logic for nodes transitioning to a critical state — a CPU pool filled to 75-99% now triggers a warning rather than a critical state.

#### Performance

* [Added](https://github.com/ydb-platform/ydb/pull/6509) support for [constant folding](https://en.wikipedia.org/wiki/Constant_folding) in the query optimizer by default, which improves query performance by computing constant expressions at the compilation stage.
* [Added](https://github.com/ydb-platform/ydb/issues/6512) a new granular timecast protocol that will reduce the execution time of distributed transactions (slowdown of one shard will not lead to slowdown of all).
* [Implemented](https://github.com/ydb-platform/ydb/issues/11561) functionality for preserving datashard state in memory during restarts, which allows preserving locks and increasing the chances of successful transaction execution. This reduces the execution time of long transactions by reducing the number of retries.
* [Implemented](https://github.com/ydb-platform/ydb/pull/15255) pipelined processing of internal transactions in [Node Broker](./concepts/glossary?version=v25.1#node-broker), which accelerated the startup of dynamic nodes in the cluster {{ ydb-short-name }}.
* [Improved](https://github.com/ydb-platform/ydb/pull/15607) Node Broker resilience to increased load from cluster nodes.
* [Enabled](https://github.com/ydb-platform/ydb/pull/19440) by default, evictable B-Tree indexes instead of non-evictable SST indexes, which reduces memory consumption when storing "cold" data.
* [Optimized](https://github.com/ydb-platform/ydb/pull/15264) memory consumption of storage nodes.
* [Reduced](https://github.com/ydb-platform/ydb/pull/10969) Hive startup time by up to 30%.
* [Optimized](https://github.com/ydb-platform/ydb/pull/6561) the replication process in distributed storage.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9491) the header size of large binary objects in VDisk.
* [Reduced](https://github.com/ydb-platform/ydb/pull/15517) memory consumption by cleaning allocator pages.
* [Optimized](https://github.com/ydb-platform/ydb/pull/20197) handling of empty inputs when executing JOIN operations.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/9707) an error in [Interconnect](./concepts/glossary.md?version=v25.1#actor-system-interconnect) settings that led to performance degradation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13993) the "Out of memory" error when deleting very large tables by regulating the number of tablets simultaneously processing this operation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9848) an error that occurred when specifying the same database node multiple times in the configuration for system tablets.
* [Resolved](https://github.com/ydb-platform/ydb/pull/11059) an error of long (seconds) data reads during frequent table resharding operations.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9723) a read error from asynchronous replicas that caused a failure.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9507) rare hangs during the initial scan of [CDC](./dev/cdc.md?version=v25.1).
* [Fixed](https://github.com/ydb-platform/ydb/pull/11483) handling of incomplete schema transactions in datashards during system restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10460) an error of inconsistent reads from a topic when attempting to explicitly commit a message read within a transaction. Now the user will get an error when trying to commit the message.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12220) an error where autopartitioning did not work correctly when working with a topic in a transaction.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12905) transaction hangs when working with topics during tablet restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13910) the "Key is out of range" error when importing from S3-compatible storage.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13741) incorrect detection of the end of the metadata field in the cluster configuration.
* [Improved](https://github.com/ydb-platform/ydb/pull/16420) secondary index building: when some errors occur, the system retries the process instead of aborting it.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16635) an error executing the expression `RETURNING` in queries `INSERT INTO` and `UPSERT INTO`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16269) the issue of the "Drop Tablet" operation hanging in PQ tablet, especially during Interconnect delays.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16194) an error that occurred during [compaction](./concepts/glossary.md?version=v25.1#compaction) of a VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) an issue where long topic read sessions ended with "too big inflight" errors.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15515) a hang when reading a topic if at least one partition had no incoming data but was read by multiple consumers.
* [Resolved](https://github.com/ydb-platform/ydb/pull/18614) a rare issue of PQ tablet restarts.
* [Resolved](https://github.com/ydb-platform/ydb/pull/18378) an issue where, after upgrading the cluster version, Hive started subscribers in data centers without working database nodes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/19057) an error `Failed to set up listener on port 9092 errno# 98 (Address already in use)` that occurred during a version upgrade.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18905) an error that led to a segmentation fault when simultaneously executing a healthcheck request and shutting down a cluster node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18899) a failure in [partitioning a row-oriented table](./concepts/datamodel/table.md?version=v25.1#partitioning_row_table) when selecting a split key from access samples containing mixed operations with a full key and a key prefix (for example, exact read or range read).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18647) an [error](https://github.com/ydb-platform/ydb/issues/17885) where the index type was incorrectly determined as `GLOBAL SYNC`, although the query explicitly specified `UNIQUE`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16797) an error where automatic topic partitioning did not work when the configuration parameter `max_active_partition` was set using an expression `ALTER TOPIC`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18938) an error where `ydb scheme describe` returned a list of columns in a different order than the one in which they were specified when creating the table.
* [Added](https://github.com/ydb-platform/ydb/pull/21918) support in asynchronous replication for a new type of change record — `reset` records (in addition to `update`- and `erase` records).
* [Fixed](https://github.com/ydb-platform/ydb/pull/21836) an [error](https://github.com/ydb-platform/ydb/issues/21814) where a replication instance with an unspecified parameter `COMMIT_INTERVAL` caused a process failure.
* [Fixed](https://github.com/ydb-platform/ydb/pull/21652) rare errors when reading from a topic during partition balancing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22455) an error where deleting a dedicated database could leave system tablets of the database undeleted.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22203) an error where tablets could hang due to insufficient memory on nodes. Now tablets will automatically start as soon as sufficient resources are freed on any of the nodes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/24278) an error where, when writing Kafka messages, only the first message from the batch was saved, while the remaining messages were ignored.

## Version 24.4 {#24-4}

### Version 24.4.4.20 {#24-4-4-20}

Release date: November 1, 2025.

#### Functionality

* In backup and restore operations, [support was added](https://github.com/ydb-platform/ydb/pull/25675) for views (VIEW). To use this functionality, you need to set the flag `enable_view_export` in the `feature_flags` section of the [database](./devops/configuration-management/configuration-v1/dynamic-config.md) or [cluster](./devops/configuration-management/configuration-v1/static-config.md) configuration.
* The error text for [Transaction locks invalidated](./troubleshooting/performance/queries/transaction-lock-invalidation) now includes additional identifiers when the table cannot be identified (Unknown table): the object path identifier (`PathId`) and the tablet identifier (`TabletId`).

### Version 24.4.4.15 {#24-4-4-15}

Release date: September 19, 2025.

#### Performance

* Columns used for sorting query results are taken into account by the optimizer when automatically selecting a secondary index. This functionality works only for queries against a single table, without joining other tables.

#### Bug fixes

* When an error `OperationAborted` is received in the response from S3, the export operation does not fail but retries writing to S3.

### Version 24.4.4.13 {#24-4-4-13}

Release date: July 29, 2025.

#### Functionality

* [Supported](https://github.com/ydb-platform/ydb/pull/11276) restart without losing cluster availability in a [minimum fault-tolerant configuration](./concepts/topology#reduced) of three nodes.
* [Added](https://github.com/ydb-platform/ydb/pull/13218) new Roaring bitmap UDF functions: AndNotWithBinary, FromUint32List, RunOptimize
* Added the ability to register a [database node](./concepts/glossary.md#database-node) by certificate. In [Node Broker](./concepts/glossary.md#node-broker), a flag `AuthorizeByCertificate` for using the certificate during registration was added.
* [Added](https://github.com/ydb-platform/ydb/pull/11775) priorities for checking authentication tickets [using a third-party IAM provider](./security/authentication.md#iam), with requests from new users processed with the highest priority. Tickets in the cache update their information with a lower priority.
* Added the ability to [read and write to a topic](./reference/kafka-api/examples.md#kafka-api-usage-examples) using the Kafka API without authentication.
* Enabled by default:

  * support for [views (VIEW)](./concepts/datamodel/view.md);
  * [auto-partitioning](./concepts/datamodel/topic.md#autopartitioning) mode for topics;
  * [transactions involving topics and row-based tables](./concepts/transactions.md#topic-table-transactions);
  * [volatile distributed transactions](./contributor/datashard-distributed-txs.md#volatile-transactions).

#### Performance

* [Speeded up](https://github.com/ydb-platform/ydb/pull/12747) tablet startup on large clusters: 210 ms **→** 125 ms (ssd), 260 ms **→** 165 ms (hdd).
* [Limited](https://github.com/ydb-platform/ydb/pull/17755) the number of concurrently processed configuration changes.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18289) memory consumption of PQ tablets.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18473) CPU consumption of the Scheme shard tablet, which reduced response delays for requests. Now the limit on the number of Scheme shard operations is checked before performing tablet split and merge operations.
* Enabled by default [automatic secondary index selection](./dev/secondary-indexes.md#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) when executing a query.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/12221) an error that significantly increased CPU load when reading small messages from a topic in small portions. This could lead to delays in reading/writing to that topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13918) an error in restoring from a backup created at the moment of automatic table splitting.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12601) an error in serialization `Uuid` for [CDC](./concepts/cdc.md).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12804) an error that could cause reads on tablet subscribers to fail during automatic table splitting.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12807) an error where the [coordination node](./concepts/datamodel/coordination-node.md) successfully registered proxy servers despite a connection break.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11593) errors that occurred when opening the tab with information about [distributed storage groups](./concepts/glossary.md#storage-group) in the interface.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12448) an [error](https://github.com/ydb-platform/ydb/issues/12443) that caused [Health Check](./reference/ydb-sdk/health-check-api) not to report problems with time synchronization.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17123) a rare error causing client applications to hang during transaction commit when partition deletion occurred before updating the write quota for the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17312) an error in copying tables with the Decimal type that caused a failure when rolling back to a previous version.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17519) [an error](https://github.com/ydb-platform/ydb/issues/17499) where a commit without confirmation of a write to a topic caused the current and subsequent transactions with topics to hang.
* Fixed transaction hangs when working with topics during [restart](https://github.com/ydb-platform/ydb/issues/17843) or [deletion](https://github.com/ydb-platform/ydb/issues/17915) of a tablet.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18114) [issues](https://github.com/ydb-platform/ydb/issues/18071) with reading messages larger than 6Mb via [Kafka API](./reference/kafka-api).
* [Eliminated](https://github.com/ydb-platform/ydb/pull/18319) a memory leak when writing to a [topic](./concepts/glossary#topic).
* Fixed errors in processing [nullable columns](https://github.com/ydb-platform/ydb/issues/15701) and [columns with the UUID type](https://github.com/ydb-platform/ydb/issues/15697) in row-oriented tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/14811) an error that caused a significant decrease in read speed from [tablet subscribers](./concepts/glossary.md#tablet-follower).
* [Fixed](https://github.com/ydb-platform/ydb/pull/14516) an error that caused a volatile distributed transaction to wait for confirmation until the next restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15077) a rare error that caused a failure when connecting tablet subscribers to a leader with an inconsistent command log state.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15074) a rare error that caused a failure when restarting a remote datashard with inconsistent changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15194) an error that could disrupt the order of message processing in a topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15308) a rare error that could cause reading from a topic to hang.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15160) an issue where a transaction hung when a user managed a topic while a PQ tablet was being moved to another node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) an issue with a counter value leak for userInfo that could lead to a read error `too big in flight`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15467) a proxy server crash due to duplicate topics in a request.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15933) a rare error that allowed a user to write to a topic bypassing account quota limits.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16288) an issue where the system returned "OK" after deleting a topic, but its tablets continued to run. To delete such tablets, follow the instructions in [pull request](https://github.com/ydb-platform/ydb/pull/16288).
* [Fixed](https://github.com/ydb-platform/ydb/pull/16418) a rare error where a backup of a large table with a secondary index was not restored.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15862) an issue that caused an error when inserting data using `UPSERT` into row-oriented tables with default values.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/15334) an error that caused a failure when executing queries against tables with secondary indexes that return result lists using the expression `RETURNING *`.

## Version 24.3 {#24-3}

### Version 24.3.13.11 {#24-3-13-11}

Release date: March 6, 2025.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/13501) a rare issue that led to leaks of uncommitted changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13948) consistency issues related to caching of deleted ranges.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15182) an issue with long-term caching of negative responses to authentication requests from an LDAP-compatible user and group directory.

### Version 24.3.13.10 {#24-3-13-10}

Release date: December 24, 2024.

#### Functionality

* Added [request tracing](./reference/observability/tracing/setup) – a tool that allows you to see in detail the path of a request through a distributed system.
* Added support for [asynchronous replication](./concepts/async-replication), which allows you to synchronize data between YDB databases almost in real time. It can also be used to migrate data between databases with minimal downtime for the applications using them.
* Added support for [views (VIEW)](https://ydb.tech/docs/en/concepts/datamodel/view), which can be enabled by the cluster administrator using the setting `enable_views` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* [Federated queries](./concepts/query_execution/federated_query/) now support new external data sources: MySQL, Microsoft SQL Server, Greenplum.
* Developed [documentation](./devops/deployment-options/manual/federated-queries/connector-deployment) on deploying YDB with federated query functionality (in manual mode).
* For the Docker container with YDB, a startup parameter was added `FQ_CONNECTOR_ENDPOINT` that allows you to specify the address of the connector to external data sources. Added the ability to use TLS encryption for the connection to the connector. Added the ability to output the port of the connector service running locally on the same host as the YDB dynamic node.
* Added [auto-partitioning](./concepts/datamodel/topic#autopartitioning) mode for topics, in which topics can split partitions depending on load while preserving guarantees of message read order and exactly once writes. The mode can be enabled by the cluster administrator using the settings `enable_topic_split_merge` and `enable_pqconfig_transactions_at_scheme_shard` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* Added [transactions](./concepts/transactions#topic-table-transactions) involving [topics](https://ydb.tech/docs/en/concepts/datamodel/topic) and row-based tables. Thus, you can transactionally move data from tables to topics and in the reverse direction, as well as between topics, so that data is not lost or duplicated. Transactions can be enabled by the cluster administrator using the settings `enable_topic_service_tx` and `enable_pqconfig_transactions_at_scheme_shard` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* [Added](https://github.com/ydb-platform/ydb/pull/7150) support for [CDC](./concepts/cdc) for synchronous secondary indexes.
* Added the ability to change the record retention period in [CDC](./concepts/cdc.md) topics.
* Added support for [auto-increment](./yql/reference/types/serial) for columns included in the table's primary key.
* Added an entry to the [audit log](./security/audit-log) for user login events in YDB, user session termination events in the user interface, as well as backup and restore-from-backup requests.
* Added a system view that allows you to get information about sessions established with the database using a query.
* Added support for constant default values for columns of row-oriented tables.
* Added support for the expression `RETURNING` in queries.
* Added a [built-in function](./yql/reference/builtins/basic.md#version) `version()`.
* [Added](https://github.com/ydb-platform/ydb/pull/8708) start/end time and author to the metadata of backup/restore operations from S3-compatible storage.
* Added support for backup/restore of table ACLs from S3-compatible storage.
* For queries reading from S3, the plan now includes paths and decompression method.
* Added new parsing settings for `timestamp`, `datetime` when reading data from S3.
* Added support for the type `Decimal` in [partitioning keys](https://ydb.tech/docs/en/dev/primary-key/column-oriented#klyuch-particionirovaniya).
* Improved storage issue diagnostics in HealthCheck.
* **_(Experimental)_** Added a [cost-based optimizer](./concepts/query_execution/optimizer#stoimostnoj-optimizator-zaprosov) for complex queries involving [column-oriented tables](./concepts/glossary#column-oriented-table). The optimizer considers a large number of alternative execution plans and selects the best one based on the estimated cost of each option. Currently, the optimizer only works with plans that include [JOIN](./yql/reference/syntax/join) operations.
* **_(Experimental)_** Implemented an initial version of the [workload manager](./dev/resource-consumption-management), which allows creating resource pools with limits on CPU, memory, and the number of active queries. Resource classifiers have been implemented to assign queries to a specific resource pool.
* **_(Experimental)_** Implemented [automatic index selection](https://ydb.tech/docs/en/dev/secondary-indexes#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) during query execution, which can be enabled by the cluster administrator using the setting `index_auto_choose_mode` in `table_service_config` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).

#### YDB UI

* Supported creation and [display](https://github.com/ydb-platform/ydb-embedded-ui/issues/782) of an async replication instance.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/929) designation for [columns with auto-increment](./yql/reference/types/serial).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1438) a tab with information about [tablets](./concepts/glossary#tablet).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1289) a tab with information about [distributed storage groups](./concepts/glossary#storage-group).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1218) a setting to add [tracing](./reference/observability/tracing/setup) to all queries and display query tracing results.
* Added [attributes](https://github.com/ydb-platform/ydb-embedded-ui/pull/1069), disk space consumption information, and a button that triggers [disk decommissioning](./devops/deployment-options/manual/decommissioning) to the PDisk page.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1313) information about running queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1291) a row limit setting for the query editor output and a display if query results exceed the limit.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1049) a display of the list of queries with maximum CPU consumption over the last hour.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1127) search on the query history and saved query list pages.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1117) the ability to interrupt query execution.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/944) the ability to save a query from the editor using hotkeys.
* [Separated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1422) the display of disks from donor disks.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1154) support for InterruptInheritance ACL and improved display of effective ACLs.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/889) a display of the current user interface version.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1229) information about the state of experimental functionality enablement settings.

#### Performance

* [Accelerated](https://github.com/ydb-platform/ydb/pull/7589) recovery from backup of tables with secondary indexes by up to 20% in our tests.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9721) Interconnect throughput.
* Improved performance of CDC topics containing thousands of partitions.
* Made a number of improvements to the Hive tablet balancing algorithm.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/6850) an error that rendered a database with a large number of tables or partitions inoperable during recovery from a backup. Now, when database size limits are exceeded, the recovery operation will fail with an error, and the database will continue to operate normally.
* [Implemented](https://github.com/ydb-platform/ydb/pull/11532) a mechanism that forcibly triggers background [compaction](./concepts/glossary#compaction) when discrepancies are detected between the data schema and the data stored in [DataShard](./concepts/glossary#data-shard). This resolves a rare issue of delays in data schema changes.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/10447) duplication of authentication tickets, which led to an increased number of requests to authentication providers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9377) an invariant violation error during the initial CDC scan, which caused the ydbd server process to crash.
* [Prohibited](https://github.com/ydb-platform/ydb/pull/9446) changing the schema of backup tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9509) hanging of the initial CDC scan during frequent table updates.
* [Excluded](https://github.com/ydb-platform/ydb/pull/9934) dropped indexes from the count of the limit on the [maximum number of indexes](https://ydb.tech/docs/en/concepts/limits-ydb#schema-object).
* [Fixed](https://github.com/ydb-platform/ydb/pull/8847) an [error](https://github.com/ydb-platform/ydb/issues/6985) in displaying the time scheduled for executing a set of transactions (scheduled step).
* [Fixed](https://github.com/ydb-platform/ydb/pull/9161) [an issue](https://github.com/ydb-platform/ydb/issues/8942) with blue–green deployment interruption in large clusters caused by frequent node list updates.
* [Fixed](https://github.com/ydb-platform/ydb/pull/8925) a rare error that led to transaction execution order violation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9841) [a bug](https://github.com/ydb-platform/ydb/issues/9797) in the EvWrite API that caused incorrect memory deallocation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10698) [an issue](https://github.com/ydb-platform/ydb/issues/10674) with volatile transactions hanging after a restart.
* Fixed a bug in CDC that in some cases led to increased CPU consumption, up to one core per CDC partition.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/11061) read latency occurring during and after splitting of some partitions.
* Fixed errors when reading data from S3.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4793) the way of calculating the AWS signature when accessing S3.
* Fixed false positives of the HealthCheck system during backup of a database with a large number of shards.
* [Removed](https://github.com/ydb-platform/ydb/pull/11901) the restriction on writing values greater than 127 to the Uint8 type.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12915) an error in recovery from a backup saved in S3 storage with Path-style addressing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12018) a potential breakage of ["frozen" locks](./contributor/datashard-locks-and-change-visibility#vzaimodejstvie-s-raspredelyonnymi-tranzakciyami) that could be caused by bulk operations (for example, deletion by TTL).
* [Fixed](https://github.com/ydb-platform/ydb/pull/11658) a rare issue that led to errors when executing a read query.

## Version 24.2 {#24-2}

### Version 24.2.7.1 {#24-2-7-1}

Release date: August 20, 2024.

### Functionality

* Added the ability to [set priorities](./devops/deployment-options/manual/maintenance.md#rolling-restart) for maintenance tasks in the [cluster management system](./concepts/glossary.md#cms).
* Added [configuration of stable names](reference/configuration/node_broker_config.md#node-broker-config) for cluster nodes within a tenant.
* Added retrieval of nested groups from the [LDAP server](./security/authentication.md#ldap); in the [LDAP configuration](reference/configuration/auth_config.md#ldap-auth-config), host parsing was improved and a setting was added to disable built-in authentication by login and password.
* Added the ability to authenticate [dynamic nodes](./concepts/glossary.md#dynamic) using an SSL certificate.
* Implemented removal of inactive nodes from [Hive](./concepts/glossary.md#hive) without restarting it.
* Improved management of inflight pings during Hive restart in large clusters.
* [Changed](https://github.com/ydb-platform/ydb/pull/6381) the order of establishing connections to nodes during Hive restart.

### YDB UI

* [Added](https://github.com/ydb-platform/ydb/pull/7485) the ability to set a TTL for the user session in the configuration file.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1028) sorting by `CPUTime` in the table with the list of queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7779) loss of precision when working with `double`, `float`.
* Supported [directory creation from the UI](https://github.com/ydb-platform/ydb-embedded-ui/issues/958).
* [Added the ability](https://github.com/ydb-platform/ydb-embedded-ui/pull/976) to set the background data refresh interval on all pages.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/issues/955) ACL display.
* Autocomplete in the query editor is enabled by default.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/834) View support.

### Bug fixes

* Added a check for the local transaction size before committing it to fix [errors](https://github.com/ydb-platform/ydb/issues/6677) in schema operations when exporting/backing up large databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7709) the [error](https://github.com/ydb-platform/ydb/issues/7674) of duplicate SELECT query results when reducing the quota in [DataShard](./concepts/glossary#data-shard).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6461) [errors](https://github.com/ydb-platform/ydb/issues/6220) that occur when the [coordinator](./concepts/glossary#coordinator) state changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5992) errors that occur during the initial [CDC](./dev/cdc) scan.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6615) a race condition in asynchronous change delivery (async indexes, CDC).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5993) a rare error where deletion by [TTL](./concepts/ttl) caused the process to terminate unexpectedly.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5760) an error displaying the PDisk status in the [CMS](./concepts/glossary#cms) interface.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6008) errors that could cause a soft drain of tablets from a node to hang.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6445) an error stopping the interconnect proxy on a node running without restarts when another node is added to the cluster.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6695) free memory accounting in [interconnect](./concepts/glossary#actor-system-interconnect).
* [Fixed](https://github.com/ydb-platform/ydb/issues/6405) the UnreplicatedPhantoms/UnreplicatedNonPhantoms counters in VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/issues/6398) handling of empty garbage collection requests on VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5894) management of TVDiskControls settings via CMS.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5883) an error loading data created by newer VDisk versions.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5862) an error executing a query `REPLACE INTO` with a default value.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7714) an error executing queries that performed multiple left joins to the same string table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7740) loss of precision for `float`, `double` types when using CDC.

## Version 24.1 {#24-1}

### Version 24.1.18.1 {#24-1-18-1}

Release date: July 31, 2024.

### Functionality

* Implemented [Knn UDF](./yql/reference/udf/list/knn.md) for exact nearest vector search.
* Developed the gRPC service QueryService, which enables execution of all query types (DML, DDL) and retrieval of unlimited data volumes.
* Implemented [integration with the LDAP protocol](./security/authentication.md) and the ability to obtain a list of groups from external LDAP directories.

### Built-in UI

* Added a resource consumption diagnostics dashboard, located on the database information tab, which allows you to determine the current state of consumption of key resources: CPU cores, RAM, and space in the network distributed storage.
* Added charts for monitoring the key performance indicators of the cluster {{ ydb-short-name }}.

### Performance

* [Optimized](https://github.com/ydb-platform/ydb/pull/1837) session timeouts of the coordination service from server to client. Previously, the timeout was 5 seconds, which in the worst case led to detecting a non-operational client (and releasing the resources it held) within 10 seconds. In the new version, the check time depends on the session wait time, which ensures faster response during leader changes or acquisition of distributed locks.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2391) CPU consumption by [SchemeShard](./concepts/glossary.md#scheme-shard) replicas, especially when processing fast updates for tables with a large number of partitions.

### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/3917) an error of possible queue overflow; [Change Data Capture](./dev/cdc.md) reserves the capacity of the change queue during the initial scan.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4597) a potential deadlock between receiving CDC records and sending them.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2056) the issue of losing the mediator task queue when the mediator reconnects; the fix allows processing the mediator task queue during resynchronization.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2624) a rare error where, with volatile transactions enabled and used, a successful transaction commit result was returned before the transaction was actually committed. Volatile transactions are disabled by default and are under development.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2839) a rare error that led to the loss of established locks and successful confirmation of transactions that should have failed with the Transaction Locks Invalidated error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3074) a rare error that could lead to violation of data integrity guarantees during concurrent writes and reads of data by a specific key.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4343) the issue due to which read replicas stopped processing requests.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4979) a rare error that could cause database processes to crash in the presence of uncommitted transactions over a table at the time of its rename.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3632) an error in the logic for determining the status of a static group, when a static group was not marked as non-operational although it should have been.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error of partial commit of a distributed transaction with uncommitted changes in case of some races with restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2374) anomalies with reading stale data that were [detected using Jepsen](https://blog.ydb.tech/hardening-ydb-with-jepsen-lessons-learned-e3238a7ef4f2).

## Version 23.4 {#23-4}

### Version 23.4.11.1 {#23-4-11-1}

Release date: May 14, 2024.

### Performance

* [Fixed](https://github.com/ydb-platform/ydb/pull/3638) the issue of increased consumption of computing resources by the topic actor `PERSQUEUE_PARTITION_ACTOR`.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2083) resource usage by SchemeBoard replicas. The greatest effect is noticeable when modifying metadata of tables with a large number of partitions.

### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error of possible incomplete commit of accumulated changes when using distributed transactions. This error occurs with an extremely rare combination of events, including a restart of tablets serving the table partitions involved in the transaction.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/3165) a race between table merging and garbage collection processes, due to which garbage collection could fail with an invariant violation error and, as a result, an emergency termination of the server process `ydbd`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2696) an error in Blob Storage, due to which information about a change in the composition of a storage group might not arrive in a timely manner to individual cluster nodes. As a result, in rare cases, read and write operations of data stored in the affected group could be blocked, and manual administrator intervention was required.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3002) an error in Blob Storage, due to which data storage nodes might not start with a correct configuration. The error manifested for systems with the experimental feature "blob depot" explicitly enabled (by default, this feature is disabled).
* [Fixed](https://github.com/ydb-platform/ydb/pull/2475) an error that occurred in some situations when writing to a topic with an empty `producer_id` with deduplication disabled. It could lead to an emergency termination of the server process `ydbd`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2651) a problem leading to a crash of the process `ydbd` due to an erroneous state of the topic write session.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3587) an error in displaying the metric of the number of partitions in a topic; previously, an incorrect value was displayed.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/2126) memory leaks that manifested when copying topic data between clusters {{ ydb-short-name }}. They could lead to termination of server processes `ydbd` due to exhaustion of available RAM.

## Version 23.3 {#23-3}

### Version 23.3.25.2 {#23-3-25-2}

Release date: October 12, 2023.

### Functionality

* Visibility of own changes within transactions is implemented. Previously, when attempting to read data already modified by the current transaction, the query failed with an error. This led to the need to order reads and writes within a transaction. With the advent of visibility of own changes, these restrictions are lifted, and queries can read rows modified in the given transaction.
* Added support for [column-oriented tables](concepts/datamodel/table.md#column-tables). Column-oriented tables are well suited for analytical queries (Online Analytical Processing), as only the columns directly involved in the query are read during query execution. YDB column-oriented tables allow you to create analytical reports with performance comparable to specialized analytical DBMSs.
* Added support for [Kafka API for topics](reference/kafka-api/index.md). Now you can work with YDB topics via a Kafka-compatible API designed for migrating existing applications. Kafka protocol version 3.4.0 is supported.
* Added the ability to [write to a topic without deduplication](concepts/datamodel/topic.md#no-dedup). This type of write is well suited for cases where the order of message processing is not critical. Writing without deduplication is faster and consumes fewer server resources, but messages are not ordered or deduplicated on the server.
* YQL now supports [creating](yql/reference/syntax/create-topic.md), [altering](yql/reference/syntax/alter-topic.md), and [dropping](yql/reference/syntax/drop-topic.md) topics.
* Added the ability to assign and revoke access rights using the YQL commands [GRANT](yql/reference/syntax/grant.md) and [REVOKE](yql/reference/syntax/revoke.md).
* Added the ability to log DML operations in the audit log.
* **_(Experimental)_** When writing messages to a topic, you can now pass metadata. To enable this functionality, add `enable_topic_message_meta: true` to the [configuration file](reference/configuration/index.md).
* **_(Experimental)_** Added the ability to [read from topics](reference/ydb-sdk/topic.md#read-tx) and write to a table within a single transaction. The new capability simplifies the scenario of moving data from a topic to a table. To enable it, add `enable_topic_service_tx: true` to the configuration file.
* **_(Experimental)_** Added support for PostgreSQL compatibility. The new mechanism allows you to run SQL queries in the PostgreSQL dialect on YDB infrastructure using the PostgreSQL network protocol. You can use familiar PostgreSQL tools such as psql and drivers (pq for Golang and psycopg2 for Python), as well as develop queries in familiar PostgreSQL syntax with YDB horizontal scalability and fault tolerance.
* **_(Experimental)_** Added support for [federated queries](concepts/query_execution/federated_query/index.md). It allows you to retrieve information from various data sources without moving it to YDB. Interaction with ClickHouse, PostgreSQL, and S3 is supported via YQL queries without duplicating data between systems.

### Built-in UI

* A new option `PostgreSQL` has been added to the query type selector settings, which is available when the `Enable additional query modes` parameter is enabled. The query history now also takes into account the syntax used when executing a query.
* The YQL query template for creating a table has been updated. A description of available parameters has been added.
* Sorting and filtering for Storage and Nodes tables have been moved to the server. You need to enable the parameter `Offload tables filters and sorting to backend` in the experiments section to use this functionality.
* Buttons for creating, modifying, and deleting [topics](concepts/datamodel/topic.md) have been added to the context menu.
* Sorting by severity has been added for all issues in the tree in `Healthcheck`.

### Performance

* Iterator reads have been implemented. The new functionality allows separating reads and computations from each other. Iterator reads allow datashards to increase the throughput of read queries.
* Write performance to YDB topics has been optimized.
* Improved tablet balancing under node overload.

### Bug fixes

* Fixed a bug where read iterators could block snapshots unknown to coordinators.
* Fixed a memory leak when closing a connection in the kafka proxy.
* Fixed an error where snapshots taken via read iterators might not be restored on restarts.
* Fixed an incorrect residual predicate for the condition `IS NULL` on a column.
* Fixed a triggering check `VERIFY failed: SendResult(): requirement ChunksLimiter.Take(sendBytes) failed`.
* Fixed `ALTER TABLE` by `TTL` for column-oriented tables.
* Implemented `FeatureFlag` that allows disabling/enabling work with `CS` and `DS`.
* Fixed the coordinator time difference between 23-2 and 23-3 by 50ms.
* Fixed an error where the handle `storage` returned extra groups when the parameter `node_id` in the `viewer backend`.
* Added `usage` filter in `/storage` in `viewer backend`.
* Fixed an error in Storage v2 where an incorrect number was returned in `Degraded`.
* Fixed cancellation of subscriptions from sessions in iterator reads during tablet restart.
* Fixed an error where during a rolling restart when going through the balancer, it blinks with `healthcheck` alerts about storage.
* Updated metrics `cpu usage` in ydb.
* Fixed ignoring `NULL` when specifying `NOT NULL` in the table schema.
* Implemented output of operation records `DDL` to the common log.
* Implemented a prohibition for the command `ydb table attribute add/drop` to work with any objects other than tables.
* Disabled `CloseOnIdle` for `interconnect`.
* Fixed duplication of read speed in the UI.
* Fixed an error where data could be lost on `block-4-2`.
* Added a topic name check.
* Fixed a possible `deadlock` in the actor system.
* Fixed the test `KqpScanArrowInChanels::AllTypesColumns`.
* Fixed the test `KqpScan::SqlInParameter`.
* Fixed concurrency issues for OLAP queries.
* Fixed insertion of `ClickBench parquet`.
* Added the missing call `CheckChangesQueueOverflow` to the general `CheckDataTxReject`.
* Fixed an error of returning an empty status when calling `ReadRows API`.
* Fixed an incorrect export retry in the final stage.
* Fixed an issue with an infinite quota on the number of records in a CDC topic.
* Fixed an error importing a column `string` and `parquet` into a column `string` OLAP.
* Fixed a crash `KqpOlapTypes.Timestamp` under tsan.
* Fixed a crash in `viewer backend` when attempting to execute a query to the database due to version incompatibility.
* Fixed an error where `viewer` did not return a response from `healthcheck` due to a timeout.
* Fixed an error where an incorrect value could be stored in Pdisks `ExpectedSerial`.
* Fixed an error where database nodes crash due to `segfault` in the S3 actor.
* Fixed a race in `ThreadSanitizer: data race KqpService::ToDictCache-UseCache`.
* Fixed a race in `GetNextReadId`.
* Fixed an overestimation of the result `SELECT COUNT(*)` immediately after import.
* Fixed an error where `TEvScan` could return an empty data set in case of a datashard split.
* Added a separate issue/error code in case of exhausting available space.
* Fixed an error `GRPC_LIBRARY Assertion failed`.
* Fixed an error where reading by a secondary index in scan queries returned an empty result.
* Fixed validation `CommitOffset` in `TopicAPI`.
* Reduced consumption `shared cache` when approaching OOM.
* Merged the scheduler logic from `data executer` and `scan executer` into one class.
* Added handles `discovery` and `proxy` to the execution process `query` in `viewer backend`.
* Fixed an error where the handle `/cluster` returns the name of the root domain of type `/ru` in `viewer backend`.
* Implemented a seamless table update scheme for `QueryService`.
* Fixed an error where `DELETE` returned data and did NOT delete it.
* Fixed an error in the operation of `DELETE ON` in `query service`.
* Fixed unexpected disabling of batching in default schema settings.
* Fixed a triggered check `VERIFY failed: MoveUserTable(): requirement move.ReMapIndexesSize() == newTableInfo->Indexes.size()`.
* Increased the default grpc streaming timeout.
* Excluded unused messages and methods from `QueryService`.
* Added sorting by `Rack` in `/nodes` in `viewer backend`.
* Fixed an error where a query with sorting returns an error when sorting in descending order.
* Fixed interaction `QP` with `NodeWhiteboard`.
* Removed support for old parameter formats.
* Fixed an error where `DefineBox` was not applied to disks that have a static group.
* Fixed a bug `SIGSEGV` in dynodes during import `CSV` via `YDB CLI`.
* Fixed a crash when processing `NGRpcService::TRefreshTokenImpl`.
* Implemented `gossip` protocol for exchanging information about cluster resources.
* Fixed an error:

  ```text
  DeserializeValuePickleV1(): requirement data.GetTransportVersion() ==
  (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0 failed
  ```

* Implemented auto-increment columns.
* Use status `UNAVAILABLE` instead of `GENERIC_ERROR` on shard identification error.
* Added support for `rope payload` in `TEvVGet`.
* Added ignoring of stale events.
* Fixed a crash of write sessions on an invalid topic name.
* Fixed an error:

  ```text
  CheckExpected(): requirement newConstr failed, message: Rewrite error,
  missing Distinct((id)) constraint in node FlatMap
  ```

* Enabled `self heal` by default.
