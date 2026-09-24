# Yandex Enterprise Database changelog

## Version 26.1 {#26-1}

### Version 26.1.1.ent.3 {#26-1-1-ent-3}

Release date: August 3, 2026.

This version includes all improvements from {{ ydb-short-name }} 26.1.1.22; see the [changelog](./changelog-server.md#26-1-1-22). It also includes the [enterprise-specific improvements](#26-1-1-ent-3-extras) listed below.

#### Enterprise-specific Improvements {#26-1-1-ent-3-extras}

The following changes are available in Yandex Enterprise Database in addition to the corresponding {{ ydb-short-name }} build:

* Added an optimization that allows filtering rows by index columns before querying the main table, reducing the number of accesses to the main table when executing certain types of queries.
* Implemented a set of fixes in index access (StreamIndexLookup) that eliminates the possibility of rare situations where executed queries could hang, and reduces RAM consumption during query execution.
* Invalid views can now be restored from a backup. This allows restoring backups created from databases containing such views without additional actions from the administrator.
* Added support for mutual certificate-based authentication (mTLS) in the [Kafka API](./reference/kafka-api/index.md).
* Added the `TraceId` column with the query trace identifier to the `.sys/top_queries_*` and `.sys/query_sessions` system views.

## Version 25.4 {#25-4}

### Version 25.4.1.ent.2 {#25-4-1-ent-2}

Release date: June 17, 2026.

This version includes all improvements from {{ ydb-short-name }} 25.4.1.15; see the [changelog](./changelog-server.md#25-4-1-15). It also includes all [additional fixes](#25-2-1-ent-13-extras) listed below for version 25.2.1.ent.13.

## Version 25.3 {#25-3}

### Version 25.3.1.ent.3 {#25-3-1-ent-3}

Release date: June 11, 2026.

This version includes all improvements from {{ ydb-short-name }} 25.3.1.27; see the [changelog](./changelog-server.md#25-3-1-27). It also includes all [additional fixes](#25-2-1-ent-13-extras) listed below for version 25.2.1.ent.13.

## Version 25.2 {#25-2}

### Version 25.2.1.ent.13 {#25-2-1-ent-13}

Release date: June 11, 2026.

This version includes all improvements from {{ ydb-short-name }} 25.2.1.26; see the [changelog](./changelog-server.md#25-2-1-26). It also includes a number of additional improvements ported from the current 26.1 version.

#### Additional Fixes {#25-2-1-ent-13-extras}

The following changes were ported from version 26.1 into supported stable versions of Yandex Enterprise Database:

* Fixed a bug that violated the sort order specified in the query when accessing system tables.
* Fixed a bug in internal state integrity check logic that in rare cases could cause a single (not mass) restart of storage nodes.
* Added an optimization that allows filtering rows by index columns before querying the main table, reducing the number of accesses to the main table when executing certain types of queries.
* Implemented a set of fixes in index access (StreamIndexLookup) that eliminates the possibility of rare situations where executed queries could hang, and reduces RAM consumption during query execution.
* Added an optimization that reduces memory consumption when processing queries with the TopSort operation (`SELECT ... ORDER BY x LIMIT n`).
* Added support for index materialization during backup and restore.
* TLI (Transaction Locks Invalidated) error messages now always include either an identifier or the path of the affected table.
* Lock metrics have been added to query statistics provided through the `.sys/query_metrics_*` system tables.
* Invalid views can now be restored from a backup. This allows restoring backups created from databases containing such views without additional actions from the administrator.

### Version 25.2.1.ent.4 {#25-2-1-ent-4}

Release date: February 12, 2026.

#### New Features

* [Analytical capabilities](./concepts/analytics/index.md) are available by default: [column-oriented tables](./concepts/datamodel/table.md#column-oriented-tables) can be created without special flags, using LZ4 compression and hash partitioning. Supported operations include a wide range of DML operations (UPDATE, DELETE, UPSERT, INSERT INTO ... SELECT) and CREATE TABLE AS SELECT. Integration with dbt, Apache Airflow, Jupyter, Superset, and federated queries to S3 enables building end-to-end analytical pipelines in YDB.
* [Cost-Based Optimizer](./concepts/query_execution/optimizer.md) is enabled by default for queries involving at least one column-oriented table but can also be enabled manually for other queries. The Cost-Based Optimizer improves query performance by determining the optimal join order and join types based on table statistics; supported [hints](./dev/query-execution-optimization/query-hints.md) allow fine-tuning execution plans for complex analytical queries.
* Added YDB Transfer — an asynchronous mechanism for transferring data from a topic to a table. You can create, update, or delete a transfer instance using YQL. For a quick start, use the [instruction with an example]().
* Added [spilling](./concepts/query_execution/spilling.md), a memory management mechanism, that temporarily offloads intermediate data arising from computations and exceeding available node RAM capacity to external storage. Spilling allows executing user queries that require processing large data volumes exceeding available node memory.
* Increased the [maximum amount of time allowed for a single query to execute](./concepts/limits-ydb) from 30 minutes to 2 hours.
* Added support for a user-defined Certificate Authority (CA) and [Yandex Cloud Identity and Access Management (IAM)](https://yandex.cloud/ru/docs/iam) authentication in [asynchronous replication](./yql/reference/syntax/create-async-replication.md).
* Enabled by default:
  * [vector index](./dev/vector-indexes.md) for approximate vector similarity search;
  * support for [client-side consumer balancing](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb), [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) and [transactions](https://www.confluent.io/blog/transactions-apache-kafka/) in [YDB Topics Kafka API](./reference/kafka-api/index.md);
  * support for [auto-partitioning topics](./concepts/cdc.md#topic-partitions) for row-oriented tables in CDC;
  * support for auto-partitioning topics in asynchronous replication;
  * support for [parameterized Decimal type](./yql/reference/types/primitive.md#numeric);
  * support for [Datetime64 data type](./yql/reference/types/primitive.md#datetime);
  * automatic cleanup of temporary tables and directories during export to S3;
  * support for [changefeeds](./concepts/cdc.md) in backup and restore operations;
  * the ability to specify the number of replicas for a secondary index;
  * system views with the history of overloaded partitions.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/24265) CPU resource limiting for column-oriented tables in Workload Manager. Previously CPU consumption could exceed the configured limits.
* [Fixed](https://github.com/ydb-platform/ydb/pull/25112) an [issue](https://github.com/ydb-platform/ydb/issues/23858) where [tablet](./concepts/glossary.md#tablet) deletion might get stuck.
* [Fixed](https://github.com/ydb-platform/ydb/pull/25145) an [issue](https://github.com/ydb-platform/ydb/issues/20866) that caused an error when changing a table's follower.
* Fixed a couple of [changefeed](./concepts/glossary.md#changefeed) related issues:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25689) an [issue](https://github.com/ydb-platform/ydb/issues/25524) where importing a table with a Utf8 primary key and an enabled changefeed could fail.
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25453) an [issue](https://github.com/ydb-platform/ydb/issues/25454) where importing a table without changefeeds could fail due to incorrect changefeed file lookup.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26069) an [issue](https://github.com/ydb-platform/ydb/issues/25869) that could cause errors during UPSERT operations in column tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26504) an [error](https://github.com/ydb-platform/ydb/issues/26225) that could cause a crash due to accessing freed memory.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26657) an [issue](https://github.com/ydb-platform/ydb/issues/23122) with duplicates in unique secondary index.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26879) an [issue](https://github.com/ydb-platform/ydb/issues/26565) with checksum mismatch error on restoration of compressed backups from S3.
* [Fixed](https://github.com/ydb-platform/ydb/pull/27528) an [issue](https://github.com/ydb-platform/ydb/issues/27193) where some queries from the TPC-H 1000 benchmark could fail.
* Fixed a couple of cluster bootstrap related issues:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25678) an [issue](https://github.com/ydb-platform/ydb/issues/25023) where cluster bootstrap could hang when mandatory authorization was enabled.
  * [Fixed](https://github.com/ydb-platform/ydb/pull/28886) an [issue](https://github.com/ydb-platform/ydb/issues/27228) where it was impossible to create new databases for several minutes immediately after cluster deployment.
* [Fixed](https://github.com/ydb-platform/ydb/pull/28655) an [issue](https://github.com/ydb-platform/ydb/issues/28510) where race condition could occur and clients receive `Could not find correct token validator` error when missing newly issued tokens before `LoginProvider` state is updated.

## Version 25.1 {#25-1}

### Version 25.1.4.ent.8 {#25-1-4-ent-8}

Release date: February 12, 2026.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/29940) an [issue](https://github.com/ydb-platform/ydb/issues/29903) where named expression containing another named expression caused incorrect `VIEW` backup.
* [Fixed](https://github.com/ydb-platform/ydb/commit/c3b025603a6ba71d27ef0f1f66b9f643407643b3) descending sorting not working in queries to system views.

### Version 25.1.4.ent.3 {#25-1-4-ent-3}

Release date: November 25, 2025.

#### New Features

* [Implemented](https://github.com/ydb-platform/ydb/issues/19504) a [vector index](./dev/vector-indexes.md?version=v25.1) for approximate vector similarity search. Recipes for [YDB CLI and YQL](), as well as examples in [C++ and Python](), are available.
* [Added](https://github.com/ydb-platform/ydb/issues/11454) support for [consistent asynchronous replication](./concepts/async-replication.md?version=v25.1).
* Added [configuration mechanism V2](./devops/configuration-management/configuration-v2/config-overview?version=v25.1) that simplifies the deployment of new {{ ydb-short-name }} clusters and further work with them. [Comparison](./devops/configuration-management/compare-configs?version=v25.1) of configuration mechanisms V1 and V2.
* Added support for the parameterized [Decimal type](./yql/reference/types/primitive.md?version=v25.1#numeric).
* [Implemented](https://github.com/ydb-platform/ydb/issues/18017) client balancing of partitions when reading using the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) (like Kafka itself). Previously, balancing took place on the server. This mode is enabled by setting the `enable_kafka_native_balancing` flag in the cluster configuration.
* Added support for [auto-partitioning topics](./concepts/cdc.md?version=v25.1#topic-partitions) for row-oriented tables in CDC. This mode is enabled by setting the `enable_topic_autopartitioning_for_cdc` flag in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/8264) the ability to [alter the retention period of CDC topics](./concepts/cdc.md?version=v25.1#topic-settings) using the `ALTER TOPIC` statement.
* [Added support](https://github.com/ydb-platform/ydb/pull/7052) for [the DEBEZIUM_JSON format](./concepts/cdc.md?version=v25.1#debezium-json-record-structure) for CDC.
* Added the ability to create changefeeds to index tables.
* Added the ability to specify the number of replicas for a secondary index. This can be enabled by setting the flag in the cluster configuration.
* Added support for changefeeds in backup and restore operations. To use this functionality, you need to set the flags in the database or cluster configuration section.
* Added automatic cleanup of temporary directories and tables during export to S3. This can be enabled by setting the flag in the cluster configuration.
* Added automatic integrity check of backups during import, which prevents restoration from damaged backups and protects against data loss.
* Added the ability to create views that use UDFs in queries.
* Added system views with information about access rights settings, the history of overloaded partitions (enabled by setting the flag in the cluster configuration), and the history of string table partitions with broken locks (TLI).
* Added new parameters to the `CREATE USER` and `ALTER USER` statements:
  * the ability to set a password in encrypted form.
* `LOGIN` and `NOLOGIN` — user unlock and lock.
* Enhanced account security:
  * [Added](https://github.com/ydb-platform/ydb/pull/11963) [password complexity verification](./reference/configuration/?version=v25.1#password-complexity) for users;
  * [Implemented](https://github.com/ydb-platform/ydb/pull/12578) [automatic user lockout](./reference/configuration/?version=v25.1#account-lockout) after a specified number of failed password attempts;
  * [Added](https://github.com/ydb-platform/ydb/pull/12983) the ability for users to change their own password.
* [Implemented](https://github.com/ydb-platform/ydb/issues/9748) the ability to toggle functional flags at runtime {{ ydb-short-name }}. Flags that do not have the `(RequireRestart) = true` parameter specified in the [proto file](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/feature_flags.proto#L60) are applied without a cluster restart.
* Now the oldest (rather than the newest) locks [are converted to full-shard locks](https://github.com/ydb-platform/ydb/pull/11329) when the number of locks on shards is exceeded.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12567) preserving optimistic locks in memory during graceful datashard restarts, which should reduce `ABORTED` errors due to lock loss during table balancing.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12689) aborting volatile transactions with the `ABORTED` status during graceful datashard restarts.
* [Added](https://github.com/ydb-platform/ydb/pull/6342) the ability to remove `NOT NULL`constraints on a column in a table using the `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL` query.
* [Added](https://github.com/ydb-platform/ydb/pull/9168) a limit of 100,000 for the number of concurrent session-creation requests in the coordination service.
* [Increased](https://github.com/ydb-platform/ydb/pull/14219) the maximum [number of columns in the primary key](./concepts/limits-ydb.md?version=v25.1#schema-object) from 20 to 30.
* Improved diagnostics and introspection of memory errors ([#10419](https://github.com/ydb-platform/ydb/pull/10419), [#11968](https://github.com/ydb-platform/ydb/pull/11968)).
* **_(Experimental)_** [Added](https://github.com/ydb-platform/ydb/pull/14075) an experimental mode with strict access control checks. This mode is enabled by setting the following flags:
  * `enable_strict_acl_check` — do not allow granting rights to non-existent users and delete users with permissions;
  * `enable_strict_user_management` — enables strict rules for local user administration (i.e., only the cluster or database administrator can administer local users);
  * `enable_database_admin` — adds the role of database administrator.
* [Added](https://github.com/ydb-platform/ydb/pull/21119) the ability to use common data streaming tools — Kafka Connect, Confluent Schema Registry, Kafka Streams, Apache Flink, AKH via [Kafka API](./reference/kafka-api/index.md) when working with YDB Topics. Now YDB Topics Kafka API supports:
  * client-side consumer balancing — enabled by setting the `enable_kafka_native_balancing` flag in the [cluster configuration](./reference/configuration/feature_flags.md). [How consumer balancing works in Apache Kafka](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb). Now consumer balancing in YDB Topics Kafka API works the same way;
  * [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) — enabled by setting the `enable_topic_compactification_by_key` flag;
  * [transactions](https://www.confluent.io/blog/transactions-apache-kafka) — enabled by setting the `enable_kafka_transactions` flag.
* [Added](https://github.com/ydb-platform/ydb/pull/20982) a [new protocol](https://github.com/ydb-platform/ydb/issues/11064) to [Node Broker](./concepts/glossary.md#node-broker), which eliminates network traffic spikes on large clusters (over 1000 servers) associated with node information broadcasting.

#### Backward Incompatible Changes

* If you use queries that access named expressions as tables via [AS_TABLE](./yql/reference/syntax/select/from_as_table?version=v25.1), update [temporal over YDB](https://github.com/yandex/temporal-over-ydb) to version [v1.23.0-ydb-compat](https://github.com/yandex/temporal-over-ydb/releases/tag/v1.23.0-ydb-compat) before updating YDB to the current version to avoid query execution errors.

#### YDB UI

* The query editor [has been updated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1974) with support for partial result loading — results display starts as soon as the first chunk is received from the server, without waiting for the query to complete. This allows developers to get results faster.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/pull/1967) security: controls that users do not have permission to use are no longer displayed in the interface. Users will no longer encounter "Access Denied" errors.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1981) tablet ID search to the "Tablets" tab.
* Added a hotkeys help that opens with the `⌘+K` key combination.
* Added an "Operations" tab to the database page, allowing you to view and cancel operations.
* Updated the cluster monitoring dashboard and added the ability to collapse it.
* Added case-sensitive search support in the hierarchical JSON display tool.
* Added YDB SDK code snippets for connecting to the selected database on the top panel after selecting a database, which speeds up the development process.
* Fixed row sorting on the "Queries" tab.
* Removed unnecessary confirmation requests when closing the browser tab in the query editor — confirmation is now only requested when necessary.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17839) [an issue](https://github.com/ydb-platform/ydb/issues/15230) where not all tablets were displayed on the "Tablets" tab in the diagnostics section.
* Fixed [an issue](https://github.com/ydb-platform/ydb/issues/18735) where the "Storage" tab in the database diagnostics section displayed nodes other than storage nodes.
* Fixed [a serialization issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/2164) that caused an error when opening query execution statistics.
* Changed the logic for nodes transitioning to a critical state — a CPU pool that is 75-99% full now triggers a warning rather than a critical state.

#### Performance

* [Added](https://github.com/ydb-platform/ydb/pull/6509) support for [constant folding](https://ru.wikipedia.org/wiki/%D0%A1%D0%B2%D1%91%D1%80%D1%82%D0%BA%D0%B0_%D0%BA%D0%BE%D0%BD%D1%81%D1%82%D0%B0%D0%BD%D1%82) in the query optimizer by default, which improves query performance by evaluating constant expressions at compile time.
* [Added](https://github.com/ydb-platform/ydb/issues/6512) a granular timecast protocol for distributed transactions, which reduces the time required to complete distributed transactions (slowing down one shard will no longer slow down all others).
* [Implemented](https://github.com/ydb-platform/ydb/issues/11561) saving the state of datashards in memory during restarts, which helps preserve locks and increase the likelihood of successful transaction completion. This reduces the time required for long transactions by decreasing the number of retries.
* [Implemented](https://github.com/ydb-platform/ydb/pull/15255) pipeline processing of internal transactions in [Node Broker](./concepts/glossary?version=v25.1#node-broker), which speeds up the launch of dynamic nodes in the cluster {{ ydb-short-name }}.
* [Improved](https://github.com/ydb-platform/ydb/pull/15607) Node Broker's resilience to high load from cluster nodes.
* [Enabled](https://github.com/ydb-platform/ydb/pull/19440) evictable B-Tree indexes by default instead of non-evictable SST indexes, which reduces memory usage for storing "cold" data.
* [Optimized](https://github.com/ydb-platform/ydb/pull/15264) memory usage by storage nodes.
* [Reduced](https://github.com/ydb-platform/ydb/pull/10969) Hive startup time by 30%.
* [Optimized](https://github.com/ydb-platform/ydb/pull/6561) the replication process in the distributed storage.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9491) the size of the header for large binary objects in VDisk.
* [Reduced](https://github.com/ydb-platform/ydb/pull/15517) memory usage by cleaning allocator pages.
* [Optimized](https://github.com/ydb-platform/ydb/pull/20197) processing of empty inputs during JOIN operations.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/9707) an error in [Interconnect](./concepts/glossary.md?version=v25.1#actor-system-interconnect) configuration that caused performance degradation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13993) the "Out of memory" error when deleting very large tables by limiting the number of tablets processing the operation simultaneously.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9848) an issue that caused duplicate entries for the same database node in the system tablet configuration.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11059) an issue causing long data read times (seconds) during frequent table resharding operations.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9723) an error reading from asynchronous replicas that caused failures.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9507) rare freezes during the initial scan of [CDC](./dev/cdc.md?version=v25.1).
* [Fixed](https://github.com/ydb-platform/ydb/pull/11483) handling of incomplete schema transactions in datashards during system restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10460) an issue causing inconsistent reads from a topic when explicitly confirming a message read within a transaction. Now, attempting to confirm a message will result in an error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12220) an issue where topic auto-partitioning worked incorrectly within a transaction.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12905) transaction freezes when working with topics during tablet restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13910) the "Key is out of range" error when importing from S3-compatible storage.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13741) incorrect detection of the end of the metadata field in the cluster configuration.
* [Improved](https://github.com/ydb-platform/ydb/pull/16420) the secondary index build process: the system now retries on certain errors instead of interrupting the build.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16635) an error executing the `RETURNING` expression in `INSERT INTO` and `UPSERT INTO` queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16269) an issue causing "Drop Tablet" operations in PQ tablets to hang, especially during Interconnect delays.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16194) an error occurring during [VDisk compaction](./concepts/glossary.md?version=v25.1#compaction).
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) an issue where long topic-reading sessions ended with "too big inflight" errors.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15515) a freeze when reading a topic if at least one partition had no incoming data but was being read by multiple consumers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18614) a rare issue with PQ tablet restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18378) an issue where Hive subscribers started in data centers without running database nodes after a cluster version update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/19057) an error `Failed to set up listener on port 9092 errno# 98 (Address already in use)` that occurred during a version update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18905) an error that led to a segmentation fault when a healthcheck request and a cluster node disable request were executed simultaneously.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18899) a failure in [string table partitioning](./concepts/datamodel/table.md?version=v25.1#partitioning_row_table) when selecting a split key from access samples with mixed operations (full key and key prefix, e.g., exact or range reads).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18647) [an issue](https://github.com/ydb-platform/ydb/issues/17885) where the index type was incorrectly identified as `GLOBAL SYNC` even when `UNIQUE` was explicitly specified in the query.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16797) an issue where topic auto-partitioning did not work when the `max_active_partition` parameter was set using `ALTER TOPIC`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18938) an issue where `ydb scheme describe` returned columns in a different order than they were defined when the table was created.
* [Added](https://github.com/ydb-platform/ydb/pull/21918) support for a new type of change record in asynchronous replication — `reset` records (in addition to `update` and `erase` records).
* [Fixed](https://github.com/ydb-platform/ydb/pull/21836) [an issue](https://github.com/ydb-platform/ydb/issues/21814) where a replication instance with an unspecified `COMMIT_INTERVAL` parameter caused the process to crash.
* [Fixed](https://github.com/ydb-platform/ydb/pull/21652) rare errors when reading from a topic during partition balancing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22455) an issue where dedicated database deletion left system tablets undeleted.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22203) an issue where tablets froze due to insufficient node memory. Now, tablets automatically restart when sufficient resources become available on any node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/24278) an issue where only the first message from a batch was saved when writing Kafka messages, and all other messages in the batch were ignored.

## Version 24.4 {#24-4}

### Version 24.4.4.20 {#24-4-4-20}

Release date: November 1, 2025.

#### New Features

* Views are now supported in backup and restore operations [. To use this feature, set the `enable_view_export` flag in the `feature_flags` section of the `enable_view_export` database [ or [ cluster ](./devops/configuration-management/configuration-v1/static-config.md) configuration.
* Additional identifiers — the object path ID (`PathId`) and tablet ID (`TabletId`) — are now included in [Transaction locks invalidated](./troubleshooting/performance/queries/transaction-lock-invalidation) error messages when the table cannot be identified (Unknown table).

### Version 24.4.4.15 {#24-4-4-15}

Release date: September 19, 2025.

#### Performance

* Columns in the `ORDER BY` statement are now considered by the optimizer when automatically selecting a secondary index. This optimization applies only to queries that reference a single table and do not include `JOIN` operations with other tables.
### Version 24.4.4.13 {#24-4-4-13}

Release date: July 29, 2025.

#### New Features

* [Added](https://github.com/ydb-platform/ydb/pull/13251) support for restart without downtime in [a minimal fault-tolerant configuration of a cluster](./concepts/topology.md#reduced) that uses the three-node variant of `mirror-3-dc`.
* [Added](https://github.com/ydb-platform/ydb/pull/13220) new UDF Roaring Bitmap functions: AndNotWithBinary, FromUint32List, RunOptimize.
* Added the ability to register a [database node](./concepts/glossary.md#database-node) using a certificate. In the [Node Broker](./concepts/glossary.md#node-broker) the flag `AuthorizeByCertificate` has been added to enable certificate-based registration.
* [Added](https://github.com/ydb-platform/ydb/pull/11775) priorities for authentication ticket through a [third-party IAM provider](./security/authentication.md#iam), with the highest priority given to requests from new users. Tickets in the cache update their information with a lower priority.
* Added the ability to [read and write to a topic](./reference/kafka-api/examples.md#kafka-api-usage-examples) using the Kafka API without authentication.
* Enabled by default:
  * support for [views](./concepts/datamodel/view.md)
  * [auto-partitioning mode](./concepts/datamodel/topic.md#autopartitioning) for topics
  * [transactions involving topics and row-oriented tables simultaneously](./concepts/transactions.md#topic-table-transactions)
  * [volatile distributed transactions](./contributor/datashard-distributed-txs.md#volatile-transactions)

#### Performance

* [Improved](https://github.com/ydb-platform/ydb/pull/12747) tablet startup time on large clusters: 210 ms → 125 ms (SSD), 260 ms → 165 ms (HDD).
* [Limited](https://github.com/ydb-platform/ydb/pull/17755) the number of internal inflight configuration updates.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18289) memory consumption by PQ tablets.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18473) CPU consumption of Scheme shard and reduced query latencies by checking operation count limits before performing tablet split and merge operations.
* Automated secondary index selection is now enabled by default.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/12221) an issue where reading small messages from a topic in small chunks significantly increased CPU load, which could lead to delays in reading and writing to the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13222) an issue with restoring from a backup that was created during an automatic table split.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12601) an issue with Uuid serialization for [CDC](./concepts/cdc.md).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12804) an issue where reading from a follower of tablets sometimes caused crashes during automatic table splits.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12807) an issue where the [coordination node](./concepts/datamodel/coordination-node.md) successfully registered proxy servers despite a connection loss.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11593) an issue that occurred when opening the Embedded UI tab with information about [distributed storage groups](./concepts/glossary.md#storage-group).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12448) an issue where the Health Check did not report time synchronization issues.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17123) a rare issue of client applications hanging during transaction commit where deleting partition had been done before write quota update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17312) an error in copying tables with Decimal type, which caused failures when rolling back to a previous version.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17519) an [issue](https://github.com/ydb-platform/ydb/issues/17499) where a commit without confirmation of writing to a topic led to the blocking of the current and subsequent transactions with topics.
* Fixed transaction hanging when working with topics during tablet [restart](https://github.com/ydb-platform/ydb/issues/17843) or [deletion](https://github.com/ydb-platform/ydb/issues/17915).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18114) [issues](https://github.com/ydb-platform/ydb/issues/18071) with reading messages larger than 6Mb via [Kafka API](./reference/kafka-api).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18319) memory leak during writing to the [topic](./concepts/glossary#topic).
* Fixed errors in processing [nullable columns](https://github.com/ydb-platform/ydb/issues/15701) and [columns with UUID type](https://github.com/ydb-platform/ydb/issues/15697) in row tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/14811) an error that led to a significant decrease in reading speed from [tablet followers](./concepts/glossary.md#tablet-follower).
* [Fixed](https://github.com/ydb-platform/ydb/pull/14516) an error that caused volatile distributed transactions to sometimes wait for confirmations until the next reboot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15077) a rare assertion failure (server process crash) when followers attached to leaders with an inconsistent snapshot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15074) a rare datashard crash when a dropped table shard is restarted with uncommitted persistent changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15194) an error that could disrupt the order of message processing in a topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15308) a rare error that could stop reading from a topic partition.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15160) an issue where a transaction could hang if a user performed a control plane operation on a topic (for example, adding partitions or a consumer) while the PQ tablet is moving to another node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) a memory leak issue with the UserInfo counter value. Because of the memory leak, a reading session would eventually return a "too big in flight" error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15467) a proxy crash due to duplicate topics in a request.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15933) a rare bug where a user could write to a topic without any account quota being applied or consumed.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16288) an issue where topic deletion returned "OK" while the topic tablets persisted in a functional state. To remove such tablets, follow the instructions from the [pull request](https://github.com/ydb-platform/ydb/pull/16288).
* [Fixed](https://github.com/ydb-platform/ydb/pull/16418) a rare issue that prevented the restoration of a backup for a large secondary indexed table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15862) an issue that caused errors when inserting data using `UPSERT` into row-oriented tables with default values.
* [Resolved](https://github.com/ydb-platform/ydb/pull/15334) a bug that caused failures when executing queries to tables with secondary indexes that returned result lists using the RETURNING * expression.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0469) an issue where, upon receiving an error, the export operation to S3 does not fail but retries the write attempt.

## Version 24.3 {#24-3}

### Version 24.3.13.11 {#24-3-13-11}

Release date: March 6, 2025.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/13501) an uncommitted changes leak and cleaned them up on startup.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13948) consistency issues related to caching deleted ranges.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15182) the issue of caching negative responses to authentication requests from an LDAP-compatible user and group directory for too long.

### Version 24.3.13.10 {#24-3-13-10}

Release date: December 24, 2024.

#### New Features

* Introduced [query tracing](./reference/observability/tracing/setup), a tool that allows you to view the detailed path of a request through a distributed system.
* Added support for [asynchronous replication](./concepts/async-replication), that allows synchronizing data between YDB databases in near real time. It can also be used for data migration between databases with minimal downtime for applications interacting with these databases.
* Added support for [views](./concepts/datamodel/view), which can be enabled by the cluster administrator using the `enable_views` setting in [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* Extended [federated query](./concepts/query_execution/federated_query/) capabilities to support new external data sources: MySQL, Microsoft SQL Server, and Greenplum.
* Published [documentation](./devops/deployment-options/manual/federated-queries/connector-deployment.md) on deploying YDB with [federated query](./concepts/query_execution/federated_query/) New Features (manual setup).
* Added a new launch parameter `FQ_CONNECTOR_ENDPOINT` for YDB Docker containers that specifies an external data source connector address. Added support for TLS encryption for connections to the connector and the ability to expose the connector service port locally on the same host as the dynamic YDB node.
* Added an [auto-partitioning mode](./concepts/datamodel/topic.md#autopartitioning) for topics, where partitions can dynamically split based on load while preserving message read-order and exactly-once guarantees. The mode can be enabled by the cluster administrator using the settings `enable_topic_split_merge` and `enable_pqconfig_transactions_at_scheme_shard` in [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* Added support for transactions involving [topics](./concepts/datamodel/topic.md) and row-based tables, enabling transactional data transfer between tables and topics, or between topics, ensuring no data loss or duplication. Transactions can be enabled by the cluster administrator using the settings `enable_topic_service_tx` and `enable_pqconfig_transactions_at_scheme_shard` in [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* [Implemented](https://github.com/ydb-platform/ydb/pull/7150) [Change Data Capture (CDC)](./concepts/cdc) for synchronous secondary indexes.
* Added support for changing record retention periods in [CDC](./concepts/cdc) topics.
* Added support for auto-increment columns as part of a table's primary key.
* Added audit logging for user login events in YDB, session termination events in the user interface, and backup/restore operations.
* Added a system view with information about sessions installed from the database using a query.
* Added support literal default values for row-oriented tables. When inserting a new row in YDB Query default values will be assigned to the column if specified.
* Added the `version()` [built-in function](./yql/reference/builtins/basic.md#version).
* Added support for `RETURNING` clause in queries.
* [Added](https://github.com/ydb-platform/ydb/pull/8708) start/end times and authors in the metadata for backup/restore operations from S3-compatible storage.
* Added support for backup/restore of ACL for tables from/to S3-compatible storage.
* Included paths and decompression methods in query plans for reading from S3.
* Added new parsing options for timestamp/datetime fields when reading data from S3.
* Added support for the `Decimal` type in [partitioning keys](./dev/primary-key/column-oriented#klyuch-particionirovaniya).
* Improved diagnostics for storage issues in HealthCheck.
* **_(Experimental)_** Added a [cost-based optimizer](./concepts/optimizer#cost-based-query-optimizer) for complex queries, involving [column-oriented tables](./concepts/glossary#column-oriented-table). The cost-based optimizer considers a large number of alternative execution plans for each query and selects the best one based on the cost estimate for each option.  Currently, this optimizer only works with plans that contain [JOIN](./yql/reference/syntax/join) operations.
* **_(Experimental)_** Initial version of the workload manager was implemented. It allows to create resource pools with CPU, memory and active queries count limits. Resource classifiers were implemented to assign queries to specific resource pool.
* **_(Experimental)_** Implemented [automatic index selection](./dev/secondary-indexes#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) for queries, which can be enabled via the `index_auto_choose_mode setting` in `table_service_config` in [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).

#### YDB UI

* Added support for creating and [viewing information on](https://github.com/ydb-platform/ydb-embedded-ui/issues/782) asynchronous replication instances.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/929) an indicator for auto-increment columns.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1438) a tab with information about [tablets](./concepts/glossary#tablet).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1289) a tab with details about [distributed storage groups](./concepts/glossary#storage-group).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1218) a setting to trace all queries and display tracing results.
* Enhanced the PDisk page with [attributes](https://github.com/ydb-platform/ydb-embedded-ui/pull/1069), disk space consumption details, and a button to initiate [disk decommissioning](./devops/deployment-options/manual/decommissioning.md).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1313) information about currently running queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1291) a row limit setting for query editor output and a notification when results exceed the limit.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1049) a tab to display top CPU-consuming queries over the last hour.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1127) a control to search the history and saved queries pages.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1117) the ability to cancel query execution.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/944) a shortcut to save queries in the editor.
* [Separated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1422) donor disks from other disks in the UI.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1154) support for InterruptInheritance ACL and improved visualization of active ACLs.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/889) a display of the current UI version.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1229) a tab with information about the status of settings for enabling experimental New Features.

#### Performance

* [Accelerated](https://github.com/ydb-platform/ydb/pull/7589) recovery of tables with secondary indexes from backups up to 20% according to our tests.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9721) Interconnect throughput.
* Improved the performance of CDC topics with thousands of partitions.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1291) the option to set a limit on the number of rows in the query editor output and display a notification if the query results exceed the limit.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1049) a display of a list of queries with the highest CPU consumption over the last hour.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1127) search functionality to the query history and saved queries pages.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1117) the ability to abort query execution.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/944) the ability to save a query from the editor using keyboard shortcuts.
* [Separated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1422) the display of disks from donor disks.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1154) support for InterruptInheritance ACL and improved the display of active ACLs.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/889) the display of the current user interface version.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1229) information about the state of experimental functionality enablement settings.

#### Performance

* [Accelerated](https://github.com/ydb-platform/ydb/pull/7589) the restoration from backup of tables with secondary indexes by up to 20% according to our tests.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9721) the throughput of Interconnect.
* Improved the performance of CDC topics containing thousands of partitions.
* Made a number of improvements to the Hive tablet balancing algorithm.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/6850) an issue that caused databases with a large number of tables or partitions to become non-functional during restoration from a backup. Now, if database size limits are exceeded, the restoration operation will fail, but the database will remain operational.
* [Implemented](https://github.com/ydb-platform/ydb/pull/11532) a mechanism to forcibly trigger background [compaction](./concepts/glossary#compaction) when discrepancies between the data schema and stored data are detected in [DataShard](./concepts/glossary#data-shard). This resolves a rare issue with delays in schema changes.
* [Resolved](https://github.com/ydb-platform/ydb/pull/10447) duplication of authentication tickets, which led to an increased number of requests to authentication providers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9377) an invariant violation issue during the initial scan of CDC, leading to an abnormal termination of the `ydbd` server process.
* [Prohibited](https://github.com/ydb-platform/ydb/pull/9446) schema changes for backup tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9509) an issue with an initial scan freezing during CDC when the table is frequently updated.
* [Excluded](https://github.com/ydb-platform/ydb/pull/9934) deleted indexes from the count against the [maximum index limit](https://ydb.tech/docs/ru/concepts/limits-ydb#schema-object).
* [Fixed](https://github.com/ydb-platform/ydb/pull/8847) [a bug](https://github.com/ydb-platform/ydb/issues/6985) in the display of the scheduled execution time for a set of transactions (planned step).
* [Fixed](https://github.com/ydb-platform/ydb/pull/9161) [a problem](https://github.com/ydb-platform/ydb/issues/8942) with interruptions in blue–green deployment in large clusters caused by frequent updates to the node list.
* [Resolved](https://github.com/ydb-platform/ydb/pull/8925) a rare issue that caused transaction order violations.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9841) [an issue](https://github.com/ydb-platform/ydb/issues/9797) in the EvWrite API that resulted in incorrect memory deallocation.
* [Resolved](https://github.com/ydb-platform/ydb/pull/10698) [a problem](https://github.com/ydb-platform/ydb/issues/10674) with volatile transactions hanging after a restart.
* Fixed a bug in the CDC, which in some cases leads to increased CPU consumption, up to a core per CDC partition.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/11061) read delays occurring during and after the splitting of certain partitions.
* Fixed issues when reading data from S3.
* [Corrected](https://github.com/ydb-platform/ydb/pull/4793) the calculation of the AWS signature for S3 requests.
* Resolved false positives in the HealthCheck system during database backups involving a large number of shards.
* [Removed](https://github.com/ydb-platform/ydb/pull/11901) the restriction on writing values greater than 127 to the Uint8 type.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12915) an issue with restoring from a backup stored in S3 with path-style addressing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12018) an issue with [«frozen» locks](./contributor/datashard-locks-and-change-visibility#vzaimodejstvie-s-raspredelyonnymi-tranzakciyami), which could be caused by bulk operations (e.g., TTL-based deletions).
* [Fixed](https://github.com/ydb-platform/ydb/pull/11658) a rare issue that caused errors during read queries.

## Version 24.2 {#24-2}

### Version 24.2.7.1 {#24-2-7-1}

Release date: August 20, 2024.

### New Features

* Added the ability to [set priorities](./devops/deployment-options/manual/maintenance.md#rolling-restart) for maintenance tasks in the [cluster management system](./concepts/glossary.md#cms).
* Added a setting to enable [stable names](reference/configuration/node_broker_config.md#node-broker-config) for cluster nodes within a tenant.
* Enabled retrieval of nested groups from the [LDAP server](./security/authentication.md#ldap), improved host parsing in the [LDAP-configuration](reference/configuration/auth_config.md#ldap-auth-config), and added an option to disable built-in authentication via login and password.
* Added support for authenticating [dynamic nodes](./concepts/glossary.md#dynamic) using SSL-certificates.
* Implemented the removal of inactive nodes from [Hive](./concepts/glossary.md#hive) without a restart.
* Improved management of inflight pings during Hive restarts in large clusters.
* [Changed](https://github.com/ydb-platform/ydb/pull/6381) the order of establishing connections with nodes during Hive restarts.

### YDB UI

* [Added](https://github.com/ydb-platform/ydb/pull/7485) the option to set a TTL for user sessions in the configuration file.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1028) an option to sort the list of queries by `CPUTime`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7779) precision loss when working with `double`, `float`.
* Added support [for creating directories in the UI](https://github.com/ydb-platform/ydb-embedded-ui/issues/958).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/976) the ability to set an interval for background data updates on all pages.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/issues/955) the display of ACLs.
* Enabled autocomplete in the queries editor by default.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/834) support for views.

### Bug fixes

* Added a check on the size of the local transaction prior to its commit to fix [errors](https://github.com/ydb-platform/ydb/issues/6677) in scheme shard operations when exporting/backing up large databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7709) [an issue](https://github.com/ydb-platform/ydb/issues/7674) with duplicate results in SELECT queries when reducing quotas in [DataShard](./concepts/glossary#data-shard).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6461) [errors](https://github.com/ydb-platform/ydb/issues/6220) occurring during [coordinator](./concepts/glossary#coordinator) state changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5992) issues during the initial CDC scan.
* [Resolved](./dev/cdc) race conditions in asynchronous change delivery (asynchronous indexes, CDC).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6615) a rare issue that led to the loss of established locks and the successful confirmation of transactions that should have failed with a «Transaction Locks Invalidated» error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5993) a rare error that could result in a violation of data integrity guarantees during concurrent read and write operations on a specific key.
* [Fixed](./concepts/ttl) an issue causing read replicas to stop processing requests.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5760) a rare error that could cause abnormal termination of database processes if there were uncommitted transactions on a table during its renaming.
* [Fixed](./concepts/glossary#cms) an error in determining the status of a static group, where it was not marked as non-working when it should have been.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6008) an error involving partial commits of a distributed transaction with uncommitted changes, caused by certain race conditions with restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6445) anomalies related to reading outdated data, detected using Jepsen.

## Version 24.1 {#24-1}

### Version 24.1.18.1 {#24-1-18-1}

Release date: July 31, 2024.

### New Features

* Implemented [Knn UDF](./yql/reference/udf/list/knn.md) for precise nearest vector search.
* Developed a gRPC Query service, enabling the execution of all types of queries (DML, DDL) and retrieval of unlimited amounts of data.
* Implemented [integration with the LDAP protocol](./security/authentication.md) and the ability to retrieve a list of groups from external LDAP directories.

### Embedded UI

* The database information tab now includes a resource consumption diagnostic dashboard, which allows users to assess the current consumption of key resources: processor cores, RAM, and distributed storage space.
* Added charts for monitoring the key performance indicators of the {{ ydb-short-name }} cluster.

### Performance

* [Optimized](https://github.com/ydb-platform/ydb/pull/1837) session timeouts for the coordination service between server and client. Previously, the timeout was 5 seconds, which could result in a 10-second delay in identifying an unresponsive client and releasing its resources. In the new version, the check interval depends on the session's wait time, allowing for faster responses during leader changes or when acquiring distributed locks.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2391) CPU consumption by [SchemeShard](./concepts/glossary.md#scheme-shard) replicas, particularly when handling rapid updates for tables with a large number of partitions.

### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/3917) a possible queue overflow error. [Change Data Capture](./dev/cdc.md) now reserves the change queue capacity during the initial scan.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4597) a potential deadlock between receiving and sending CDC records.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2056) an issue causing the loss of the mediator task queue during mediator reconnection. This fix allows processing of the mediator task queue during resynchronization.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2624) a rare issue where, with volatile transactions enabled, a successful transaction confirmation result could be returned before the transaction was fully committed. Volatile transactions remain disabled by default and are still under development.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2839) a rare error that led to the loss of established locks and the successful confirmation of transactions that should have failed with a «Transaction Locks Invalidated» error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3074) a rare error that could result in a violation of data integrity guarantees during concurrent read and write operations on a specific key.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4343) an issue causing read replicas to stop processing requests.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4979) a rare error that could cause abnormal termination of database processes if there were uncommitted transactions on a table during its renaming.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3632) an error in determining the status of a static group, where it was not marked as non-working when it should have been.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error involving partial commits of a distributed transaction with uncommitted changes, caused by certain race conditions with restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2374) anomalies related to reading outdated data, detected using Jepsen.

## Version 23.4 {#23-4}

### Version 23.4.11.1 {#23-4-11-1}

Release date: May 14, 2024.

### Performance

* [Fixed](https://github.com/ydb-platform/ydb/pull/3638) an issue of increased CPU consumption by a topic actor `PERSQUEUE_PARTITION_ACTOR`.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2083) resource usage by SchemeBoard replicas. The greatest effect is noticeable when modifying the metadata of tables with a large number of partitions.

### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an issue of possible partial commit of accumulated changes when using persistent distributed transactions. This error occurs in an extremely rare combination of events, including restarting tablets that service the table partitions involved in the transaction.
* [Resolved](https://github.com/ydb-platform/ydb/pull/3165) a race condition between the table merge and garbage collection processes, which could result in garbage collection ending with an invariant violation error, leading to an abnormal termination of the `ydbd` server process.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2696) an issue in Blob Storage where information about changes in the storage group composition might not be delivered timely to individual cluster nodes. In rare cases, this could block read and write operations on the affected group, requiring manual administrator intervention.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3002) an error in Blob Storage that could prevent data storage nodes from starting with a correct configuration. The error occurred in systems with the experimental "blob depot" feature explicitly enabled (this feature is disabled by default).
* [Fixed](https://github.com/ydb-platform/ydb/pull/2475) an error that occurred in some situations when writing to a topic with an empty `producer_id` when deduplication was disabled. It could lead to an abnormal termination of the server process `ydbd`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2651) an issue that caused the `ydbd` process to crash due to an erroneous write session state in the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3587) a display error of the metric for the number of partitions in the topic; it previously showed an incorrect value.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2126) memory leaks that occurred when copying topic data between clusters {{ ydb-short-name }}. They could lead to server processes `ydbd` terminating due to running out of available RAM.

## Version 23.3 {#23-3}

### Version 23.3.25.2 {#23-3-25-2}

Release date: October 12, 2023.

### Functionality

* Visibility of own changes within transactions has been implemented. Previously, when trying to read data already modified in the current transaction, the query would result in an error. This required ordering reads and writes within the transaction. With the introduction of visibility of own changes, these restrictions are lifted, and queries can read rows modified in the current transaction.
* Support for [columnar tables](concepts/datamodel/table.md#column-tables) has been added. Columnar tables are well-suited for working with analytical queries (Online Analytical Processing) because only the columns directly involved in the query are read when the query is executed. YDB columnar tables allow creating analytical reports with performance comparable to specialized analytical DBMS.
* Support for [Kafka API for topics](reference/kafka-api/index.md) has been added. Now YDB topics can be worked with via Kafka-compatible API, intended for migrating existing applications. Support for the Kafka protocol version 3.4.0 is provided.
* The ability to [write to a topic without deduplication](concepts/datamodel/topic.md#no-dedup) has been added. This type of writing is well-suited for cases where the order of message processing is not critical. Writing without deduplication is faster and consumes fewer server resources, but message ordering and deduplication on the server does not occur.
* YQL has been enhanced with the ability to [create](yql/reference/syntax/create-topic.md), [modify](yql/reference/syntax/alter-topic.md), and [delete](yql/reference/syntax/drop-topic.md) topics.
* The ability to grant and revoke access rights using YQL commands [GRANT](yql/reference/syntax/grant.md) and [REVOKE](yql/reference/syntax/revoke.md) has been added.
* The ability to log DML operations in the audit log has been added.
* **_(Experimentally)_** When writing messages to a topic, it is now possible to pass metadata. To enable this functionality, add `enable_topic_message_meta: true` to the [configuration file](reference/configuration/index.md).
* **_(Experimentally)_** The ability to [read from topics](reference/ydb-sdk/topic.md#read-tx) and write to a table within the same transaction has been added. This new feature simplifies the scenario of transferring data from a topic to a table. To enable it, add `enable_topic_service_tx: true` to the configuration file.
* **_(Experimentally)_** Support for PostgreSQL compatibility has been added. The new mechanism allows executing SQL queries in the PostgreSQL dialect on the YDB infrastructure using the PostgreSQL network protocol. You can use familiar PostgreSQL tools, such as psql and drivers (pq for Golang and psycopg2 for Python), and develop queries using the familiar PostgreSQL syntax with the horizontal scalability and fault tolerance of YDB.
* **_(Experimentally)_** Support for [federated queries](concepts/query_execution/federated_query/index.md) has been added. It allows retrieving information from various data sources without moving them to YDB. Interaction with ClickHouse, PostgreSQL, and S3 is supported via YQL queries without duplicating data between systems.

### Built-in UI

* A new option `PostgreSQL` has been added to the request type selector settings, which is available when the `Enable additional query modes` parameter is enabled. The request history now also takes into account the syntax used when executing the request.
* The YQL query template for creating a table has been updated, and a description of the available parameters has been added.
* Sorting and filtering for Storage and Nodes tables have been moved to the server. You need to enable the `Offload tables filters and sorting to backend` parameter in the experiments section to use this functionality.
* Buttons for creating, modifying, and deleting [topics](concepts/datamodel/topic.md) have been added to the context menu.
* Sorting by severity for all issues in the tree has been added in `Healthcheck`.

### Performance

* Iterator reads have been implemented. The new functionality allows separating reads and computations. Iterator reads enable data shards to increase the throughput of read queries.
* YDB topic writing performance has been optimized.
* Tablet balancing has been improved when nodes are overloaded.

### Bug fixes

* Fixed an error of possible blocking of snapshots by reading iterators that coordinators are not aware of.
* Fixed a memory leak when closing a connection in the kafka proxy.
* Fixed an error where snapshots taken via reading iterators may not be restored on restarts.
* Fixed an incorrect residual predicate for the condition `IS NULL` on the column.
* Fixed the triggered check `VERIFY failed: SendResult(): requirement ChunksLimiter.Take(sendBytes) failed`.
* Fixed `ALTER TABLE` for columnar tables by `TTL`.
* Implemented `FeatureFlag`, which allows enabling/disabling work with `CS` and `DS`.
* Fixed the discrepancy in coordinator time between 23-2 and 23-3 by 50 ms.
* Fixed an error where the handle `storage` returned extra groups when the parameter `node_id` was in `viewer backend`.
* Added a `usage` filter to `/storage` in `viewer backend`.
* Fixed an error in Storage v2 where an incorrect number was returned in `Degraded`.
* Fixed the cancellation of session subscriptions in iterator reads on tablet restart.
* Fixed an error where during a rolling restart, when going through the balancer, `healthcheck` alerts about storage flicker.
* Updated `cpu usage` metrics in ydb.
* Fixed ignoring `NULL` when specifying `NOT NULL` in the table schema.
* Implemented the output of records about `DDL` operations to the general log.
* Implemented a ban for the `ydb table attribute add/drop` command to work with any objects other than tables.
* Disabled `CloseOnIdle` for `interconnect`.
* Fixed the doubling of reading speed in the UI.
* Fixed an error where data could be lost on `block-4-2`.
* Added a topic name check.
* Fixed a possible `deadlock` in the actor system.
* Fixed the test `KqpScanArrowInChanels::AllTypesColumns`.
* Fixed the test `KqpScan::SqlInParameter`.
* Fixed concurrency issues for OLAP queries.
* Fixed the insertion of `ClickBench parquet`.
* Added a missing call to `CheckChangesQueueOverflow` in the general `CheckDataTxReject`.
* Fixed the error of returning an empty status when calling `ReadRows API`.
* Fixed incorrect retry of export in the final stage.
* Fixed the issue with an infinite quota on the number of records in the CDC topic.
* Fixed an error importing columns `string` and `parquet` into the `string` OLAP column.
* Fixed the crash of `KqpOlapTypes.Timestamp` under tsan.
* Fixed the crash in `viewer backend` when trying to execute a query to the database due to version incompatibility.
* Fixed an error where `viewer` did not return a response from `healthcheck` due to a timeout.
* Fixed an error where an incorrect value of `ExpectedSerial` could be stored in Pdisks.
* Fixed an error where database nodes crashed due to `segfault` in the S3 actor.
* Fixed a race in `ThreadSanitizer: data race KqpService::ToDictCache-UseCache`.
* Fixed a race in `GetNextReadId`.
* Fixed the overestimation of the `SELECT COUNT(*)` result immediately after import.
* Fixed an error where `TEvScan` could return an empty dataset in the case of a dateshard split.
* Added a separate issue/error code in case of exhaustion of available space.
* Fixed error `GRPC_LIBRARY Assertion failed`.
* Fixed an error where scanning queries with a secondary index returned an empty result.
* Fixed validation of `CommitOffset` in `TopicAPI`.
* Reduced consumption of `shared cache` when approaching OOM.
* Merged the logic of schedulers from `data executer` and `scan executer` into one class.
* Added handles `discovery` and `proxy` to the execution process of `query` in `viewer backend`.
* Fixed an error where the handle `/cluster` returns the name of the root domain of type `/ru` in `viewer backend`.
* Implemented a scheme for seamless tablet updates for `QueryService`.
* Fixed an error where `DELETE` returned data and did not delete it.
* Fixed an error in the operation of `DELETE ON` in `query service`.
* Fixed the unexpected disabling of batching in the default schema settings.
* Fixed the triggered check `VERIFY failed: MoveUserTable(): requirement move.ReMapIndexesSize() == newTableInfo->Indexes.size()`.
* Increased the default grpc-streaming timeout.
* Excluded unused messages and methods from `QueryService`.
* Added sorting by `Rack` in `/nodes` in `viewer backend`.
* Fixed an error where a query with sorting returns an error when decreasing.
* Fixed the interaction of `QP` with `NodeWhiteboard`.
* Removed support for old parameter formats.
* Fixed an error where `DefineBox` was not applied to disks with a static group.
* Fixed an error `SIGSEGV` in dynodes when importing `CSV` via `YDB CLI`.
* Fixed an error with a crash when processing `NGRpcService::TRefreshTokenImpl`.
* Implemented the `gossip` protocol for exchanging information about cluster resources.
* Fixed the error:

  ```text
  DeserializeValuePickleV1(): requirement data.GetTransportVersion() ==
  (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0 failed
  ```

* Implemented auto-increment columns.
* Use the status `UNAVAILABLE` instead of `GENERIC_ERROR` when identifying a shard error.
* Added support for `rope payload` in `TEvVGet`.
* Added ignoring of outdated events.
* Fixed the crash of write sessions on an invalid topic name.
* Fixed the error:

  ```text
  CheckExpected(): requirement newConstr failed, message: Rewrite error,
  missing Distinct((id)) constraint in node FlatMap
  ```

* Enabled `self heal` by default.
