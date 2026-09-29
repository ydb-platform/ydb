# Changelog {{ ydb-short-name }} Server

## Version 26.3 {#26-3}

### Release candidate 26.3.1.16 {#26-3-1-16-rc}

Release date: 18.09.26

#### Functionality

* [Export and import of backups, including S3-compatible storage, are available for columnar tables](./recipes/backup/backup-collections/exporting-to-external-storage.md?version=main).
* Columns of columnar tables support [dictionary encoding](./yql/reference/syntax/create_table/index.md?version=v26.3#encoding). Use `ENCODING(DICT)` for values with low cardinality.
* [Local min_max-indexes](./dev/min_max-skip-index.md?version=v26.3) are enabled for columnar tables. They skip data fragments outside the query range, reducing the amount of reads. Use `ADD INDEX ... LOCAL USING min_max` to apply to a column.
* Added [decommissioning of storage groups using virtual groups](./maintenance/manual/virtual_storage_groups_decommit.md?version=v26.3). Data is moved to virtual groups in the background, while applications continue reading and writing.
* Added [authentication via external OpenID Connect identity providers](./security/authentication.md?version=v26.3#external-idp). {{ ydb-short-name }} validates JWT tokens against the provider's JSON Web Key Set (JWKS) and periodically refreshes authentication data.
* Kafka API supports [mutual TLS authentication](./reference/kafka-api/auth.md?version=v26.3). The client certificate is mapped to a security identifier; SASL authentication is not required.
* Columnar table engine optimization: an updated compaction strategy is used for columnar tables, which organizes data more efficiently, and a new data merge strategy for reads, which speeds up queries on constantly changing data.
* Authentication/authorization subsystem optimization: batched authorization requests in AccessService are enabled by default, reducing overhead.
* System virtual attributes such as `__ydb_create_time`, `__ydb_write_time`, etc., as well as user attributes `__ydb_user_attributes`, became available to streaming YQL queries. [Link to functionality](./concepts/query_execution/topics.md?version=v26.3#system-metadata).
* Distributed Storage subsystem optimization: full VDisk synchronization became faster by removing processed SyncLog data.
* Metrics and statistics for monitoring and diagnosing transfers were added to `DescribeTransfer`.
* Added a configurable limit on the number of stored forced compaction operations. Completed and canceled operations can be automatically deleted after reaching the limit.
* Change data capture records can contain the [OpenTelemetry trace ID](./concepts/cdc.md?version=v26.3#record-structure) of the query that created the change.
* When [reading a topic from a timestamp](./reference/ydb-cli/topic-read.md?version=v26.3), messages with an earlier write time are filtered out, including those from the same blob as newer messages.

#### Disabled functionality

The functionality listed below is not enabled by default.

* For columnar tables, you can run forced compaction using `ALTER TABLE ... COMPACT`.
* Columnar and row-based tables have achieved parity in the set of YQL types (Interval, Uuid, DyNumber are supported).
* Added [hybrid search](./dev/hybrid-search.md?version=v26.3), combining full-text relevance and vector proximity into a ranked result.
* You can work with topics via the [Amazon SQS API](./reference/sqs-api/index.md?version=v26.3), using SQS-compatible clients to read and write messages.
* Added [JSON indexes](./dev/json-indexes.md?version=v26.3) to speed up queries with `JSON_EXISTS` and `JSON_VALUE`.
* Full-text indexes support [filter columns](./dev/fulltext-indexes.md?version=v26.3#filtered), allowing you to search within a logical table partition.
* Full-text indexes can be created for tables with [arbitrary primary key types](./dev/fulltext-indexes.md?version=v26.3#primary-key).

## Version 26.2 {#26-2}

### Version 26.2.1.14 {#26-2-1-14}

Release date: September 16, 2026.

#### Functionality

* [Full-text indexes](./dev/fulltext-indexes.md?version=v26.2) are enabled by default.
* [Streaming queries](./dev/streaming-query/index.md?version=v26.2) support reading from local topics, writing to local topics, reading local tables, and multiple statements `INSERT` in a single query.
* [Watermarks](./dev/streaming-query/watermarks.md?version=v26.2) are available in streaming queries.
* Added [Bloom indexes](./dev/bloom-skip-indexes.md?version=v26.2): Bloom and Bloom n-gram for columnar tables and prefix Bloom indexes for row-based tables.
* Configuring [column compression](./yql/reference/syntax/create_table/index.md?version=v26.2) in columnar tables is enabled by default.
* You can now configure the [parallelism level](./yql/reference/syntax/alter_table/indexes.md?version=v26.2) for building indexes.
* For row-based tables, the [`ALTER TABLE`](./yql/reference/syntax/alter_table/columns.md?version=v26.2) statements `ALTER COLUMN SET DEFAULT` and `ALTER COLUMN DROP DEFAULT` are available by default.
* For row-based tables, the YQL statement [`TRUNCATE TABLE`](./yql/reference/syntax/truncate-table.md?version=v26.2) is available by default.
* The YQL statement [`DISCARD SELECT`](./yql/reference/syntax/discard.md?version=v26.2) is available by default.
* QueryService supports returning query results in [Apache Arrow format](./reference/ydb-sdk/data-formats/format-arrow.md?version=v26.2); this feature is enabled by default.
* For row-based tables, forced [compaction](./yql/reference/syntax/alter_table/compact.md?version=v26.2) can now be triggered using `ALTER TABLE ... COMPACT`.
* Automatic storage balancing between groups and background verification of correct disk placement have been added.
* Operations for splitting and merging tables with a large number of partitions have been accelerated: SchemeShard now updates only the affected partitions instead of fully recalculating the list.
* [Audit logging](./security/audit-log.md?version=v26.2) for topic operations has been added.
* [Built-in minidump collection based on Google Breakpad](./devops/observability/minidumps.md?version=v26.2) has been added for Linux nodes.
* A subcommand [`ydb-dstool pdisk populate`](./reference/ydb-dstool/pdisk-populate.md?version=v26.2) has been added to reproduce PDisk load on another device.

#### Disabled functionality

The functionality is present in the kernel to allow rollback from the future 26-3 release, but is not enabled by default. It will be available by default in the next major release. It can also be enabled on some managed YDB services.

* Added support for [incremental backups](./concepts/datamodel/backup-collection.md?version=v26.2), which allow saving only changes relative to the previous backup in a collection.
* Supported [export and import of columnar tables](./concepts/query_execution/federated_query/import_and_export.md?version=v26.2) via S3-compatible storage.
* Added [export and import of row-based tables](./reference/ydb-cli/export-import/export-nfs.md?version=main) via the local file system, including file systems mounted over NFS.
* Added snapshot retention for long-running analytical queries to columnar tables, so that snapshot data is not deleted until the query completes.
* QueryService can notify the SDK about node or session shutdown, so that the client stops sending new requests there.
* For columnar tables, limits on the number and volume of small blobs at the database level have been added. When the hard limit is exceeded, new writes are rejected.
* Expressions for [watermarks](./dev/streaming-query/watermarks.md?version=v26.2) can be computed outside the context of an individual message.
* Added [min-max indexes](./yql/reference/syntax/create_table/min_max_index.md?version=main) for columnar tables.
* Added [dictionary encoding](./yql/reference/syntax/create_table/index.md?version=v26.2#encoding) of columns in columnar tables.
* Added online building of unique secondary indexes.
* For transactions between topics and tables, an optimized conflict check has been added.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/46747) incorrect results of some scan queries to columnar tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/50358) handling of corrupted Kafka requests, which could lead to excessive memory consumption or buffer overruns.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49929) hanging of reads from a topic after restarting the read balancer.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35470) race conditions in the server-side topic read session and in the [Topic SDK](https://github.com/ydb-platform/ydb/pull/42213).
* [Fixed](https://github.com/ydb-platform/ydb/pull/50897) crashes and [hangs](https://github.com/ydb-platform/ydb/pull/50621) of streaming requests when creating checkpoints.
* [Fixed](https://github.com/ydb-platform/ydb/pull/50379) race conditions when canceling and scheduling distributed transactions.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49469) handling of overly large blocks during encrypted export and [false data corruption error](https://github.com/ydb-platform/ydb/pull/48986) during encrypted restore.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49460) a race condition when collecting statistics by the command `ydb workload topic`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/48174) double freeing of memory when completing `DqHashCombine` with spilling.
* [Fixed](https://github.com/ydb-platform/ydb/pull/40912) loss of acknowledgments `ReadSet`, which could prevent a transaction with topics from completing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/40801) handling of the response `NODATA` in the KeyValue API: instead of crashing the process, an error is returned `NOT_FOUND` or `INTERNAL_ERROR`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41895) IAM authentication for external data sources in Generic Provider and [handling of errors returned by them](https://github.com/ydb-platform/ydb/pull/40761).
* [Fixed](https://github.com/ydb-platform/ydb/pull/41411) a memory leak when loading metadata of external data sources.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46739) handling of three-component feature flags in the YAML configuration, which could lead to loss of subsequent settings.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41009) copying and exporting tables with secondary indexes after deleting internal index tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/45958) filtering of exported objects and operations with lists when exporting to the file system.
* [Fixed](https://github.com/ydb-platform/ydb/pull/47591) Metadata responses in the Kafka API, which could contain an empty list of brokers or an inconsistent controller ID and lead to timeouts in Kafka AdminClient and Kafka Streams.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46033) a leak of records about execution of scripts created by streaming queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/42277) overwriting of the user `config.yaml` mounted at the default path during the first deployment `local-ydb` in Docker.
* [Fixed](https://github.com/ydb-platform/ydb/pull/44011) a Hive crash after restart when the tablet lock and the saved leader pointed to different nodes.

## Version 26.1 {#26-1}

### Version 26.1.1.22 {#26-1-1-22}

Release date: July 27, 2026.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/46894) authentication via the Kafka API for local users: when the setting `DomainLoginOnly` was enabled, users could not work with tenant databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46946) a crash (use-after-free) when updating a vector index due to asynchronous destruction of ReadActor.

### Version 26.1.1.20 {#26-1-1-20}

Release date: July 02, 2026.

#### Functionality

* YQL queries [`SHOW CREATE TABLE`](./yql/reference/syntax/show_create.md?version=v26.1) and [`SHOW CREATE VIEW`](./yql/reference/syntax/show_create.md?version=v26.1) are now available for obtaining DDL expressions required to recreate the structure of a table or view.
* In `ALTER TABLE` added support for default values in [`ADD COLUMN`](./yql/reference/syntax/alter_table/columns.md?version=v26.1) (`DEFAULT`).
* [Shuffle Elimination](./concepts/query_execution/optimizer.md?version=v26.1) is enabled in production: the optimizer can eliminate unnecessary data redistributions in joins.
* Implemented [backup and restore](./reference/ydb-cli/export-import/file-structure.md?version=v26.1) of schema objects: [asynchronous replications](./concepts/async-replication.md?version=v26.1), [external data sources](./concepts/datamodel/external_data_source.md?version=v26.1), [external tables](./concepts/datamodel/external_table.md?version=v26.1), and [transfers](./concepts/transfer.md?version=v26.1).
* The cluster remains operational when [CMS](./concepts/glossary.md?version=v26.1#cms) is unavailable.
* Added the ability to [register dynamic nodes](./devops/configuration-management/configuration-v1/node-authorization.md?version=v26.1) using client TLS certificates.
* For [LDAP authentication](./security/authentication.md?version=v26.1) of a service account, the SASL protocol with the EXTERNAL mechanism is supported — see [`enable_sasl_external_bind`](./reference/configuration/auth_config.md?version=v26.1#ldap-auth-config).
* In [asynchronous replication](./concepts/async-replication.md?version=v26.1), mirroring of [auto-partitioned topics](./concepts/datamodel/topic.md?version=v26.1#autopartitioning) is supported; see also [topic partitions in CDC](./concepts/cdc.md?version=v26.1#topic-partitions).
* [TLI diagnostics](./reference/configuration/tli_config.md?version=v26.1) (Transaction Lock Invalidation) are extended: configuration `tli_config`, [logging](./troubleshooting/performance/queries/tli-logging.md?version=v26.1), and [system views](./dev/system-views.md?version=v26.1#top-tli-partitions).
* [Load-based auto-partitioning](./concepts/datamodel/table.md?version=v26.1#auto_partitioning_by_load) takes into account the CPU load on the partition leader and all its replicas.
* [Streaming queries](./dev/streaming-query/index.md?version=v26.1) support [writing results to local tables](./dev/streaming-query/table-writing.md?version=v26.1).
* In [change data capture (CDC)](./concepts/cdc.md?version=v26.1), you can export user security identifiers (`USER_SIDS`) — see [`ALTER TABLE` `CHANGEFEED`](./yql/reference/syntax/alter_table/changefeed.md?version=v26.1).
* For [external data sources](./concepts/datamodel/external_data_source.md?version=v26.1), `AUTH_METHOD=IAM` is added.
* The CLI supports authentication using a token file (`--token-file`).
* Optimized transactions between topics and tables.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/34906) `RETURNING` in streaming `UPDATE` and interactive queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/34915) `SET DEFAULT` and `DROP DEFAULT` in `ALTER TABLE`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/34958) handling of authentication token expiration in the ticket parser.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35003) query execution in Workload Manager after tenant recreation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35187) use-after-free in the gRPC service.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35663) incremental recovery during SchemeShard restarts and shard failures.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35787) missing streaming query metadata immediately after creation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35793) async checkpointing hang when the input is full and the checkpoint is empty.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36217) mutual influence of quotas of different databases in the Kesus quoter proxy.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36220) hangs in the PQ read session.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36292) scan executor hang on `SELECT … LIMIT` for empty tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36692) delayed TLI reset `LOCKS_BROKEN`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37130) streaming query timeout overflow.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37145) thread safety and token expiration parsing in the IAM credentials provider.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37285) errors in the HTTP gateway.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37668) overflow when processing an empty password.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38033) CDC collection for columns `IsBuildInProgress` disabled.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38490) segfault on update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38544) Shuffle Elimination with the pragma `HashJoinMode`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39337) duplicate rows in scan query on delivery issues.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39687) access to viewer HTTP endpoints: only viewer/admin SID; database scope for database-only tokens.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39798) OOM when loading trash to blob depot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41681) crash `TQueryBase` after canceling a streaming request.
* [Fixed](https://github.com/ydb-platform/ydb/pull/43068) local CDC read from YQL.
* [Fixed](https://github.com/ydb-platform/ydb/pull/44340) fast remote cancellation of queries in query service.

## Version 25.4 {#25-4}

### Version 25.4.1.15 {#25-4-1-15}

Release date: June 5, 2026.

#### Functionality

* YQL instructions [`BATCH UPDATE`](./yql/reference/syntax/batch-update.md?version=v25.4) and [`BATCH DELETE FROM`](./yql/reference/syntax/batch-delete.md?version=v25.4) are available for bulk update and delete of data in tables.
* The execution mechanism for write operations has changed significantly -- now writes are performed in streaming mode without full materialization of data on the Query Processor side before sending to datashards, which improves performance for large write operations. The change applies to some scenarios; in certain cases (for example, tables with secondary indexes) the previous approach is still used. For general information about the execution pipeline, see the [Query execution](./concepts/query_execution/index.md?version=v25.4) section.
* Lookup Join execution is optimized: streaming mode is used without materializing one side of the join, which reduces peak memory consumption, speeds up execution on large datasets, and removes previous restrictions on join side sizes. See the [Index lookup Join](./faq/yql.md?version=v25.4#index-lookup-join) description and the [operator `JOIN`](./yql/reference/syntax/select/join.md?version=v25.4) syntax.
* Added the ability to set access rights for [system views](./devops/observability/system-views.md?version=v25.4) of the cluster and databases.
* For row-oriented tables, the [caching modes](./concepts/datamodel/table.md?version=v25.4#cache-modes) setting and a new mode `in_memory` are available, which allows preloading table data into RAM provided the necessary amounts of RAM are available.
* For topic readers, a parameter [`availability-period`](./reference/ydb-cli/topic-consumer-add.md?version=v25.4) has been added that allows extending the storage of unacknowledged messages beyond the retention period.
* [Per-partition topic metrics and export to custom shard quotas](./reference/observability/metrics/index.md?version=v25.4#topics_partitions) are available for accounting and observability.
* Accelerating queries with `LIMIT` in column-oriented tables by early limiting the sample on storage nodes (for queries without sorting or with sorting by primary key). General syntax [`LIMIT` and `OFFSET`](./yql/reference/syntax/select/limit_offset.md?version=v25.4) in YQL.
* Column-oriented tables support the `Bool` type in schemas and queries — see [YQL primitive types](./yql/reference/types/primitive.md?version=v25.4#numeric).
* [Filterable vector index](./dev/vector-indexes.md?version=v25.4#filtered) correctly finds rows inserted into the table after index creation with new filter column values.
* Streaming processing and data delivery are more tightly integrated into the core: [topic → table transfer](./concepts/transfer.md?version=v25.4), [streaming queries](./dev/streaming-query/index.md?version=v25.4) became available to users when 'EnableStreamingQueries' is enabled.
* Added the `overlap_clusters` option to significantly improve vector search quality by placing vectors into multiple index clusters (index settings) — see [vector indexes](./dev/vector-indexes.md?version=v25.4).
* Significantly accelerated search across all types of vector indexes by computing distances locally on each datashard before network transfer — see [VIEW (vector index)](./yql/reference/syntax/select/vector_index.md?version=v25.4) and [vector indexes](./dev/vector-indexes.md?version=v25.4).
* Accelerated full vector search without an ANN index via pushdown (vector search, KNN UDF) — see [vector search](./concepts/query_execution/vector_search.md?version=v25.4) and [KNN module](./yql/reference/udf/list/knn.md?version=v25.4).
* Fully supported the mechanism for working with secrets stored in the database: creation, modification, deletion, and usage — see [Secrets](./concepts/datamodel/secrets.md?version=v25.4). Note that the [old syntax](./concepts/datamodel/secrets.md?version=v25.3) is declared deprecated.
* Improved execution of [`UNION ALL`](./yql/reference/syntax/select/union.md?version=v26.2#union-all): parallel execution is now supported, which improves analytical query performance.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) a vulnerability in [LDAP authentication](./security/authentication.md): knowing the login and password of any LDAP user (including one not in a group with access to {{ ydb-short-name }}), it was possible to bypass group membership checks and gain access to the cluster (injection into the LDAP user search filter; special character escaping per RFC 2254 added).

## Version 25.3 {#25-3}

### Version 25.3.1.27 {#25-3-1-27}

Release date: May 20, 2026.

#### Functionality

* Added support for 2 DC configuration with synchronous data writes (mode [`Bridge`](./concepts/bridge.md)); available in {{ ydb-short-name }} Enterprise.
* Topic improvements:
  * In the Kafka API, you can now create [compacted](https://docs.confluent.io/kafka/design/log_compaction.html#ak-log-compaction) topics; YDB automatically creates and deletes the internal service consumer used for topic compaction;
  * The topic API has been extended: the output `DescribeConsumer` now includes [new parameters](./reference/ydb-sdk/topic.md), and [per-partition topic metrics can be exported to user quotas](./reference/observability/metrics/index.md#topics).
* Implemented [backup and restore](./reference/ydb-cli/export-import/file-structure.md?version=v25.3#topics) of topic configuration to and from S3.
* Implemented [export of views](./reference/ydb-cli/export-import/file-structure.md#views) (`VIEW`) to and from S3.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) an [LDAP authentication](./security/authentication.md) vulnerability: knowing the login and password of any LDAP user (including one not in a group with access to {{ ydb-short-name }}), it was possible to bypass the group membership check and gain access to the cluster (injection into the LDAP user search filter; special characters are now escaped per RFC 2254).
* [Fixed](https://github.com/ydb-platform/ydb/pull/33758) an error that caused a session leak on the server side.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36926) an error that, in rare cases, could cause reads from a table to block its deletion.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20238) a race condition when updating the CPU soft limit.
* [Fixed the behavior](https://github.com/ydb-platform/ydb/pull/18121) where `ALTER TABLE` could fail with an error for tables with a vector index.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18088) inconsistent results in some read-write transactions — conflicting writes no longer overwrite uncommitted changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18234) a serializability violation in read-write transactions after shard restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20560) a memory management error when committing offsets in topics with automatic partitioning enabled.
* [Added](https://github.com/ydb-platform/ydb/pull/18698) checks for enabled encryption in zero-copy transfer.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20519) an error causing a VDisk to hang in local recovery after an error `ChunkRead`.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/18924) the appearance of phantom VDisks due to races between group creation and deletion operations.
* [Improved](https://github.com/ydb-platform/ydb/pull/17687) PDisk state detection — the actual state from BSC is now used, which improves healthcheck accuracy.
* When a session ends via an attach stream, a [notification is now sent](https://github.com/ydb-platform/ydb/pull/22298).
* The coordination service now correctly [returns](https://github.com/ydb-platform/ydb/pull/16901) the code `SCHEME_ERROR` for non-existent resources instead of the erroneously used code `INTERNAL_ERROR`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20157) memory handling errors and internal data consistency violations in the Workload Manager and related scheduler code.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20432) an issue where PDisk information requests could time out if the target node was down or unavailable.

## Version 25.2 {#25-2}

### Version 25.2.1.26 {#25-2-1-26}

Release date: May 12, 2026.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) a vulnerability in [LDAP authentication](./security/authentication.md): knowing the login and password of any LDAP user (including one not in a group with access to {{ ydb-short-name }}), it was possible to bypass the group membership check and gain access to the cluster (injection into the LDAP user search filter; special characters are now escaped per RFC 2254).
* [Fixed](https://github.com/ydb-platform/ydb/pull/25112) an [issue](https://github.com/ydb-platform/ydb/issues/23858) where deleting a [tablet](./concepts/glossary.md#tablet) could hang
* [Fixed](https://github.com/ydb-platform/ydb/pull/25145) an [error](https://github.com/ydb-platform/ydb/issues/20866) that caused an error when changing a table's follower
* Fixed a number of errors related to [changefeed](./concepts/glossary.md#changefeed):
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25689) an [error](https://github.com/ydb-platform/ydb/issues/25524) where importing a table with a Utf8 key and changefeed enabled could fail
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25453) an [error](https://github.com/ydb-platform/ydb/issues/25454) where importing a table without change streams could fail due to incorrect search for changefeed files
* [Fixed](https://github.com/ydb-platform/ydb/pull/26069) an [error](https://github.com/ydb-platform/ydb/issues/25869) that could cause failures during UPSERT operations in column-oriented tables
* [Fixed](https://github.com/ydb-platform/ydb/pull/26504) an [error](https://github.com/ydb-platform/ydb/issues/26225) that caused a crash due to accessing already freed memory
* [Fixed](https://github.com/ydb-platform/ydb/pull/26657) an [error](https://github.com/ydb-platform/ydb/issues/23122) with duplicates in unique secondary indexes
* [Fixed](https://github.com/ydb-platform/ydb/pull/26879) an [error](https://github.com/ydb-platform/ydb/issues/26565) of incorrect checksum matching when restoring compressed backups from S3
* [Fixed](https://github.com/ydb-platform/ydb/pull/27528) an [error](https://github.com/ydb-platform/ydb/issues/27193) where some TPC-H 1000 benchmark queries could fail
* Fixed a number of issues related to cluster initialization:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25678) an [error](https://github.com/ydb-platform/ydb/issues/25023) where cluster initialization could hang when authorization was mandatory
  * [Fixed](https://github.com/ydb-platform/ydb/pull/28886) an [issue](https://github.com/ydb-platform/ydb/issues/27228) where creating new databases immediately after cluster deployment was impossible for several minutes
* [Fixed](https://github.com/ydb-platform/ydb/pull/28655) an [error](https://github.com/ydb-platform/ydb/issues/28510) where a race condition could occur and clients received an error `Could not find correct token validator` if recently issued tokens were used before the state update `LoginProvider`
* [Fixed](https://github.com/ydb-platform/ydb/pull/29940) an [error](https://github.com/ydb-platform/ydb/issues/29903) where a named expression containing another named expression led to an incorrect backup `VIEW`

### Release candidate 25.2.1.10 {#25-2-1-10-rc}

Release date: September 21, 2025.

#### Functionality

* [Analytical capabilities](./concepts/analytics/index.md) are enabled by default: [column-oriented tables](./concepts/datamodel/table.md#column-oriented-tables) can be created without enabling special flags, using LZ4 compression and hash partitioning. Supported operations include a wide range of DML (UPDATE, DELETE, UPSERT, INSERT INTO ... SELECT) and CREATE TABLE AS SELECT. Integration with dbt, Apache Airflow, Jupyter, Superset, and federated queries to S3 allow building end-to-end analytical pipelines in YDB.
* The [cost-based optimizer](./concepts/query_execution/optimizer.md) runs by default for queries that use at least one column-oriented table, but can also be enabled forcibly for other queries. The cost-based optimizer improves query execution performance by computing the optimal join order and type based on table statistics; supported [hints](./dev/optimization/hints.md) allow fine-tuning execution plans for complex analytical queries.
* Implemented [data transfer](./concepts/transfer.md), an asynchronous mechanism for moving data from a topic to a table. [Creating](./yql/reference/syntax/create-transfer.md) a transfer instance, [modifying](./yql/reference/syntax/alter-transfer.md) it, and [deleting](./yql/reference/syntax/drop-transfer.md) it are performed using YQL. For a quick start, use the [guide with an example](./recipes/transfer/quickstart.md).
* Added [spilling](./concepts/query_execution/spilling.md), a memory management mechanism in which intermediate data resulting from query execution that exceeds the available RAM of a node is temporarily offloaded to external storage. Spilling ensures the execution of user queries that require processing large volumes of data exceeding the available node memory.
* Increased the [maximum time for executing a single query](./concepts/limits-ydb?version=v25.2) from 30 minutes to 2 hours.
* Added support for Certificate Authority (CA) and [Yandex Cloud Identity and Access Management (IAM)](https://yandex.cloud/ru/docs/iam) authentication in [asynchronous replication](./yql/reference/syntax/create-async-replication.md?version=v25.2).
* Required configuration:

  * [Authentication and authorization of nodes](./devops/configuration-management/configuration-v1/node-authorization.md) for registering nodes in the cluster.
* Enabled by default:

  * [vector index](./dev/vector-indexes.md) for approximate vector search;
  * support in [YDB Topics Kafka API](./reference/kafka-api/index.md) for [client-side read balancing](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb), [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html), and [transactions](https://www.confluent.io/blog/transactions-apache-kafka);
  * support for [topic auto-partitioning](./concepts/cdc.md#topic-partitions) in CDC for row-oriented tables;
  * support for topic auto-partitioning for asynchronous replication;
  * support for the parameterized [Decimal type](./yql/reference/types/primitive.md#numeric);
  * support for the [DateTime64 type](./yql/reference/types/primitive.md#datetime);
  * automatic deletion of temporary directories and tables when exporting to S3;
  * support for [change feeds](./concepts/cdc.md) in backup and restore operations;
  * the ability to [specify the number of replicas](./yql/reference/syntax/alter_table/indexes.md) for a secondary index;
  * system views with [history of overloaded partitions](./dev/system-views#top-overload-partitions).

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/24265) an error in [Workload Manager](./dev/resource-consumption-management.md) that could cause CPU consumption by column-oriented tables to exceed the set limits.

## Version 25.1 {#25-1}

### Version 25.1.4.18 {#25-1-4-18}

Release date: May 12, 2026.

#### Functionality

* [Added](https://github.com/ydb-platform/ydb/pull/21119) the ability to use familiar data streaming tools – Kafka Connect, Confluent Schema Registry, Kafka Streams, Apache Flink, AKH via the [Kafka API](./reference/kafka-api/index.md) when working with YDB Topics. Now the YDB Topics Kafka API supports:
  * client-side read balancing – enabled by setting the flag `enable_kafka_native_balancing` in the [cluster configuration](./reference/configuration/feature_flags.md). [How read balancing works in Apache Kafka](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb). Now read balancing in the YDB Topics Kafka API will work exactly the same way,
  * [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html) – enabled by setting the flag `enable_topic_compactification_by_key`,
  * [transactions](https://www.confluent.io/blog/transactions-apache-kafka) – enabled by setting the flag `enable_kafka_transactions`.
* [Added](https://github.com/ydb-platform/ydb/pull/20982) a [new protocol](https://github.com/ydb-platform/ydb/issues/11064) in [Node Broker](./concepts/glossary.md#node-broker) that eliminates network traffic spikes on large clusters (over 1000 servers) associated with broadcasting node information.

#### YDB UI

* [Fixed](https://github.com/ydb-platform/ydb/pull/17839) an [error](https://github.com/ydb-platform/ydb/issues/15230) that caused not all tablets to be displayed on the Tablets tab in the diagnostics section.
* Fixed an [error](https://github.com/ydb-platform/ydb/issues/18735) that caused the Storage tab in the database diagnostics section to display not only storage nodes.
* Fixed a [serialization error](https://github.com/ydb-platform/ydb-embedded-ui/issues/2164) that could cause a crash when opening query execution statistics.
* Changed the logic for nodes transitioning to a critical state – a CPU pool filled to 75-99% now triggers a warning rather than a critical state.

#### Performance

* [Optimized](https://github.com/ydb-platform/ydb/pull/20197) handling of empty inputs when performing JOIN operations.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) a vulnerability in [LDAP authentication](./security/authentication.md): knowing the login and password of any LDAP user (including one not in a group with access to {{ ydb-short-name }}), it was possible to bypass the group membership check and gain access to the cluster (injection into the LDAP user search filter; special characters are now escaped per RFC 2254).
* [Added](https://github.com/ydb-platform/ydb/pull/21918) support in asynchronous replication for a new type of change record — `reset`-records (in addition to `update`- and `erase`-records).
* [Fixed](https://github.com/ydb-platform/ydb/pull/21836) an [error](https://github.com/ydb-platform/ydb/issues/21814) that caused a replication instance with an unspecified parameter `COMMIT_INTERVAL` to lead to a process failure.
* [Fixed](https://github.com/ydb-platform/ydb/pull/21652) rare errors when reading from a topic during partition balancing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22455) an error that could leave system tablets of a database undeleted when deleting a dedicated database.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22203) an error that could cause tablets to hang due to insufficient memory on nodes. Now tablets will automatically start as soon as enough resources become available on any of the nodes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/24278) an error that caused only the first message from a batch to be saved when writing Kafka messages, while the remaining messages were ignored.

### Release candidate 25.1.2.7 {#25-1-2-7-rc}

Release date: July 14, 2025.


#### Functionality

* [Implemented](https://github.com/ydb-platform/ydb/pull/19504) a [vector index](./dev/vector-indexes.md?version=v25.1) for approximate vector search. Recipes for [YDB CLI and YQL](./recipes/vector-search?version=v25.1) have been published for vector search, as well as examples of working [in C++ and Python](./recipes/ydb-sdk/vector-search?version=v25.1).
* [Added](https://github.com/ydb-platform/ydb/issues/11454) support for [consistent asynchronous replication](./concepts/async-replication.md?version=v25.1).
* Added a [V2 configuration mechanism](./devops/configuration-management/configuration-v2/config-overview?version=v25.1) that simplifies deploying new clusters {{ ydb-short-name }} and further working with them. [Comparison](./devops/configuration-management/compare-configs?version=v25.1) of V1 and V2 configuration mechanisms.
* Added support for the parameterized [Decimal type](./yql/reference/types/primitive.md?version=v25.1#numeric).
* [Added](https://github.com/ydb-platform/ydb/pull/8065) the ability to not use the operator `DECLARE` to declare parameter types in queries. Now parameter types are determined automatically based on the passed values.
* Implemented client-side partition balancing when reading via the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) (similar to Kafka itself). Previously, balancing occurred on the server. It is enabled by setting the flag `enable_kafka_native_balancing` in the cluster configuration.
* Added support for [automatic topic partitioning](./concepts/cdc.md?version=v25.1#topic-partitions) in CDC for row-oriented tables. It is enabled by setting the flag `enable_topic_autopartitioning_for_cdc` in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/8264) the ability to [change data retention time](./concepts/cdc.md?version=v25.1#topic-options) in a CDC topic using the expression `ALTER TOPIC`.
* [Supported](https://github.com/ydb-platform/ydb/pull/7052) [format DEBEZIUM_JSON](./concepts/cdc.md?version=v25.1#debezium-json-record-structure) for change feeds.
* [Added](https://github.com/ydb-platform/ydb/pull/19507) the ability to create change feeds for index tables.
* Added the ability to [specify the number of replicas](./yql/reference/syntax/alter_table/indexes.md?version=v25.1) for a secondary index. It is enabled by setting the flag `enable_access_to_index_impl_tables` in the cluster configuration.
* The set of supported objects in backup and restore operations has been expanded. It is enabled by setting the flags specified in parentheses:
  * [support](https://github.com/ydb-platform/ydb/issues/7054) for change feeds (flags `enable_changefeeds_export` and `enable_changefeeds_import`);
  * [support](https://github.com/ydb-platform/ydb/issues/12724) for views (`VIEW`) (flag `enable_view_export`).
* Added automatic deletion of temporary directories and tables when exporting to S3. It is enabled by setting the flag `enable_export_auto_dropping` in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/12909) automatic integrity check of backups during import, preventing restoration from corrupted backups and protecting against data loss.
* [Added](https://github.com/ydb-platform/ydb/pull/15570) the ability to create views that use [UDF](./yql/reference/builtins/basic.md?version=v25.1#udf) in queries.
* Added system views with information about [access permission settings](./dev/system-views?version=v25.1#top-tli-partitions), [history of overloaded partitions](./dev/system-views?version=v25.1#top-tli-partitions) - enabled by setting the flag `enable_followers_stats` in the cluster configuration, [history of partitions of row-oriented tables with broken locks (TLI)](./dev/system-views?version=v25.1#top-tli-partitions).
* Added new parameters to the [CREATE USER](./yql/reference/syntax/create-user.md?version=v25.1) and [ALTER USER](./yql/reference/syntax/alter-user.md?version=v25.1) statements:
  * `HASH` — the ability to specify a password in encrypted form;
  * `LOGIN` and `NOLOGIN` — unlocking and locking a user.
* Increased account security:
  * [Added](https://github.com/ydb-platform/ydb/pull/11963) [user password complexity check](./reference/configuration/?version=v25.1#password-complexity);
  * [Implemented](https://github.com/ydb-platform/ydb/pull/12578) [automatic user lockout](./reference/configuration/?version=v25.1#account-lockout) when the password entry attempt limit is exhausted;
  * [Added](https://github.com/ydb-platform/ydb/pull/12983) the ability for a user to change their password independently.
* [Implemented](https://github.com/ydb-platform/ydb/issues/9748) the ability to switch functional flags while the server is running {{ ydb-short-name }}. Flags for which the [proto file](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/feature_flags.proto#L60) does not specify the parameter `(RequireRestart) = true` will be applied without restarting the cluster.
* Now the oldest (rather than new) locks [are switched to full-shard locks](https://github.com/ydb-platform/ydb/pull/11329) when the number of locks on shards is exceeded.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12567) keeping optimistic locks in memory during a smooth restart of data shards, which should reduce the number of ABORTED errors caused by lock loss during table balancing between nodes.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12689) cancellation of volatile transactions with the ABORTED status during a smooth restart of data shards.
* [Added](https://github.com/ydb-platform/ydb/pull/6342) the ability to remove `NOT NULL` constraints on a column in a table using the query `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL`.
* [Added](https://github.com/ydb-platform/ydb/pull/9168) a limit of 100,000 on the number of concurrent session creation requests in the coordination service.
* [Increased](https://github.com/ydb-platform/ydb/pull/14219) the maximum [number of columns in a primary key](./concepts/limits-ydb.md?version=v25.1#schema-object) from 20 to 30.
* Improved diagnostics and introspection of memory-related errors ([#10419](https://github.com/ydb-platform/ydb/pull/10419), [#11968](https://github.com/ydb-platform/ydb/pull/11968)).
* **_(Experimental)_** [Added](https://github.com/ydb-platform/ydb/pull/14075) an experimental mode with stricter access permission checks. It is enabled by setting the following flags:
  * `enable_strict_acl_check` – do not allow granting permissions to non-existent users and deleting users if permissions have been granted to them;
  * `enable_strict_user_management` — enables strict rules for administering local users (i.e., only a cluster or database administrator can administer local users);
  * `enable_database_admin` — adds the database administrator role.

#### Backward incompatible changes

* If you use queries that access named expressions as tables using [AS_TABLE](./yql/reference/syntax/select/from_as_table?version=v25.1), update [temporal over YDB](https://github.com/yandex/temporal-over-ydb) to version [v1.23.0-ydb-compat](https://github.com/yandex/temporal-over-ydb/releases/tag/v1.23.0-ydb-compat) before updating YDB to the current version to avoid errors in executing such queries.

#### YDB UI

* The query editor [added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1974) support for partial result loading — display starts immediately upon receiving the first fragment from the server without waiting for the query to complete. This allows faster result retrieval.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/pull/1967) security: controls that are unavailable to the user are now not displayed in the interface. Users will not encounter "Access denied" errors.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1981) search by tablet ID on the "Tablets" tab.
* Added a hotkey hint that opens with the combination `⌘+K`.
* Added an "Operations" tab to the database page that allows viewing the list of operations and canceling them.
* Updated the cluster monitoring panel, added the ability to collapse it.
* Implemented support for case-sensitive search in the JSON hierarchical display tool.
* After selecting a database, code examples for connecting in YDB SDK were added to the top panel, which speeds up the development process.
* Fixed sorting of strings on the Queries tab.
* Removed unnecessary confirmation prompts when closing the browser page in the query editor — confirmation is now requested only when necessary.

#### Performance

* [Added](https://github.com/ydb-platform/ydb/pull/6509) support for [constant folding](https://en.wikipedia.org/wiki/Constant_folding) in the query optimizer by default, which improves query performance by computing constant expressions at the compilation stage.
* [Added](https://github.com/ydb-platform/ydb/issues/6512) a new granular timecast protocol that will reduce the execution time of distributed transactions (slowdown of one shard will not lead to slowdown of all).
* [Implemented](https://github.com/ydb-platform/ydb/issues/11561) functionality for preserving datashard state in memory during restarts, which allows preserving locks and increasing the chances of successful transaction execution. This reduces the execution time of long transactions by reducing the number of retries.
* [Implemented](https://github.com/ydb-platform/ydb/pull/15255) pipelined processing of internal transactions in [Node Broker](./concepts/glossary?version=v25.1#node-broker), which accelerated the startup of dynamic nodes in the cluster {{ ydb-short-name }}.
* [Improved](https://github.com/ydb-platform/ydb/pull/15607) Node Broker resilience to increased load from cluster nodes.
* [Enabled](https://github.com/ydb-platform/ydb/pull/19440) by default, offloaded B-Tree indexes instead of non-offloaded SST indexes, which reduces memory consumption when storing "cold" data.
* [Optimized](https://github.com/ydb-platform/ydb/pull/15264) memory consumption of storage nodes.
* [Reduced](https://github.com/ydb-platform/ydb/pull/10969) Hive startup time by up to 30%.
* [Optimized](https://github.com/ydb-platform/ydb/pull/6561) the replication process in distributed storage.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9491) the header size of large binary objects in VDisk.
* [Reduced](https://github.com/ydb-platform/ydb/pull/15517) memory consumption by cleaning allocator pages.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/9707) an error in [Interconnect](./concepts/glossary.md?version=v25.1#actor-system-interconnect) configuration that led to performance degradation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13993) the "Out of memory" error when deleting very large tables by regulating the number of tablets concurrently processing this operation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9848) an error that occurred when specifying the same database node multiple times in the configuration for system tablets.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/11059) an error of long (seconds) data reads during frequent table resharding operations.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9723) a read error from asynchronous replicas that caused a failure.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9507) rare hangs during initial scanning of [CDC](./dev/cdc.md?version=v25.1).
* [Fixed](https://github.com/ydb-platform/ydb/pull/11483) handling of incomplete schema transactions in data shards during system restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10460) an error of inconsistent reading from a topic when trying to explicitly commit a message read within a transaction. Now the user will get an error when trying to commit the message.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12220) an error that caused auto-partitioning to work incorrectly when working with a topic in a transaction.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12905) transaction hangs when working with topics during tablet restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13910) the “Key is out of range” error when importing from S3-compatible storage.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13741) incorrect detection of the end of the metadata field in the cluster configuration.
* [Improved](https://github.com/ydb-platform/ydb/pull/16420) building of secondary indexes: when some errors occur, the system retries the process instead of interrupting it.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16635) an error in executing the expression `RETURNING` in queries `INSERT INTO` and `UPSERT INTO`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16269) the issue of the “Drop Tablet” operation hanging in PQ tablet, especially during delays in Interconnect operation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16194) an error that occurred during [compaction](./concepts/glossary.md?version=v25.1#compaction) of a VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) the issue that caused long topic read sessions to end with “too big inflight” errors.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15515) a hang when reading a topic if at least one partition had no incoming data but was read by multiple consumers.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/18614) a rare issue of PQ tablet restarts.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/18378) the issue where, after upgrading the cluster version, Hive started subscribers in data centers without working database nodes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/19057) an error `Failed to set up listener on port 9092 errno# 98 (Address already in use)` that occurred during version upgrade.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18905) an error that led to a segmentation fault when simultaneously executing a healthcheck query and shutting down a cluster node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18899) a failure in [partitioning of a row-oriented table](./concepts/datamodel/table.md?version=v25.1#partitioning_row_table) when choosing a split key from access samples containing mixed operations with a full key and a key prefix (for example, exact read or range read).
* [Fixed](https://github.com/ydb-platform/ydb/pull/16797) an error that caused topic auto-partitioning not to work when the configuration parameter `max_active_partition` was set using the expression `ALTER TOPIC`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18938) an error that caused `ydb scheme describe` to return the list of columns in a different order than they were specified when creating the table.

## Version 24.4 {#24-4}

### Version 24.4.4.12 {#24-4-4-12}

Release date: June 3, 2025.

#### Performance

* [Limited](https://github.com/ydb-platform/ydb/pull/17755) the number of concurrently processed configuration changes.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18289) memory consumption by PQ tablets.
* [Optimized](https://github.com/ydb-platform/ydb/issues/18473) CPU consumption by the Scheme shard tablet, which reduced response latency for requests. Now the limit on the number of Scheme shard operations is checked before performing tablet split and merge operations.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/17123) a rare client application hang during a transaction commit when a partition was deleted before the topic write quota was updated.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17312) an error in copying tables with the Decimal type that caused a failure when rolling back to a previous version.
* [Fixed](https://github.com/ydb-platform/ydb/pull/17519) [an error](https://github.com/ydb-platform/ydb/issues/17499) where a commit without confirming a write to a topic caused the current and subsequent transactions with topics to be blocked.
* Fixed transaction hangs when working with topics during [tablet restart](https://github.com/ydb-platform/ydb/issues/17843) or [deletion](https://github.com/ydb-platform/ydb/issues/17915).
* [Fixed](https://github.com/ydb-platform/ydb/pull/18114) [issues](https://github.com/ydb-platform/ydb/issues/18071) with reading messages larger than 6Mb via the [Kafka API](./reference/kafka-api).
* [Eliminated](https://github.com/ydb-platform/ydb/pull/18319) a memory leak when writing to a [topic](./concepts/glossary#topic).
* Fixed errors in processing [nullable columns](https://github.com/ydb-platform/ydb/issues/15701) and [columns with the UUID type](https://github.com/ydb-platform/ydb/issues/15697) in row tables.

### Version 24.4.4.2 {#24-4-4-2}

Release date: April 15, 2025.

#### Functionality

* Enabled by default:

  * support for {% if feature_view %}[views (VIEW)](./concepts/datamodel/view.md){% else %}views (VIEW){% endif %};
  * the [auto-partitioning](./concepts/datamodel/topic.md#autopartitioning) mode for topics;
  * [transactions involving topics and row tables](./concepts/transactions.md#topic-table-transactions);
  * [volatile distributed transactions](./contributor/datashard-distributed-txs.md#volatile-transactions).

* Added the ability to [read from and write to a topic](./reference/kafka-api/examples.md#kafka-api-usage-examples) using the Kafka API without authentication.

#### Performance

* Enabled by default [automatic secondary index selection](./dev/secondary-indexes.md#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) when executing a query.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/14811) an error that caused a significant decrease in read speed from [tablet subscribers](./concepts/glossary.md#tablet-follower).
* [Fixed](https://github.com/ydb-platform/ydb/pull/14516) an error that caused a volatile distributed transaction to wait for confirmation until the next restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15077) a rare error that caused a failure when connecting tablet subscribers to a leader with an inconsistent command log state.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15074) a rare error that caused a failure when restarting a remote datashard with inconsistent changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15194) an error that could disrupt the order of message processing in a topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15308) a rare error that could cause reading from a topic to hang.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15160) an issue where a transaction would hang when a user managed a topic while a PQ tablet was being moved to another node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15233) an issue with a counter value leak for userInfo that could lead to a read error `too big in flight`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15467) a proxy server crash caused by duplicate topics in a request.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15933) a rare error that allowed a user to write to a topic bypassing account quota limits.
* [Fixed](https://github.com/ydb-platform/ydb/pull/16288) an issue where the system returned "OK" after deleting a topic, but its tablets continued to run. To delete such tablets, follow the instructions in the [pull request](https://github.com/ydb-platform/ydb/pull/16288).
* [Fixed](https://github.com/ydb-platform/ydb/pull/16418) a rare error where a backup of a large table with a secondary index could not be restored.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15862) an issue that caused an error when inserting data using `UPSERT` into row tables with default values.
* [Fixed](https://github.com/ydb-platform/ydb/pull/15334) an error that caused a crash when executing queries to tables with secondary indexes that return result lists using the `RETURNING *` expression.

## Version 24.3 {#24-3}

### Version 24.3.15.5 {#24-3-15-5}

Release date: February 6, 2025.

#### Functionality

* Added the ability to register a [database node](./concepts/glossary.md#database-node) by certificate. In [Node Broker](./concepts/glossary.md#node-broker), a flag `AuthorizeByCertificate` for using the certificate during registration was added.
* [Added](https://github.com/ydb-platform/ydb/pull/11775) priorities for checking authentication tickets [using a third-party IAM provider](./security/authentication.md#iam), with requests from new users processed with the highest priority. Tickets in the cache update their information with lower priority.

#### Performance

* [Speed up](https://github.com/ydb-platform/ydb/pull/12747) tablet startup on large clusters: 210 ms **→** 125 ms (ssd), 260 ms **→** 165 ms (hdd).

#### Bug fixes

* [Removed](https://github.com/ydb-platform/ydb/pull/11901) the restriction on writing values greater than 127 to the Uint8 type.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12221) an error where reading small messages from a topic in small portions significantly increased CPU load. This could lead to delays in reading/writing to that topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12915) an error restoring from a backup saved in S3 storage with Path-style addressing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13918) an error restoring from a backup created at the moment of automatic table splitting.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12601) a serialization error `Uuid` for [CDC](./concepts/cdc.md).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12018) a potential breakage of ["frozen" locks](./contributor/datashard-locks-and-change-visibility#vzaimodejstvie-s-raspredelyonnymi-tranzakciyami) that could be caused by bulk operations (for example, deletion by TTL).
* [Fixed](https://github.com/ydb-platform/ydb/pull/12804) an error that could cause reads on tablet followers to fail during automatic table splitting.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12807) an error where the [coordination node](./concepts/datamodel/coordination-node.md) successfully registered proxy servers despite a connection break.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11593) errors occurring when opening the tab with information about [distributed storage groups](./concepts/glossary.md#storage-group) in the interface.
* [Fixed](https://github.com/ydb-platform/ydb/pull/12448) an [error](https://github.com/ydb-platform/ydb/issues/12443) due to which [Health Check](./reference/ydb-sdk/health-check-api) did not report problems with time synchronization.
* [Fixed](https://github.com/ydb-platform/ydb/pull/11658) a rare issue that led to errors when executing a read query.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13501) a rare issue that led to leaks of uncommitted changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/13948) consistency issues related to caching of deleted ranges.

### Version 24.3.11.14 {#24-3-11-14}

Release date: January 9, 2025.

* [Supported](https://github.com/ydb-platform/ydb/pull/11276) restart without cluster availability loss in a [minimum fault-tolerant configuration](./concepts/topology#reduced) of three nodes.
* [Added](https://github.com/ydb-platform/ydb/pull/13218) new Roaring bitmap UDF functions: AndNotWithBinary, FromUint32List, RunOptimize

### Version 24.3.11.13 {#24-3-11-13}


Release date: December 24, 2024.

#### Functionality

* Added [query tracing](./reference/observability/tracing/setup) – a tool that allows you to see in detail the path of a query through the distributed system.
* Added support for [asynchronous replication](./concepts/async-replication), which allows you to synchronize data between YDB databases almost in real time. It can also be used to migrate data between databases with minimal downtime for the applications using them.
* Added support for [views (VIEW)](https://ydb.tech/docs/en/concepts/datamodel/view), which can be enabled by the cluster administrator using the setting `enable_views` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* In [federated queries](./concepts/query_execution/federated_query/), new external data sources are supported: MySQL, Microsoft SQL Server, Greenplum.
* Developed [documentation](./devops/deployment-options/manual/federated-queries/connector-deployment) on deploying YDB with federated query functionality (in manual mode).
* For a Docker container with YDB, a startup parameter has been added `FQ_CONNECTOR_ENDPOINT` to specify the connector address for external data sources. TLS encryption of the connection to the connector is now supported. It is now possible to output the port of the connector service running locally on the same host as the YDB dynamic node.
* A topic [auto-partitioning](./concepts/datamodel/topic#autopartitioning) mode has been added, in which topics can split partitions based on load while preserving message read order guarantees and exactly once writes. The mode can be enabled by the cluster administrator using the settings `enable_topic_split_merge` and `enable_pqconfig_transactions_at_scheme_shard` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* [Transactions](./concepts/transactions#topic-table-transactions) involving [topics](https://ydb.tech/docs/en/concepts/datamodel/topic) and row-based tables have been added. Thus, you can transactionally move data from tables to topics and in the reverse direction, as well as between topics, so that data is neither lost nor duplicated. Transactions can be enabled by the cluster administrator using the settings `enable_topic_service_tx` and `enable_pqconfig_transactions_at_scheme_shard` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).
* [Support has been added](https://github.com/ydb-platform/ydb/pull/7150) for [CDC](./concepts/cdc) for synchronous secondary indexes.
* The ability to change the record retention period in [CDC](./concepts/cdc.md) topics has been added.
* [Auto-increment](./yql/reference/types/serial) support has been added for columns included in the table primary key.
* Writing to the [audit log](./security/audit-log) of user login events in YDB, user session termination events in the user interface, as well as backup and restore-from-backup requests has been added.
* A system view has been added that allows you to obtain information about sessions established with the database using a query.
* Support for constant default values for columns of row-based tables has been added.
* Support for the expression `RETURNING` in queries has been added.
* The [built-in function](./yql/reference/builtins/basic.md#version) `version()` has been added.
* [Added](https://github.com/ydb-platform/ydb/pull/8708) start/end time and author to the metadata of backup/restore operations from S3-compatible storage.
* Support for backup/restore of table ACLs from S3-compatible storage has been added.
* For queries reading from S3, paths and decompression method have been added to the plan.
* New parsing settings have been added for `timestamp`, `datetime` when reading data from S3.
* Support for the type `Decimal` in [partitioning keys](https://ydb.tech/docs/en/dev/primary-key/column-oriented#klyuch-particionirovaniya) has been added.
* Improved storage problem diagnostics in HealthCheck.
* **_(Experimental)_** A [cost-based optimizer](./concepts/query_execution/optimizer#stoimostnoj-optimizator-zaprosov) has been added for complex queries involving [column-oriented tables](./concepts/glossary#column-oriented-table). The optimizer considers a large number of alternative execution plans and selects the best one based on the estimated cost of each option. Currently, the optimizer only works with plans that include [JOIN](./yql/reference/syntax/join) operations.
* **_(Experimental)_** An initial version of the [workload manager](./dev/resource-consumption-management) has been implemented, which allows creating resource pools with limits on CPU, memory, and the number of active queries. Resource classifiers have been implemented to assign queries to a specific resource pool.
* **_(Experimental)_** [Automatic index selection](https://ydb.tech/docs/en/dev/secondary-indexes#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) has been implemented during query execution, which can be enabled by the cluster administrator using the setting `index_auto_choose_mode` in `table_service_config` in the [dynamic configuration](./devops/configuration-management/configuration-v1/dynamic-config#updating-dynamic-configuration).

#### YDB UI

* Support for creating and [displaying](https://github.com/ydb-platform/ydb-embedded-ui/issues/782) an async replication instance has been added.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/929) a designation for [columns with auto-increment](./yql/reference/types/serial).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1438) a tab with information about [tablets](./concepts/glossary#tablet).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1289) a tab with information about [distributed storage groups](./concepts/glossary#storage-group).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1218) a setting to add [tracing](./reference/observability/tracing/setup) to all queries and display query tracing results.
* The PDisk page now includes [attributes](https://github.com/ydb-platform/ydb-embedded-ui/pull/1069), disk space consumption information, and a button that triggers [disk decommissioning](./devops/deployment-options/manual/decommissioning).
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1313) information about running queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1291) a setting for the row limit in query editor output and a display if query results exceed the limit.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1049) a display of the list of queries with maximum CPU consumption over the last hour.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1127) search on the pages with query history and the list of saved queries.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1117) the ability to interrupt query execution.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/issues/944) the ability to save a query from the editor using hotkeys.
* [Separated](https://github.com/ydb-platform/ydb-embedded-ui/pull/1422) the display of disks from donor disks.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1154) support for InterruptInheritance ACL and improved display of effective ACLs.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/889) a display of the current user interface version.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1229) a tab with information about the state of experimental functionality enablement settings.

#### Performance

* [Speeded up](https://github.com/ydb-platform/ydb/pull/7589) recovery of tables with secondary indexes from backups by up to 20% in our tests.
* [Optimized](https://github.com/ydb-platform/ydb/pull/9721) Interconnect throughput.
* Improved performance of CDC topics containing thousands of partitions.
* Made a number of improvements to the Hive tablet balancing algorithm.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/6850) an error that rendered a database with a large number of tables or partitions inoperable during recovery from a backup. Now, when database size limits are exceeded, the recovery operation will fail, and the database will continue to operate normally.
* [Implemented](https://github.com/ydb-platform/ydb/pull/11532) a mechanism that forcibly triggers background [compaction](./concepts/glossary#compaction) when discrepancies are detected between the data schema and the data stored in [DataShard](./concepts/glossary#data-shard). This resolves a rare issue with delays in data schema changes.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/10447) duplication of authentication tickets, which led to an increased number of requests to authentication providers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9377) an invariant violation error during the initial CDC scan that caused the ydbd server process to crash.
* [Prohibited](https://github.com/ydb-platform/ydb/pull/9446) changing the schema of backup tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9509) a hang of the initial CDC scan during frequent table updates.
* [Excluded](https://github.com/ydb-platform/ydb/pull/9934) dropped indexes from the limit count for the [maximum number of indexes](https://ydb.tech/docs/en/concepts/limits-ydb#schema-object).
* [Fixed](https://github.com/ydb-platform/ydb/pull/8847) an [error](https://github.com/ydb-platform/ydb/issues/6985) in displaying the time scheduled for executing a set of transactions (scheduled step).
* [Fixed](https://github.com/ydb-platform/ydb/pull/9161) a [problem](https://github.com/ydb-platform/ydb/issues/8942) with interruption of blue–green deployment in large clusters, caused by frequent updates to the node list.
* [Fixed](https://github.com/ydb-platform/ydb/pull/8925) a rare error that led to a violation of transaction execution order.
* [Fixed](https://github.com/ydb-platform/ydb/pull/9841) an [error](https://github.com/ydb-platform/ydb/issues/9797) in the EvWrite API that led to incorrect memory deallocation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/10698) a [problem](https://github.com/ydb-platform/ydb/issues/10674) with volatile transactions hanging after a restart.
* Fixed a CDC error that in some cases led to increased CPU consumption, up to one core per CDC partition.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/11061) read latency that occurred during and after splitting some partitions.
* Fixed errors when reading data from S3.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4793) the method for calculating the AWS signature when accessing S3.
* Fixed false positives of the HealthCheck system during backup of a database with a large number of shards.

## Version 24.2 {#24-2}

Release date: August 20, 2024.

### Functionality

* Added the ability to [set priorities](./devops/deployment-options/manual/maintenance.md) for maintenance tasks in the [cluster management system](./concepts/glossary#cms).
* Added [configuration of stable names](reference/configuration/node_broker_config.md#node-broker-config) for cluster nodes within a tenant.
* Added retrieval of nested groups from the [LDAP server](./security/authentication.md#ldap), improved host parsing in the [LDAP configuration](reference/configuration/auth_config.md#ldap-auth-config), and added a setting to disable built-in authentication by login and password.
* Added the ability to authenticate [dynamic nodes](./concepts/glossary#dynamic) using an SSL certificate.
* Implemented removal of inactive nodes from [Hive](./concepts/glossary#hive) without restarting it.
* Improved management of inflight pings during Hive restart in large clusters.
* [Changed](https://github.com/ydb-platform/ydb/pull/6381) the order of establishing connections to nodes during Hive restart.

### YDB UI

* [Added](https://github.com/ydb-platform/ydb/pull/7485) the ability to set a TTL for a user session in the configuration file.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1028) sorting by `CPUTime` in the query list table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7779) precision loss when working with `double`, `float`.
* Supported [directory creation from the UI](https://github.com/ydb-platform/ydb-embedded-ui/issues/958).
* [Added the ability](https://github.com/ydb-platform/ydb-embedded-ui/pull/976) to set the background data refresh interval on all pages.
* [Improved](https://github.com/ydb-platform/ydb-embedded-ui/issues/955) ACL display.
* Enabled autocomplete in the query editor by default.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/834) View support.

### Bug fixes

* Added a check for the size of a local transaction before committing it to fix [errors](https://github.com/ydb-platform/ydb/issues/6677) in schema operations when exporting/backing up large databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7709) the [error](https://github.com/ydb-platform/ydb/issues/7674) of duplicating SELECT query results when reducing the quota in [DataShard](./concepts/glossary#data-shard).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6461) [errors](https://github.com/ydb-platform/ydb/issues/6220) occurring when the [coordinator](./concepts/glossary#coordinator) state changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5992) errors occurring during the initial scan of [CDC](./dev/cdc).
* [Fixed](https://github.com/ydb-platform/ydb/pull/6615) a race condition in asynchronous change delivery (async indexes, CDC).
* [Fixed](https://github.com/ydb-platform/ydb/pull/5993) a rare error where deletion by [TTL](./concepts/ttl) caused the process to terminate unexpectedly.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5760) an error in displaying the PDisk status in the [CMS](./concepts/glossary#cms) interface.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6008) errors that could cause a soft drain of tablets from a node to hang.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6445) an error stopping the interconnect proxy on a node running without restarts when another node is added to the cluster.
* [Fixed](https://github.com/ydb-platform/ydb/pull/6695) accounting of free memory in [interconnect](./concepts/glossary#actor-system-interconnect).
* [Fixed](https://github.com/ydb-platform/ydb/issues/6405) UnreplicatedPhantoms/UnreplicatedNonPhantoms counters in VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/issues/6398) handling of empty garbage collection requests on VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5894) management of TVDiskControls settings via CMS.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5883) error loading data created by newer versions of VDisk.
* [Fixed](https://github.com/ydb-platform/ydb/pull/5862) error executing a query `REPLACE INTO` with a default value.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7714) error executing queries that performed multiple left joins to a single string table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/7740) loss of precision for `float`, `double` types when using CDC.


## Version 24.1 {#24-1}

Release date: July 31, 2024.

### Functionality

* Implemented [Knn UDF](./yql/reference/udf/list/knn.md) for exact nearest vector search.
* Developed the gRPC service QueryService, which enables execution of all query types (DML, DDL) and retrieval of unlimited data volumes.
* Implemented [integration with the LDAP protocol](./security/authentication.md) and the ability to obtain a list of groups from external LDAP directories.

### Built-in UI

* Added a resource consumption diagnostics dashboard located on the database information tab, which allows you to determine the current state of consumption of the main resources: CPU cores, RAM, and space in the network distributed storage.
* Added charts for monitoring the main cluster performance indicators {{ ydb-short-name }}.

### Performance

* [Optimized](https://github.com/ydb-platform/ydb/pull/1837) timeouts of coordination service sessions from server to client. Previously, the timeout was 5 seconds, which in the worst case led to detecting a non-working client (and releasing resources held by it) within 10 seconds. In the new version, the check time depends on the session wait time, which provides faster response during leader changes or distributed lock acquisition.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2391) CPU consumption by [SchemeShard](./concepts/glossary.md#scheme-shard) replicas, especially when processing fast updates for tables with a large number of partitions.

### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/3917) an error of possible queue overflow; [Change Data Capture](./dev/cdc.md) reserves change queue capacity during initial scan.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4597) a potential deadlock between retrieving CDC records and sending them.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2056) an issue of losing the mediator task queue when reconnecting the mediator; the fix allows processing the mediator task queue during resynchronization.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2624) a rare error where, with volatile transactions enabled and in use, a successful transaction commit result was returned before the transaction was actually committed. Volatile transactions are disabled by default and are under development.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2839) a rare error that led to the loss of established locks and to successful commits of transactions that should have failed with the Transaction Locks Invalidated error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3074) a rare error that could lead to a violation of data integrity guarantees during concurrent writes and reads of data by a specific key.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4343) an issue that caused read replicas to stop processing requests.
* [Fixed](https://github.com/ydb-platform/ydb/pull/4979) a rare error that could cause database processes to crash in the presence of uncommitted transactions on a table at the time of its rename.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3632) an error in the logic for determining the status of a static group, where a static group was not marked as down even though it should have been.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error of partial commit of a distributed transaction with uncommitted changes in the event of certain races with restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2374) anomalies with reading stale data that were [detected using Jepsen](https://blog.ydb.tech/hardening-ydb-with-jepsen-lessons-learned-e3238a7ef4f2).


## Version 23.4 {#23-4}

Release date: May 14, 2024.

### Performance

* [Fixed](https://github.com/ydb-platform/ydb/pull/3638) an issue of increased CPU consumption by the topic actor `PERSQUEUE_PARTITION_ACTOR`.
* [Optimized](https://github.com/ydb-platform/ydb/pull/2083) resource usage by SchemeBoard replicas. The greatest effect is noticeable when modifying metadata of tables with a large number of partitions.

### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/2169) an error of possible incomplete commit of accumulated changes when using distributed transactions. This error occurs under an extremely rare combination of events, including a restart of tablets serving the table partitions involved in the transaction.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/3165) a race between table merge and garbage collection processes, due to which garbage collection could fail with an invariant violation error and, as a result, crash the server process `ydbd`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2696) an error in Blob Storage, due to which information about a change in the storage group composition might not be delivered in a timely manner to individual cluster nodes. As a result, in rare cases, read and write operations on data stored in the affected group could be blocked, requiring manual administrator intervention.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3002) an error in Blob Storage that could prevent data storage nodes from starting with a correct configuration. The error occurred on systems with the experimental feature "blob depot" explicitly enabled (this feature is disabled by default).
* [Fixed](https://github.com/ydb-platform/ydb/pull/2475) an error that occurred in some situations when writing to a topic with an empty `producer_id` with deduplication disabled. It could lead to an emergency termination of the server process `ydbd`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/2651) an issue that caused the process `ydbd` to crash due to an erroneous state of a topic write session.
* [Fixed](https://github.com/ydb-platform/ydb/pull/3587) an error in displaying the metric for the number of partitions in a topic; previously, an incorrect value was displayed.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/2126) memory leaks that occurred when copying topic data between clusters {{ ydb-short-name }}. They could lead to termination of server processes `ydbd` due to exhaustion of available RAM.

## Version 23.3 {#23-3}

Release date: October 12, 2023.

### Functionality

* Implemented visibility of own changes within transactions. Previously, when attempting to read data already modified by the current transaction, the query failed with an error. This made it necessary to order reads and writes within a transaction. With the advent of own changes visibility, these restrictions are lifted, and queries can read rows modified in the current transaction.
* Added support for [column-oriented tables](concepts/datamodel/table.md#column-tables). Column-oriented tables are well suited for analytical queries (Online Analytical Processing), as only the columns directly involved in the query are read during execution. YDB column-oriented tables allow creating analytical reports with performance comparable to specialized analytical DBMSs.
* Added support for [Kafka API for topics](reference/kafka-api/index.md). Now YDB topics can be accessed via a Kafka-compatible API designed for migrating existing applications. Support for Kafka protocol version 3.4.0 is provided.
* Added the ability to [write to a topic without deduplication](concepts/datamodel/topic.md#no-dedup). This type of write is well suited for cases where the order of message processing is not critical. Writing without deduplication is faster and consumes fewer server resources, but ordering and deduplication of messages on the server do not occur.
* In YQL, added the ability to [create](yql/reference/syntax/create-topic.md), [alter](yql/reference/syntax/alter-topic.md), and [drop](yql/reference/syntax/drop-topic.md) topics.
* Added the ability to grant and revoke access rights using YQL commands [GRANT](yql/reference/syntax/grant.md) and [REVOKE](yql/reference/syntax/revoke.md).
* Added the ability to log DML operations in the audit log.
* **_(Experimental)_** When writing messages to a topic, you can now pass metadata. To enable this functionality, add `enable_topic_message_meta: true` to the [configuration file](reference/configuration/index.md).
* **_(Experimental)_** Added support for [reading from topics](reference/ydb-sdk/topic.md#read-tx) and writing to a table within a single transaction. The new functionality simplifies the scenario of moving data from a topic to a table. To enable it, add `enable_topic_service_tx: true` to the configuration file.
* **_(Experimental)_** Added support for PostgreSQL compatibility. The new mechanism allows you to run SQL queries in the PostgreSQL dialect on YDB infrastructure using the PostgreSQL network protocol. You can use familiar PostgreSQL tools such as psql and drivers (pq for Golang and psycopg2 for Python), as well as develop queries using familiar PostgreSQL syntax with YDB horizontal scalability and fault tolerance.
* **_(Experimental)_** Added support for [federated queries](concepts/query_execution/federated_query/index.md). It allows you to get information from various data sources without moving them to YDB. Interaction with ClickHouse, PostgreSQL, and S3 is supported via YQL queries without duplicating data between systems.

### Built-in UI

* A new option has been added to the query type selector settings `PostgreSQL`, which is available when the parameter `Enable additional query modes` is enabled. Also, the query history now takes into account the syntax used when executing the query.
* The YQL query template for creating a table has been updated. A description of the available parameters has been added.
* Sorting and filtering for the Storage and Nodes tables has been moved to the server. You need to enable the parameter `Offload tables filters and sorting to backend` in the experiments section to use this functionality.
* Buttons for creating, modifying, and deleting [topics](concepts/datamodel/topic.md) have been added to the context menu.
* Sorting by severity has been added for all issues in the tree in `Healthcheck`.

### Performance

* Iterator reads have been implemented. The new functionality allows you to separate reads and computations. Iterator reads allow datashards to increase the throughput of read queries.
* Write performance to YDB topics has been optimized.
* Improved tablet balancing under node overload.

### Bug fixes

* Fixed an error of possible blocking of snapshots by read iterators that coordinators are not aware of.
* Fixed a memory leak when closing a connection in the kafka proxy.
* Fixed an error where snapshots taken via read iterators might not be restored on restarts.
* Fixed an incorrect residual predicate for the condition `IS NULL` on a column.
* Fixed the triggered check `VERIFY failed: SendResult(): requirement ChunksLimiter.Take(sendBytes) failed`.
* Fixed `ALTER TABLE` for `TTL` for column-oriented tables.
* Implemented `FeatureFlag` that allows disabling/enabling work with `CS` and `DS`.
* Fixed the coordinator time difference between 23-2 and 23-3 by 50 ms.
* Fixed an error where the handle `storage` returned extra groups when the query parameter `node_id` in `viewer backend`.
* Added `usage` filter to `/storage` in `viewer backend`.
* Fixed an error in Storage v2 where an incorrect number was returned in `Degraded`.
* Fixed cancellation of subscriptions from sessions in iterator reads during a tablet restart.
* Fixed an error where, during a rolling restart when going through the balancer, `healthcheck` alerts about storage blink.
* Updated metrics `cpu usage` in ydb.
* Fixed ignoring `NULL` when specifying `NOT NULL` in the table schema.
* Implemented output of operation records `DDL` to the common log.
* Implemented a prohibition for the command `ydb table attribute add/drop` to work with any objects other than tables.
* Disabled `CloseOnIdle` for `interconnect`.
* Fixed the read speed doubling in the UI.
* Fixed an error where data could be lost on `block-4-2`.
* Added a topic name check.
* Fixed a possible `deadlock` in the actor system.
* Fixed the test `KqpScanArrowInChanels::AllTypesColumns`.
* Fixed the test `KqpScan::SqlInParameter`.
* Fixed concurrency issues for OLAP queries.
* Fixed insertion of `ClickBench parquet`.
* Added the missing call `CheckChangesQueueOverflow` to the common `CheckDataTxReject`.
* Fixed an error of returning an empty status when calling `ReadRows API`.
* Fixed incorrect export retry in the final stage.
* Fixed an issue with an infinite quota on the number of records in a CDC topic.
* Fixed an error importing the column `string` and `parquet` into the column `string` OLAP.
* Fixed a crash `KqpOlapTypes.Timestamp` under tsan.
* Fixed a crash in `viewer backend` when attempting to execute a query to the database due to version incompatibility.
* Fixed an error where `viewer` did not return a response from `healthcheck` due to a timeout.
* Fixed an error where an incorrect value could be saved in Pdisks `ExpectedSerial`.
* Fixed an error where database nodes crash due to `segfault` in the S3 actor.
* Fixed a race in `ThreadSanitizer: data race KqpService::ToDictCache-UseCache`.
* Fixed a race in `GetNextReadId`.
* Fixed overestimation of the result `SELECT COUNT(*)` immediately after import.
* Fixed a bug where `TEvScan` could return an empty dataset when a datashard was split.
* Added a separate issue/error code for when available space is exhausted.
* Fixed a bug `GRPC_LIBRARY Assertion failed`.
* Fixed a bug where reading by a secondary index in scan queries returned an empty result.
* Fixed validation `CommitOffset` in `TopicAPI`.
* Reduced consumption of `shared cache` when approaching OOM.
* Merged the scheduler logic from `data executer` and `scan executer` into a single class.
* Added handles `discovery` and `proxy` to the execution process of `query` in `viewer backend`.
* Fixed a bug where the handle `/cluster` returns the name of the root domain of type `/ru` in `viewer backend`.
* Implemented a seamless table update scheme for `QueryService`.
* Fixed a bug where `DELETE` returned data and did NOT delete it.
* Fixed a bug in the operation of `DELETE ON` in `query service`.
* Fixed unexpected disabling of batching in default schema settings.
* Fixed the triggered check `VERIFY failed: MoveUserTable(): requirement move.ReMapIndexesSize() == newTableInfo->Indexes.size()`.
* Increased the default gRPC streaming timeout.
* Excluded unused messages and methods from `QueryService`.
* Added sorting by `Rack` in `/nodes` in `viewer backend`.
* Fixed a bug where a query with sorting returns an error when sorting in descending order.
* Fixed the interaction of `QP` with `NodeWhiteboard`.
* Removed support for old parameter formats.
* Fixed a bug where `DefineBox` was not applied to disks that have a static group.
* Fixed a bug `SIGSEGV` in dynamic nodes when importing `CSV` via `YDB CLI`.
* Fixed a crash when processing `NGRpcService::TRefreshTokenImpl`.
* Implemented a `gossip` protocol for exchanging information about cluster resources.
* Fixed a bug `DeserializeValuePickleV1(): requirement data.GetTransportVersion() == (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0 failed`.
* Implemented auto-increment columns.
* Use the status `UNAVAILABLE` instead of `GENERIC_ERROR` when a shard identification error occurs.
* Added support for `rope payload` in `TEvVGet`.
* Added ignoring of outdated events.
* Fixed a crash of write sessions on an invalid topic name.
* Fixed a bug `CheckExpected(): requirement newConstr failed, message: Rewrite error, missing Distinct((id)) constraint in node FlatMap`.
* Enabled `safe heal` by default.

## Version 23.2 {#23-2}

Release date: August 14, 2023.

### Functionality

* **_(Experimental)_** Visibility of own changes is implemented. When this feature is enabled, you can read changed values from the current transaction that has not yet been committed. This functionality also allows you to perform multiple modifying operations in a single transaction on a table with secondary indexes. To enable this functionality, add `enable_kqp_immediate_effects: true` to the section `table_service_config` in the [configuration file](reference/configuration/index.md).
* **_(Experimental)_** Iterator reads are implemented. This functionality allows you to separate reads and computations from each other. Iterator reads allow datashards to increase the throughput of read queries. To enable this functionality, add `enable_kqp_data_query_source_read: true` to the section `table_service_config` in the [configuration file](reference/configuration/index.md).

### Built-in UI

* Navigation improved:
  * Buttons for switching between diagnostics and development modes are moved to the left panel.
  * Breadcrumbs added to all pages.
  * On the database page, information about storage groups and database nodes is moved to tabs.
* History and saved queries are moved to tabs above the query editor.
* On the Info tabs for schema objects, settings are displayed in terms of the construct `CREATE` or `ALTER`.
* Support for displaying [columnar tables](concepts/datamodel/table.md#column-table) in the schema tree.

### Performance

* For scanning queries, the ability to efficiently search for individual rows using the primary key or secondary indexes is implemented, which in many cases can significantly improve performance. As in regular queries, to use a secondary index, you must explicitly specify its name in the query text using the keyword `VIEW`.

* **_(Experimental)_** Added the ability to manage the system tablets of a database (SchemeShard, Coordinators, Mediators, SysViewProcessor) by its own Hive, instead of the root Hive, and to do this immediately at the moment of creating a new database. Without this flag, the system tablets of a new database are created in the root Hive, which can negatively affect its load. Enabling this flag makes databases completely isolated by load, which can be especially relevant for installations consisting of a hundred or more nodes. To enable this functionality, add `alter_database_create_hive_first: true` to the section `feature_flags` in the [configuration file](reference/configuration/index.md).

### Bug fixes

* Fixed an error in the actor system autoconfiguration, as a result of which all load falls on the system pool.
* Fixed an error leading to a full scan when searching by a primary key prefix via `LIKE`.
* Fixed errors when interacting with datashard replicas.
* Fixed memory errors in columnar tables.
* Fixed errors when processing conditions for immediate transactions.
* Fixed an error in iterator reads on datashard replicas.
* Fixed an error that caused an avalanche of data delivery session reinstallation to async indexes
* Fixed optimizer errors in scan queries
* Fixed an error in incorrect calculation of hive storage consumption after database expansion
* Fixed an error of hanging operations from non-existent iterators
* Fixed errors when reading a range on a `NOT NULL` column
* Fixed an error of VDisk replication hanging
* Fixed an error in the operation of the option `run_interval` in TTL

## Version 23.1 {#23-1}

Release date May 5, 2023. To update to version 23.1, go to the [Downloads](downloads/index.md#ydb-server) section.

### Functionality

* Added [initial table scan](concepts/cdc.md#initial-scan) when creating a CDC change feed. Now you can upload all data that exists at the time of feed creation.
* Added the ability to [atomically replace an index](dev/secondary-indexes.md#atomic-index-replacement). Now you can atomically and transparently for the application replace one index with another pre-created index. The replacement is performed without downtime.
* Added [audit log](security/audit-log.md) — an event stream that contains information about all operations on objects {{ ydb-short-name }}.

### Performance

* Improved data transfer formats between query execution stages, which sped up SELECT on queries with parameters by 10%, and on write operations by up to 30%.
* Added [automatic configuration](reference/configuration/index.md) of actor system pools depending on their load. This improves performance through more efficient sharing of CPU resources.
* Optimized predicate application logic — execution of constraints using OR and IN with parameters is automatically moved to the DataShard side.
* (Experimental) For scan queries, the ability to efficiently search for individual rows using the primary key or secondary indexes has been implemented, which in many cases significantly improves performance. As in regular queries, to use a secondary index, you must explicitly specify its name in the query text using the keyword `VIEW`.
* Implemented caching of the computation graph during query execution, which reduces CPU consumption during its construction.

### Bug fixes

* Fixed a number of errors in the implementation of the distributed data storage. We strongly recommend that all users update to the current version.
* Fixed an error in building an index on not null columns.
* Fixed statistics calculation with MVCC enabled.
* Fixed errors with backups.
* Fixed a race condition during split and deletion of a table with CDC.

## Version 22.5 {#22-5}

Release date: March 7, 2023. To update to version **22.5**, go to [Downloads](downloads/index.md#ydb-server).

### What's new

* Added [change stream configuration parameters](yql/reference/syntax/alter_table/changefeed.md) to pass additional information about changes to a topic.
* Added support for [renaming tables](concepts/datamodel/table.md#rename) with TTL enabled.
* Added [management of record retention time](concepts/cdc.md#retention-period) for the change stream.

### Bug fixes and improvements

* Fixed an error when inserting 0 rows with the BulkUpsert operation.
* Fixed an error when importing Date/DateTime columns from CSV.
* Fixed an error when importing data from CSV with a line break.
* Fixed an error when importing data from CSV with empty values.
* Improved Query Processing performance (WorkerActor replaced with SessionActor).
* DataShard compaction now starts immediately after split or merge operations.

## Version 22.4 {#22-4}

Release date: October 12, 2022. To update to version **22.4**, go to [Downloads](downloads/index.md#ydb-server).

### What's new

* {{ ydb-short-name }} Topics and Change Data Capture (CDC):

  * A new Topic API is introduced. A [topic](concepts/datamodel/topic.md) {{ ydb-short-name }} is an entity for storing unstructured messages and delivering them to various subscribers.
  * Support for the new Topic API has been added to the [{{ ydb-short-name }} CLI](reference/ydb-cli/topic-overview.md) and [SDK](reference/ydb-sdk/topic.md). The Topic API provides methods for streaming writing and reading messages, as well as managing topics.
  * Added the ability to [capture table data changes](concepts/cdc.md) by sending change messages to a topic.

* SDK:

  * Added the ability to interact with topics in {{ ydb-short-name }} SDK.
  * Added official support for the database/sql driver for working with {{ ydb-short-name }} in Golang.

* Embedded UI:

  * The CDC change stream and secondary indexes are now displayed in the database schema hierarchy as separate objects.
  * Improved visualization of the graphical representation of query explain plans.
  * Problematic storage groups are now more visible.
  * Various improvements based on UX research.

* Query Processing:

  * Added Query Processor 2.0, a new subsystem for executing OLTP queries with significant improvements over the previous version.
  * Write performance improved by up to 60%, read performance by up to 10%.
  * Added the ability to enable the NOT NULL constraint for primary keys in YDB when creating tables.
  * Enabled support for renaming a secondary index online without stopping the service.
  * Improved the query explain representation, which now includes graphs for physical operators.

* Core:

  * Added support for a consistent snapshot for read-only transactions that does not conflict with write transactions.
  * Added support for BulkUpsert for tables with asynchronous secondary indexes.
  * Added support for TTL for tables with asynchronous secondary indexes.
  * Added support for compression when exporting data to S3.
  * Added an audit log for DDL statements.
  * Added support for authentication with static credentials.
  * Added system views for query performance diagnostics.
