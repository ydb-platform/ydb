# {{ ydb-short-name }} Server changelog

## Version 26.3 {#26-3} {#26-3}

### Release candidate 26.3.1.16 {#26-3-1-16-rc} {#26-3-1-16-rc}

Release date: 18.09.26

#### Functionality

* [Backup export and import are available for column-oriented tables, including S3-compatible storage](./recipes/backup/backup-collections/exporting-to-external-storage.md?version=main).
* Column-oriented table columns support [dictionary encoding](./yql/reference/syntax/create_table/index.md?version=v26.3#encoding). Use `ENCODING(DICT)` for low-cardinality values.
* [Local min_max indexes](./dev/min_max-skip-index.md?version=v26.3) are enabled for column-oriented tables. They skip data fragments outside a query range, reducing the amount of data read.
* Added [storage group decommissioning through virtual storage groups](./maintenance/manual/virtual_storage_groups_decommit.md?version=v26.3). Data moves to virtual groups in the background while applications continue reading and writing data.
* Added [authentication through external OpenID Connect identity providers](./security/authentication.md?version=v26.3#external-idp). {{ ydb-short-name }} validates JWT tokens using the provider's JSON Web Key Set (JWKS) and periodically refreshes authentication data.
* Kafka API supports [mutual TLS authentication](./reference/kafka-api/auth.md?version=v26.3). A client certificate is mapped to a security identifier and SASL authentication is not required.
* Columnar engine optimization: for column-oriented tables, an updated compaction strategy is used that organizes data more efficiently, and a new data merging strategy when reading that speeds up queries on constantly changing data.
* Authentication and authorization subsystem optimization: bulk authorization requests to AccessService are enabled by default, reducing authorization request overhead.
* Streaming YQL queries can access system virtual attributes such as `__ydb_create_time`, `__ydb_write_time`, and others, as well as user attributes `__ydb_user_attributes`. [Feature documentation](./concepts/query_execution/topics.md?version=v26.3#system-metadata).
* Distributed Storage optimization: full VDisk synchronization is faster because processed SyncLog data is removed locally by default.
* Added transfer metrics and statistics to `DescribeTransfer` for monitoring and diagnostics.
* Added a configurable limit for stored forced-compaction operations. Completed and cancelled operations can be removed automatically when the limit is reached.
* Change Data Capture records can include the [OpenTelemetry trace ID](./concepts/cdc.md?version=v26.3#record-structure) of the request that produced the change.
* [Topic reads that start from a timestamp](./reference/ydb-cli/topic-read.md?version=v26.3) filter out messages with earlier write timestamps, including messages stored in the same blob as newer messages.

#### Disabled functionality

The following functionality is not enabled by default.

* For column-oriented tables, `ALTER TABLE ... COMPACT` can start forced compaction.
* Column-oriented and row-oriented tables now have parity in the set of YQL data types (`Interval`, `Uuid`, and `DyNumber` are supported).
* Added [hybrid search](./dev/hybrid-search.md?version=v26.3), combining full-text relevance and vector similarity into one ranked result.
* Topics can be accessed through the [Amazon SQS API](./reference/sqs-api/index.md?version=v26.3), allowing SQS-compatible clients to read and write messages.
* Added [JSON indexes](./dev/json-indexes.md?version=v26.3) for accelerating `JSON_EXISTS` and `JSON_VALUE` queries.
* Full-text indexes support [filter columns](./dev/fulltext-indexes.md?version=v26.3#filtered), allowing search within a logical table partition.
* Full-text indexes can be created for tables with [arbitrary primary-key types](./dev/fulltext-indexes.md?version=v26.3#primary-key).

## Version 26.2 {#26-2} {#26-2}

### Version 26.2.1.14 {#26-2-1-14} {#26-2-1-14}

Release date: September 16, 2026.

#### Functionality

* [Full-text indexes](./dev/fulltext-indexes.md?version=v26.2) are enabled by default.
* [Streaming queries](./dev/streaming-query/index.md?version=v26.2) can read from local topics, write to local topics, read local tables, and contain multiple `INSERT` statements.
* Streaming queries support [watermarks](./dev/streaming-query/watermarks.md?version=v26.2).
* Added [Bloom skip indexes](./dev/bloom-skip-indexes.md?version=v26.2): Bloom and Bloom n-gram indexes for column-oriented tables, and prefix Bloom indexes for row-oriented tables.
* [Column compression](./yql/reference/syntax/create_table/index.md?version=v26.2) settings for column-oriented tables are available by default.
* The [parallelism level](./yql/reference/syntax/alter_table/indexes.md?version=v26.2) can now be configured for index builds.
* For row-oriented tables, [`ALTER TABLE`](./yql/reference/syntax/alter_table/columns.md?version=v26.2) statements `ALTER COLUMN SET DEFAULT` and `ALTER COLUMN DROP DEFAULT` are available by default.
* For row-oriented tables, the YQL statement [`TRUNCATE TABLE`](./yql/reference/syntax/truncate-table.md?version=v26.2) is available by default.
* The YQL statement [`DISCARD SELECT`](./yql/reference/syntax/discard.md?version=v26.2) is available by default.
* QueryService can return query results in [Apache Arrow format](./reference/ydb-sdk/data-formats/format-arrow.md?version=v26.2); this capability is enabled by default.
* Added forced [compaction](./yql/reference/syntax/alter_table/compact.md?version=v26.2) for row-oriented tables using `ALTER TABLE ... COMPACT`.
* Added automatic storage balancing between groups and background validation of disk placement.
* Table split and merge operations are faster for tables with many partitions: SchemeShard updates only the affected partitions instead of rebuilding the entire partition list.
* Added [audit logging](./security/audit-log.md?version=v26.2) for topic operations.
* Added [built-in minidump collection based on Google Breakpad](./devops/observability/minidumps.md?version=v26.2) for Linux nodes.
* Added the [`ydb-dstool pdisk populate`](./reference/ydb-dstool/pdisk-populate.md?version=v26.2) subcommand for reproducing a PDisk workload on another device.

#### Disabled functionality

This functionality is present in the core to allow rollback from the future 26-3 release, but is not enabled by default. It will be enabled by default in the next major release. It may also be enabled in some managed YDB services.

* Added support for [incremental backups](./concepts/datamodel/backup-collection.md?version=v26.2), which store only changes relative to the preceding backup in a collection.
* [Column-oriented tables](./recipes/backup/import-export-column-tables.md?version=v26.2) can be exported and imported using S3-compatible storage.
* Added [export and import of row-oriented tables](./reference/ydb-cli/export-import/export-nfs.md?version=main) using a local file system, including file systems mounted over NFS.
* Added snapshot retention for long-running analytical queries over column-oriented tables, preventing snapshot data from being removed before a query completes.
* QueryService can notify SDKs when a node or session is shutting down, allowing clients to stop sending new queries there.
* Added database-level limits on the count and volume of small blobs for column-oriented tables. New writes are rejected when the hard limit is exceeded.
* [Watermark](./dev/streaming-query/watermarks.md?version=v26.2) expressions can be evaluated outside the context of an individual message.
* Added [min-max skip indexes](./yql/reference/syntax/create_table/min_max_index.md?version=v26.2) for column-oriented tables.
* Added [dictionary encoding](./yql/reference/syntax/create_table/index.md?version=v26.2#encoding) for columns in column-oriented tables.
* Added online construction of unique secondary indexes.
* Transactions between topics and tables can use optimized conflict checking.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/46747) incorrect results from some scan queries over column-oriented tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/50358) handling of malformed Kafka requests that could cause excessive memory use or out-of-bounds access.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49929) topic reads hanging after a read balancer restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35470) race conditions in the server-side topic read session and in the [Topic SDK](https://github.com/ydb-platform/ydb/pull/42213).
* [Fixed](https://github.com/ydb-platform/ydb/pull/50897) a crash and a [hang](https://github.com/ydb-platform/ydb/pull/50621) in streaming query checkpointing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/50379) race conditions when cancelling and planning distributed transactions.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49469) handling of oversized blocks during encrypted export and a [false data corruption error](https://github.com/ydb-platform/ydb/pull/48986) during encrypted restore.
* [Fixed](https://github.com/ydb-platform/ydb/pull/49460) a race condition when collecting statistics with `ydb workload topic`.
* [Fixed](https://github.com/ydb-platform/ydb/pull/48174) a double free during `DqHashCombine` spilling teardown.
* [Fixed](https://github.com/ydb-platform/ydb/pull/40912) lost `ReadSet` acknowledgements that could prevent a transaction involving topics from completing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/40801) handling of `NODATA` responses in the KeyValue API: `NOT_FOUND` or `INTERNAL_ERROR` is returned instead of terminating the process.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41895) IAM authentication for external data sources in Generic Provider and [error handling](https://github.com/ydb-platform/ydb/pull/40761) for provider responses.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41411) a memory leak when loading external data source metadata.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46739) parsing of tri-state feature flags in YAML configuration that could cause subsequent settings to be lost.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41009) copying and exporting tables with secondary indexes after index implementation tables were removed.
* [Fixed](https://github.com/ydb-platform/ydb/pull/45958) object filtering and list operations during file-system export.
* [Fixed](https://github.com/ydb-platform/ydb/pull/47591) Kafka API Metadata responses that could return an empty broker list or an inconsistent controller ID, causing Kafka AdminClient and Kafka Streams operations to time out.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46033) a leak of script execution records created by streaming queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/42277) `local-ydb` overwriting a user-provided `config.yaml` mounted at the default path during the first Docker deployment.
* [Fixed](https://github.com/ydb-platform/ydb/pull/44011) a Hive crash on restart when a tablet lock and its persisted leader pointed to different nodes.

## Version 26.1 {#26-1} {#26-1}

### Version 26.1.1.22 {#26-1-1-22} {#26-1-1-22}

Release date: July 27, 2026.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/46894) Kafka API authentication for local users: with enabled `DomainLoginOnly` setting, users could not access tenant databases.
* [Fixed](https://github.com/ydb-platform/ydb/pull/46946) a crash (use-after-free) when updating a vector index caused by asynchronous ReadActor destruction.

### Version 26.1.1.20 {#26-1-1-20} {#26-1-1-20}

Release date: July 2, 2026.

#### Functionality

* YQL statements [`SHOW CREATE TABLE`](./yql/reference/syntax/show_create.md?version=v26.1) and [`SHOW CREATE VIEW`](./yql/reference/syntax/show_create.md?version=v26.1) return the DDL required to recreate a table or view.
* `ALTER TABLE` supports default values in [`ADD COLUMN`](./yql/reference/syntax/alter_table/columns.md?version=v26.1) (`DEFAULT`).
* [Shuffle Elimination](./concepts/query_execution/optimizer.md?version=v26.1) is enabled in production: the optimizer can remove unnecessary data shuffles in joins.
* [Backup and restore](./reference/ydb-cli/export-import/file-structure.md?version=v26.1) now cover [asynchronous replications](./concepts/async-replication.md?version=v26.1), [external data sources](./concepts/datamodel/external_data_source.md?version=v26.1), [external tables](./concepts/datamodel/external_table.md?version=v26.1), and [transfers](./concepts/transfer.md?version=v26.1).
* The cluster keeps running when [CMS](./concepts/glossary.md?version=v26.1#cms) is unavailable.
* [Dynamic nodes](./devops/configuration-management/configuration-v1/node-authorization.md?version=v26.1) can be registered using client TLS certificates.
* [LDAP service account authentication](./security/authentication.md?version=v26.1) supports the SASL EXTERNAL mechanism — see [`enable_sasl_external_bind`](./reference/configuration/auth_config.md?version=v26.1#ldap-auth-config).
* [Asynchronous replication](./concepts/async-replication.md?version=v26.1) mirroring supports [auto-partitioned topics](./concepts/datamodel/topic.md?version=v26.1#autopartitioning); see also [topic partitions in CDC](./concepts/cdc.md?version=v26.1#topic-partitions).
* [TLI](./reference/configuration/tli_config.md?version=v26.1) (Transaction Lock Invalidation) diagnostics were extended: `tli_config`, [logging](./troubleshooting/performance/queries/tli-logging.md?version=v26.1), and [system views](./dev/system-views.md?version=v26.1#top-tli-partitions).
* [Load-based auto-partitioning](./concepts/datamodel/table.md?version=v26.1#auto_partitioning_by_load) considers CPU load on the partition leader and all its replicas.
* [Streaming queries](./dev/streaming-query/index.md?version=v26.1) support [writing results to local tables](./dev/streaming-query/table-writing.md?version=v26.1).
* [Changefeeds (CDC)](./concepts/cdc.md?version=v26.1) can export user security identifiers (`USER_SIDS`) — see [`ALTER TABLE` `CHANGEFEED`](./yql/reference/syntax/alter_table/changefeed.md?version=v26.1).
* [External data sources](./concepts/datamodel/external_data_source.md?version=v26.1) support `AUTH_METHOD=IAM`.
* CLI supports token file authentication (`--token-file`).
* Transaction handling between topics and tables has been optimized.
#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/35003) query execution in Workload Manager after tenant recreation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35187) a use-after-free in the gRPC service layer.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35663) incremental restore surviving SchemeShard restarts and shard failures.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35787) streaming query metadata missing immediately after creation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/35793) async checkpointing stalling when input is full and the checkpoint is empty.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36217) cross-database quota interference in the Kesus quoter proxy.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36220) hangs in PQ read sessions.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36292) scan executor hangs on `SELECT … LIMIT` over empty tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36692) TLI deferred `LOCKS_BROKEN` flush accounting.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37130) streaming query timeout overflow.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37145) thread safety and token expiry parsing in the IAM credentials provider.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37285) errors in the HTTP gateway.
* [Fixed](https://github.com/ydb-platform/ydb/pull/37668) integer overflow when handling null passwords.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38033) CDC collection disabled for `IsBuildInProgress` columns.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38490) segfault during updates.
* [Fixed](https://github.com/ydb-platform/ydb/pull/38544) Shuffle Elimination with the `HashJoinMode` pragma.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39337) duplicated rows in scan queries after delivery issues.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39687) viewer HTTP endpoints restricted to viewer/admin SIDs; database scope enforced for database-only tokens.
* [Fixed](https://github.com/ydb-platform/ydb/pull/39798) OOM when loading trash on blob depot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/41681) `TQueryBase` crash after cancelling a streaming query.
* [Fixed](https://github.com/ydb-platform/ydb/pull/43068) local CDC reads from YQL.
* [Fixed](https://github.com/ydb-platform/ydb/pull/44340) quick remote cancellation in the query service.

## Version 25.4 {#25-4}

### Version 25.4.1.15 {#25-4-1-15}

Release date: June 5, 2026.

#### Functionality

* YQL statements [`BATCH UPDATE`](./yql/reference/syntax/batch-update.md?version=v25.4) and [`BATCH DELETE FROM`](./yql/reference/syntax/batch-delete.md?version=v25.4) are available for bulk updates and deletes in tables.
* The write execution path has changed substantially: writes now run in streaming mode without fully materializing data on the Query Processor side before sending it to DataShards, which improves performance for large write workloads. The change applies to a subset of scenarios; in some cases (for example, tables with secondary indexes) the previous approach is still used. For an overview of the execution pipeline, see [Query execution](./concepts/query_execution/index.md?version=v25.4).
* Lookup Join execution was optimized: it uses a streaming mode without materializing one side of the join, which lowers peak memory use, speeds up queries over large datasets, and removes previous limits on join side sizes. See [Index lookup Join](./faq/yql.md?version=v25.4#index-lookup-join) and the [`JOIN`](./yql/reference/syntax/select/join.md?version=v25.4) operator syntax.
* You can now configure access permissions for cluster and database [system views](./devops/observability/system-views.md?version=v25.4).
* Row-oriented tables support configurable [cache modes](./concepts/datamodel/table.md?version=v25.4#cache-modes), including a new `in_memory` mode that preloads table data into RAM when sufficient memory is available.
* Topic consumers gained an [`availability-period`](./reference/ydb-cli/topic-consumer-add.md?version=v25.4) parameter that extends retention of uncommitted messages beyond `retention-period`.
* [Per-partition topic metrics and export to user shard quotas](./reference/observability/metrics/index.md?version=v25.4#topics_partitions) are available for accounting and observability.
* Faster queries with `LIMIT` on column-oriented tables through early result limiting on storage nodes (for queries with no sort or with sort by primary key). See [`LIMIT` and `OFFSET`](./yql/reference/syntax/select/limit_offset.md?version=v25.4) in YQL.
* Column-oriented tables support the `Bool` type in schema and queries — see [primitive YQL types](./yql/reference/types/primitive.md?version=v25.4#numeric).
* A [filtered vector index](./dev/vector-indexes.md?version=v25.4#filtered) correctly returns rows inserted after the index was created when filter column values are new.
* Stream processing and data delivery are more tightly integrated into the core: [topic → table transfer](./concepts/transfer.md?version=v25.4); [streaming queries](./dev/streaming-query/index.md?version=v25.4) are available to users when `EnableStreamingQueries` is enabled.
* The `overlap_clusters` option substantially improves vector search quality by placing vectors in multiple index clusters (index settings) — see [Vector indexes](./dev/vector-indexes.md?version=v25.4).
* Vector index search is significantly faster across all index types because distances are computed locally on each DataShard before data is sent over the network — see [VIEW (vector index)](./yql/reference/syntax/select/vector_index.md?version=v25.4) and [Vector indexes](./dev/vector-indexes.md?version=v25.4).
* Full vector search without an ANN index is faster thanks to pushdown (vector search, KNN UDF) — see [Vector search](./concepts/query_execution/vector_search.md?version=v25.4) and the [KNN](./yql/reference/udf/list/knn.md?version=v25.4) module.
* Database-stored secrets are fully supported (create, alter, drop, and use) — see [Secrets](./concepts/datamodel/secrets.md?version=v25.4). Note that the [legacy syntax](./concepts/datamodel/secrets.md?version=v25.3) is deprecated.
* [`UNION ALL`](./yql/reference/syntax/select/union.md?version=v26.2#union-all) execution was improved with parallel execution, improving performance of analytical queries.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) an [LDAP authentication](./security/authentication.md) vulnerability: knowing the login and password of any LDAP user (including one who is not a member of a group allowed to access {{ ydb-short-name }}), an attacker could bypass group membership checks and gain access to the cluster (LDAP search filter injection; special characters are now escaped per RFC 2254).

## Version 25.3 {#25-3}

### Version 25.3.1.27 {#25-3-1-27}

Release date: May 20, 2026.

#### Functionality

* Added support for two–data center configuration with synchronous data writes (Bridge mode). Available in {{ ydb-short-name }} Enterprise.
* Topic improvements:
  * In Kafka API [compacted](https://docs.confluent.io/kafka/design/log_compaction.html#ak-log-compaction) topics can now be created, and YDB automatically creates and removes the internal service consumer used for topic compaction;
  * Topic APIs were extended with new `DescribeConsumer` and [per-partition topic metrics can now be exported into user quotas](./reference/observability/metrics/index.md#topics).
* Implemented [backup and restore](./reference/ydb-cli/export-import/file-structure.md?version=v25.3#topics) of topic configuration to and from S3;
* Implemented [backup and restore](./reference/ydb-cli/export-import/file-structure.md#views) (`VIEW`) to S3 and from S3.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) an [LDAP authentication](./security/authentication.md) vulnerability: knowing the login and password of any LDAP user (including one who is not a member of a group allowed to access {{ ydb-short-name }}), an attacker could bypass group membership checks and gain access to the cluster (LDAP search filter injection; special characters are now escaped per RFC 2254).
* [Fixed](https://github.com/ydb-platform/ydb/pull/33758) an issue that caused a server-side session leak.
* [Fixed](https://github.com/ydb-platform/ydb/pull/36926) an issue where, in rare cases, reads from a table could block its deletion.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20238) a race condition when updating the CPU soft limit.
* [Fixed behavior](https://github.com/ydb-platform/ydb/pull/18121), where `ALTER TABLE` could fail for tables with a vector index.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18088) nconsistent results in some read-write transactions — conflicting writes no longer overwrite uncommitted changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/18234) serializability violations in read-write transactions after shard restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20560) a memory management issue when committing offsets in topics with automatic partitioning enabled.
* [Added](https://github.com/ydb-platform/ydb/pull/18698) checks for enabled encryption in zero-copy transfers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20519) an issue that could cause a VDisk to hang in local recovery after a ChunkRead error.
* [Eliminated](https://github.com/ydb-platform/ydb/pull/18924) phantom VDisk appearances caused by race conditions between group creation and deletion operations.
* [Improved](https://github.com/ydb-platform/ydb/pull/17687) phantom VDisks caused by races between group creation and deletion operations.
* When a session ends via attach stream, a notification is now [sent](https://github.com/ydb-platform/ydb/pull/22298).
* The coordination service now correctly [returns](https://github.com/ydb-platform/ydb/pull/16901) `SCHEME_ERROR` for non-existent resources instead of the incorrect `INTERNAL_ERROR` code.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20157) memory handling issues and internal data consistency violations in Workload Manager and related scheduler code.
* [Fixed](https://github.com/ydb-platform/ydb/pull/20432) an issue where PDisk info requests could time out when the target node was disabled or unavailable.

## Version 25.2 {#25-2}

### Version 25.2.1.26 {#25-2-1-26}

Release date: May 12, 2026.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) an [LDAP authentication](./security/authentication.md) vulnerability: knowing the login and password of any LDAP user (including one not in a group with access to {{ ydb-short-name }}), an attacker could bypass group membership checks and gain access to the cluster (injection into the LDAP user search filter; added escaping of special characters per RFC 2254).
* [Fixed](https://github.com/ydb-platform/ydb/pull/25112) an [issue](https://github.com/ydb-platform/ydb/issues/23858) where [tablet](./concepts/glossary.md#tablet) deletion might get stuck.
* [Fixed](https://github.com/ydb-platform/ydb/pull/25145) an [issue](https://github.com/ydb-platform/ydb/issues/20866) that caused an error when changing a table's follower.
* Fixed a couple of [changefeed](./concepts/glossary.md#changefeed) related issues:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25689) an [issue](https://github.com/ydb-platform/ydb/issues/25524) where importing a table with a Utf8 primary key and an enabled changefeed could fail.
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25453) an [issue](https://github.com/ydb-platform/ydb/issues/25454) where importing a table without changefeeds could fail due to incorrect changefeed file lookup.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26069) an [issue](https://github.com/ydb-platform/ydb/issues/25869) that could cause errors during UPSERT operations in column tables.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26504) an [error](https://github.com/ydb-platform/ydb/issues/26225) that could cause a crash due to accessing freed memory.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26657) an [issue](https://github.com/ydb-platform/ydb/issues/23122) with duplicates in unique secondary index.
* [Fixed](https://github.com/ydb-platform/ydb/pull/26879) an [issue](https://github.com/ydb-platform/ydb/issues/26565) with checksum mismatch error on restoration compressed backup from S3.
* [Fixed](https://github.com/ydb-platform/ydb/pull/27528) an [issue](https://github.com/ydb-platform/ydb/issues/27193) where some queries from the TPC-H 1000 benchmark could fail.
* Fixed a couple of cluster bootstrap related issues:
  * [Fixed](https://github.com/ydb-platform/ydb/pull/25678) an [issue](https://github.com/ydb-platform/ydb/issues/25023) where cluster bootstrap could hang when mandatory authorization was enabled.
  * [Fixed](https://github.com/ydb-platform/ydb/pull/28886) an [issue](https://github.com/ydb-platform/ydb/issues/27228) where it was impossible to create new databases for several minutes immediately after cluster deployment.
* [Fixed](https://github.com/ydb-platform/ydb/pull/28655) an [issue](https://github.com/ydb-platform/ydb/issues/28510) where race condition could occur and clients receive `Could not find correct token validator` error when mising newly issued tokens before `LoginProvider` state is updated.
* [Fixed](https://github.com/ydb-platform/ydb/pull/29940) an [issue](https://github.com/ydb-platform/ydb/issues/29903) where named expression containing another named expression caused incorrect `VIEW` backup.

### Release candidate 25.2.1.10 {#25-2-1-10-rc}

Release date: September 21, 2025.

#### Functionality

* [Analytical capabilities](./concepts/analytics/index.md) are available by default: [column-oriented tables](./concepts/datamodel/table.md#column-oriented-tables) can be created without special flags, using LZ4 compression and hash partitioning. Supported operations include a wide range of DML operations (UPDATE, DELETE, UPSERT, INSERT INTO ... SELECT) and CREATE TABLE AS SELECT. Integration with dbt, Apache Airflow, Jupyter, Superset, and federated queries to S3 enables building end-to-end analytical pipelines in YDB.
* Cost optimizer is enabled by default for queries that use at least one columnar table, but can be forced for other queries as well. The cost optimizer improves query performance by calculating the optimal order and type of joins based on table statistics; supported hints allow fine-tuning execution plans for complex analytical queries.
* Data transfer — an asynchronous mechanism for moving data from a topic to a table — has been implemented. Creating, modifying, and deleting a transfer instance is done using YQL. For a quick start, use the instruction with an example.
* Spilling, a memory management mechanism, has been added. With spilling, intermediate data generated during query execution that exceeds the available node RAM is temporarily offloaded to external storage. Spilling enables processing of user queries that require handling large datasets exceeding node memory capacity.
* The maximum time for executing a single query has been increased from 30 minutes to 2 hours.
* Support for Certificate Authority (CA) and Yandex Cloud Identity and Access Management (IAM) authentication in asynchronous replication has been added.

Mandatory to configure:
* Authentication and authorization of nodes for registering nodes in the cluster.

Enabled by default:
* Vector index for approximate vector search;
* Support for client-side reader balancing, compacted topics, and transactions in YDB Topics Kafka API;
* Support for auto-partitioning topics in CDC for row-oriented tables;
* Support for auto-partitioning topics for asynchronous replication;
* Support for parameterized Decimal type;
* Support for DateTime64 type;
* Automatic cleanup of temporary directories and tables during export to S3;
* Support for changefeeds in backup and restore operations;
* The ability to enable followers (read replicas) for covered secondary indexes;
* System views with history of overloaded partitions.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/24265) CPU resource limiting for column-oriented tables in Workload Manager. Previously, CPU consumption could exceed the configured limits.

## Version 25.1 {#25-1}

### Version 25.1.4.18 {#25-1-4-18}

Release date: May 12, 2026.

#### Functionality

* [Added](https://github.com/ydb-platform/ydb/pull/21119) support for the Kafka frameworks, such as Kafka Connect, Kafka Streams, Confluent Schema Registry, Kafka Streams, Apache Flink, etc. Now [YDB Topics Kafka API](./reference/kafka-api/index.md) supports the following features:
  * client-side consumer balancing. To enable it, use the `enable_kafka_native_balancing` flag in the [cluster configuration](./reference/configuration/index.md). For for information, see [How consumer balancing works in Apache Kafka](https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/). When enabled, consumer balancing will work the same way in YDB Topics.
  * [compacted topics](https://docs.confluent.io/kafka/design/log_compaction.html). To enable topic compaction, use the `enable_topic_compactification_by_key` flag.
  * [transactions](https://www.confluent.io/blog/transactions-apache-kafka/). To enable transactions, use the `enable_kafka_transactions` flag.
* [Added](https://github.com/ydb-platform/ydb/pull/20982) a [new protocol](https://github.com/ydb-platform/ydb/issues/11064) to [Node Broker](./concepts/glossary.md#node-broker) that eliminates the long startup of nodes on large clusters (more than 1000 servers).

#### YDB UI

* [Fixed](https://github.com/ydb-platform/ydb/pull/17839) an [issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/18615) where not all tablets are shown for pers queue group on the tablets tab in diagnostics.
* Fixed an [issue](https://github.com/ydb-platform/ydb/issues/18735) where the storage tab on the diagnostics page displayed nodes of other types in addition to storage nodes.
* Fixed a [serialization issue](https://github.com/ydb-platform/ydb-embedded-ui/issues/2164) that caused an error when opening query execution statistics.
* Changed the logic for nodes transitioning to critical state — the CPU pool, which is 75-99% full, now triggers a warning, not a critical state.

#### Performance

* [Optimized](https://github.com/ydb-platform/ydb/pull/20197) processing of empty inputs when performing JOIN operations.

#### Bug fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/38425) an [LDAP authentication](./security/authentication.md) vulnerability: knowing the login and password of any LDAP user (including one not in a group with access to {{ ydb-short-name }}), an attacker could bypass group membership checks and gain access to the cluster (injection into the LDAP user search filter; added escaping of special characters per RFC 2254).
* [Added support](https://github.com/ydb-platform/ydb/pull/21918) for a new kind of change record in asynchronous replication — `reset` record (in addition to `update` & `erase` records).
* [Fixed](https://github.com/ydb-platform/ydb/pull/21836) an [issue](https://github.com/ydb-platform/ydb/issues/21814) where a replication instance with an unspecified `COMMIT_INTERVAL` option caused the process to crash.
* [Fixed](https://github.com/ydb-platform/ydb/pull/21652) rare errors when reading from a topic during partition balancing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22455) an [issue](https://github.com/ydb-platform/ydb/issues/19842) where dedicated database deletion might leave database system tablets improperly cleaned.
* [Fixed](https://github.com/ydb-platform/ydb/pull/22203) an [issue](https://github.com/ydb-platform/ydb/issues/22030) that caused tablets to hang when nodes experienced critical memory shortage. Now tablets will automatically start as soon as any of the nodes frees up sufficient resources.
* [Fixed](https://github.com/ydb-platform/ydb/pull/24278) an issue where only the first message from a batch was saved when writing Kafka messages, with all other messages in the batch being ignored.

### Release candidate 25.1.2.7 {#25-1-2-7-rc}

Release date: July 14, 2025.

#### Functionality

* [Implemented](https://github.com/ydb-platform/ydb/issues/19504) a [vector index](./dev/vector-indexes.md?version=v25.1) for approximate vector similarity search.
* [Added](https://github.com/ydb-platform/ydb/issues/11454) support for [consistent asynchronous replication](./concepts/async-replication.md?version=v25.1).
* Added [configuration mechanism V2](./devops/configuration-management/configuration-v2/config-overview?version=v25.1) that simplifies the deployment of new {{ ydb-short-name }} clusters and further work with them. [Comparison](./devops/configuration-management/compare-configs?version=v25.1) of configuration mechanisms V1 and V2.
* Added support for the parameterized [Decimal type](./yql/reference/types/primitive.md?version=v25.1#numeric).
* [Added](https://github.com/ydb-platform/ydb/pull/8065) the ability to omit the `DECLARE` operator for query parameter type declarations. Parameter types are now automatically inferred from the provided values.
* [Implemented](https://github.com/ydb-platform/ydb/issues/18017) client balancing of partitions when reading using the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) (like Kafka itself). Previously, balancing took place on the server. This mode is enabled by setting the `enable_kafka_native_balancing` flag in the cluster configuration.
* Added support for [auto-partitioning topics](./concepts/cdc.md?version=v25.1#topic-partitions) for row-oriented tables in CDC. This mode is enabled by setting the `enable_topic_autopartitioning_for_cdc` flag in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/8264) the ability to [alter the retention period of CDC topics](./concepts/cdc.md?version=v25.1#topic-settings) using the `ALTER TOPIC` statement.
* [Added support](https://github.com/ydb-platform/ydb/pull/7052) for [the DEBEZIUM_JSON format](./concepts/cdc.md?version=v25.1#debezium-json-record-structure) for CDC.
* [Added](https://github.com/ydb-platform/ydb/pull/19507) the ability to create changefeed streams to index tables.
* [Added](https://github.com/ydb-platform/ydb/issues/19310) the ability to [enable followers (read replicas)](./yql/reference/syntax/alter_table/indexes.md?version=v25.1) for covered secondary indexes. This mode is enabled by setting the `enable_access_to_index_impl_tables` flag in the cluster configuration.
* The scope of supported objects in backup and restore operations has been expanded:
  * [Support for changefeeds](https://github.com/ydb-platform/ydb/issues/7054) (enabled with the `enable_changefeeds_export` and `enable_changefeeds_import` flags).
  * [Support for views](https://github.com/ydb-platform/ydb/issues/12724) (enabled with the `enable_view_export` flag).
* [Added](https://github.com/ydb-platform/ydb/issues/17734) automatic cleanup of temporary tables and directories during export to S3. This mode is enabled by setting the `enable_export_auto_dropping` flag in the cluster configuration.
* [Added](https://github.com/ydb-platform/ydb/pull/12909) automatic integrity checks of backups during import, which prevent restoration from corrupted backups and protect against data loss.
* [Added](https://github.com/ydb-platform/ydb/pull/15570) the ability to create views that refer to [UDFs](./yql/reference/builtins/basic?version=v25.1#udf) in queries.
* Added system views with information about [access right settings](./dev/system-views.md?version=v25.1#auth), [history of overloaded partitions](./dev/system-views.md?version=v25.1#top-overload-partitions) - enabled by setting the `enable_followers_stats` flag in the cluster configuration, [history of partitions with broken locks](./dev/system-views?version=v25.1#top-tli-partitions).
* Added new parameters to the [CREATE USER](./yql/reference/syntax/create-user.md?version=v25.1) and [ALTER USER](./yql/reference/syntax/alter-user.md?version=v25.1) operators:
  * `HASH` — sets a password in encrypted form.
  * `LOGIN` and `NOLOGIN` — unlocks and blocks a user, respectively.
* Enhanced account security:
  * [Added](https://github.com/ydb-platform/ydb/pull/11963) user [password complexity](./reference/configuration/?version=v25.1#password-complexity) verification.
  * [Implemented](https://github.com/ydb-platform/ydb/pull/12578) [automatic user lockout](./reference/configuration/?version=v25.1#account-lockout) after a specified number of failed attempts to enter the correct password.
  * [Added](https://github.com/ydb-platform/ydb/pull/12983) the ability for users to change their own passwords.
* [Implemented](https://github.com/ydb-platform/ydb/issues/9748) the ability to toggle functional flags at runtime. Changes to flags that do not specify `(RequireRestart) = true` in the [proto file](https://github.com/ydb-platform/ydb/blob/main/ydb/core/protos/feature_flags.proto#L60) are applied without a cluster restart.
* [Changed](https://github.com/ydb-platform/ydb/pull/11329) lock behavior when shard locks exceed the limit. Once the limit is exceeded, the oldest locks (rather than the newest) are converted into full-shard locks.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12567) a mechanism to preserve optimistic locks in memory during graceful datashard restarts, reducing `ABORTED` errors caused by lock loss during table balancing.
* [Implemented](https://github.com/ydb-platform/ydb/pull/12689) a mechanism to abort volatile transactions with the `ABORTED` status during graceful datashard restarts.
* [Added](https://github.com/ydb-platform/ydb/pull/6342) support for removing `NOT NULL` constraints from a table column using the `ALTER TABLE ... ALTER COLUMN ... DROP NOT NULL` statement.
* [Added](https://github.com/ydb-platform/ydb/pull/9168) a limit of 100,000 concurrent session-creation requests in the coordination service.
* [Increased](https://github.com/ydb-platform/ydb/pull/14219) the maximum number of columns in the primary key from 20 to 30.
* Improved diagnostics and introspection of memory errors ([#10419](https://github.com/ydb-platform/ydb/pull/10419), [#11968](https://github.com/ydb-platform/ydb/pull/11968)).
* **_(Experimental)_** [Added](https://github.com/ydb-platform/ydb/pull/14075) an experimental mode with strict access control checks. This mode is enabled by setting these flags:
  * `enable_strict_acl_check` — do not allow granting rights to non-existent users and delete users with permissions;
  * `enable_strict_user_management` — enables strict checks for local users (i.e. only the cluster or database administrator can administer local users);
  * `enable_database_admin` — add the role of database administrator;

#### Backward Incompatible Changes

* If you are using queries that access named expressions as tables using the AS_TABLE function, update [temporary over YDB](https://github.com/yandex/temporal-over-ydb) to version [v1.23.0-ydb-compat](https://github.com/yandex/temporal-over-ydb/releases/tag/v1.23.0-ydb-compat) before updating {{ ydb-short-name }} to the current version to avoid errors in query execution.

#### YDB UI

* Query Editor was redesigned to [support partial results load](https://github.com/ydb-platform/ydb-embedded-ui/pull/1974) — it starts displaying results when receives a chunk from the server, doesn't have to wait until the query completion. This approach allows application developers to see query results faster.
* [Security Improvement](https://github.com/ydb-platform/ydb-embedded-ui/pull/1967): controls that are could not be activated by current user due to lack of permissions are not displayed. Users won't click and experience Access Denied error.
* [Added](https://github.com/ydb-platform/ydb-embedded-ui/pull/1981) search by tablet id on Tablets tab.
* HotKeys help tab accessible by ⌘+K key is added.
* Operations tab is added to Database page. Operations allow to list operations and cancel them.
* Cluster dashboard redesign and make it collapsable.
* JsonViewer: handle case sensitive search.
* Added code snippets for YDB SDK to connect to selected database. Such snippets must speed up development.
* Rows on Queries tab were sorted by string values after proper backend sort.
* QueryEditor: removed extra confirmation requests on leaving browser page — do not ask confirmation when it's irrelevant.
* Implemented case-sensitive search support in the JSON hierarchical display tool.
* Added code examples for connecting via YDB SDK to the top panel after selecting a database, which speeds up the development process.
* Fixed row sorting in the Queries tab.
* Removed unnecessary confirmation prompts when closing the browser tab in the query editor — confirmation is now requested only when necessary.

#### Performance

* [Added](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0726) support for constant folding in the query optimizer by default, which improves query performance by computing constant expressions at compile time.
* [Added](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0730) a new granular timestamps protocol, which will reduce the execution time of distributed transactions (slowing down one shard will no longer slow down all).
* [Implemented](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0732) the functionality to save the state of data shards in memory during restarts, which preserves locks and increases the chances of successful transaction execution. This reduces the execution time of long transactions by decreasing the number of retries.
* [Implemented](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0734) pipelined processing of internal transactions in [Node Broker](concepts/glossary.md#node-broker), which sped up the launch of dynamic nodes in the cluster.
* [Improved](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0739) the stability of Node Broker under high load from cluster nodes.
* [Enabled](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0741) by default unloadable B-Tree indexes instead of non-unloadable SST indexes, which reduces memory consumption when storing "cold" data.
* [Optimized](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0743) memory consumption by storage nodes.
* [Reduced](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0745) Hive startup time by 30%.
* [Optimized](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0747) the replication process in the distributed storage.
* [Optimized](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0749) the size of the header for large binary objects in VDisk.
* [Reduced](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0751) memory consumption by cleaning up allocator pages.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0753) an issue with Interconnect configuration that led to performance degradation.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0757) the "Out of memory" error when deleting very large tables by regulating the number of tablets processing the operation simultaneously.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0759) an error that occurred when specifying the same database node multiple times in the configuration for system tablets.
* [Resolved](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0761) the issue of long (seconds) data reading during frequent table resharding operations.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0763) an error in reading from asynchronous replicas that caused a failure.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0765) rare freezes during the initial scan of [CDC](concepts/cdc.md).
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0769) the handling of unfinished schema transactions in data shards during system restart.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0771) the inconsistent reading from a topic when trying to explicitly acknowledge a message read within a transaction. Now, attempting to acknowledge the message will result in an error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0773) an error that caused auto-partitioning to work incorrectly when working with a topic in a transaction.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0775) transaction freezes when working with topics during tablet restarts.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0777) the "Key is out of range" error when importing from an S3-compatible storage.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0779) incorrect detection of the end of the metadata field in the cluster configuration.
* [Improved](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0781) the building of secondary indexes: the system now retries on certain errors instead of interrupting the process.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0783) an error executing an expression in queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0788) the issue of the "Drop Tablet" operation hanging in PQ tablets, especially during Interconnect delays.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0790) an error occurring during VDisk compaction.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0794) an issue where long topic-reading sessions ended with "too big inflight" errors.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0796) a freeze when reading a topic if at least one partition had no incoming data but was being read by multiple consumers.
* [Resolved](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0798) a rare issue with PQ tablet restarts.
* [Resolved](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0800) an issue where, after updating the cluster version, Hive started subscribers in data centers without running database nodes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0802) an issue occurring during version updates.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0805) an error that led to a segmentation fault when a healthcheck request and a cluster-node disable request executed simultaneously.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0807) a partitioning issue with row-oriented tables when selecting a split key from access samples containing mixed operations with the full key and key prefix (e.g., exact reads or range reads).
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0811) an issue where topic auto-partitioning did not work when the configuration parameter was set using an expression.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0815) an issue where the column list was returned in the wrong order compared to the table creation order.

## Version 24.4 {#24-4}

### Version 24.4.4.12 {#24-4-4-12}

Release date: June 3, 2025.

#### Performance

* [Limited](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0822) the number of simultaneously processed configuration changes.
* [Optimized](https://github.com/ydb-platform/ydb/issues/YDBDOC_PROTECTED_0824) memory consumption by PQ tablets.
* [Optimized](https://github.com/ydb-platform/ydb/issues/YDBDOC_PROTECTED_0826) CPU consumption by the Scheme shard, reducing query response delays. The limit on the number of Scheme shard operations is now checked before split and merge operations are performed.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0828) a rare issue where client applications froze during transaction commit when partition deletion occurred before write quota update.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0830) an error in copying tables with the Decimal type that caused failures when rolling back to a previous version.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0832) an issue where a commit without topic write confirmation led to blocking of the current and subsequent topic transactions.
* Fixed transaction freezes when working with topics during tablet [restart](https://github.com/ydb-platform/ydb/issues/YDBDOC_PROTECTED_0836) or [deletion](https://github.com/ydb-platform/ydb/issues/YDBDOC_PROTECTED_0838).
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0840) issues with reading messages larger than 6 MB via [Kafka API](reference/kafka-api).
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0846) a memory leak during writing to a [topic](concepts/glossary.md#topic).
* Fixed errors in processing [nullable columns](https://github.com/ydb-platform/ydb/issues/YDBDOC_PROTECTED_0850) and [columns with UUID type](https://github.com/ydb-platform/ydb/issues/YDBDOC_PROTECTED_0852) in row tables.

### Version 24.4.4.2 {#24-4-4-2}

Release date: April 15, 2025.

#### Functionality

* Enabled by default:
  * support for [views](concepts/datamodel/view.md);
  * auto-partitioning mode for topics;
  * transactions involving topics and row-oriented tables;
  * volatile distributed transactions.
* Added the ability to read and write to a topic using the Kafka API without authentication.

#### Performance

* Enabled by default automatic secondary index selection for queries.

#### Bug Fixes

* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0871) an error that led to a significant decrease in reading speed from tablet followers.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0875) an error where volatile distributed transactions sometimes waited for confirmations until the next reboot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0877) a rare assertion failure (server process crash) when followers attached to leaders with an inconsistent snapshot.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0879) a rare datashard crash when a dropped table shard is restarted with uncommitted persistent changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0881) an error that could disrupt the order of message processing in a topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0883) a rare error that could stop reading from a topic partition.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0885) an issue where a transaction could hang if a user performed a control plane operation on a topic (e.g., adding partitions or a consumer) while the PQ tablet is moving to another node.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0887) an issue with the userInfo counter value leak, which could lead to a "too big in flight" error.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0890) a proxy crash due to duplicate topics in a request.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0892) a rare bug where a user could write to a topic without any account quota being applied or consumed.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0894) an issue where topic deletion returned "OK" while the topic tablets persisted in a functional state. To remove such tablets, follow the instructions from the [pull request](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0896).
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0898) a rare issue that prevented the restoration of a backup for a large secondary indexed table.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0900) an issue that caused errors when inserting data using UPSERT into row-oriented tables with default values.
* [Resolved](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0903) a bug that caused failures when executing queries to tables with secondary indexes that returned result lists using the RETURNING * expression.

## Version 24.3 {#24-3}

### Version 24.3.15.5 {#24-3-15-5}

Release date: February 6, 2025.

#### Functionality

* Added the ability to register a [database node](concepts/glossary.md#database-node) using a certificate. In [Node Broker](concepts/glossary.md#node-broker), the `AuthorizeByCertificate` flag has been added to enable certificate-based registration.
* [Added](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0915) priorities for authentication ticket verification through a [third-party IAM provider](security/authentication.md#iam), with the highest priority given to requests from new users. Tickets in the cache update their information with a lower priority.

#### Performance

* [Improved](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0919) tablet startup time on large clusters: 210 ms → 125 ms (SSD), 260 ms → 165 ms (HDD).

#### Bug Fixes

* [Removed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0925) the restriction on writing values greater than 127 to the Uint8 type.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0927) an issue where reading small messages from a topic in small chunks significantly increased CPU load, which could lead to delays in reading and writing to the topic.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0929) an issue with restoring from a backup stored in S3 with path-style addressing.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0931) an issue with restoring from a backup that was created during an automatic table split.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0933) an issue with Uuid serialization for [CDC](concepts/cdc.md).
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0938) an issue with ["frozen" locks](contributor/datashard-locks-and-change-visibility.md#interaction-with-distributed-transactions), which could be caused by bulk operations (e.g., TTL-based deletions).
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0942) an issue where reading from a follower of tablets sometimes caused crashes during automatic table splits.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0944) an issue where the [coordination node](concepts/datamodel/coordination-node.md) successfully registered proxy servers despite a connection loss.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0948) an issue that occurred when opening the Embedded UI tab with information about [distributed storage groups](concepts/glossary.md#storage-group).
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0952) an issue where Health Check did not report time synchronization issues.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0958) a rare issue that caused errors during read queries.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0960) a rare issue that led to leaks of uncommitted changes.
* [Fixed](https://github.com/ydb-platform/ydb/pull/YDBDOC_PROTECTED_0962) consistency issues related to caching deleted ranges.

### Version 24.3.11.14 {#24-3-11-14}

Release date: January 9, 2025.
* [(https://github.com/ydb-platform/ydb/pull/11276) Supported restart without loss of cluster availability in [(https://ydb.tech/docs/ru/concepts/topology#reduced) minimal fault-tolerant configuration of three nodes.
* [(https://github.com/ydb-platform/ydb/pull/13218) Added new UDF Roaring bitmap functions: AndNotWithBinary, FromUint32List, RunOptimize.

### Version 24.3.11.13 {#24-3-11-13}

Release date: December 24, 2024.

#### Functionality

* [(https://ydb.tech/docs/ru/reference/observability/tracing/setup) Added query tracing — a tool that allows you to see in detail the path a query takes through the distributed system.
* [(https://ydb.tech/docs/ru/concepts/async-replication) Added support for asynchronous replication, which allows you to synchronize data between YDB databases almost in real time. It can also be used to migrate data between databases with minimal downtime for applications working with them.
* [(https://ydb.tech/docs/ru/concepts/datamodel/view) Added support for views (VIEW), which can be enabled by the cluster administrator using the `enable_views` `enable_views` [(https://ydb.tech/docs/ru/devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii) setting in the dynamic configuration.
* [(https://ydb.tech/docs/ru/concepts/query_execution/federated_query/) In federated queries, support for new external data sources has been added: MySQL, Microsoft SQL Server, Greenplum.
* [(https://ydb.tech/docs/ru/devops/deployment-options/manual/federated-queries/connector-deployment) Documentation has been developed for deploying YDB with federated query functionality (manually).
* For the YDB Docker container, a startup parameter `FQ_CONNECTOR_ENDPOINT` `FQ_CONNECTOR_ENDPOINT` has been added, allowing you to specify the address of the connector to external data sources. TLS encryption of the connection to the connector has been added. The ability to output the port of the connector service running locally on the same host as the dynamic YDB node has been added.
* [(https://ydb.tech/docs/ru/concepts/datamodel/topic#autopartitioning) Added support for auto-partitioning of topics, where topics can split partitions based on load while maintaining read message order and exactly once write guarantees. This can be enabled by the cluster administrator using the `enable_topic_split_merge` `enable_topic_split_merge` `enable_pqconfig_transactions_at_scheme_shard` `enable_pqconfig_transactions_at_scheme_shard` [(https://ydb.tech/docs/ru/devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii) settings in the dynamic configuration.
* [(https://ydb.tech/docs/ru/concepts/transactions#topic-table-transactions) Added transactions involving [(https://ydb.tech/docs/ru/concepts/datamodel/topic) topics and string tables. This allows you to transactionally move data between tables and topics, and between topics, so that data is not lost or duplicated. Transactions can be enabled by the cluster administrator using the `enable_topic_service_tx` `enable_topic_service_tx` `enable_pqconfig_transactions_at_scheme_shard` `enable_pqconfig_transactions_at_scheme_shard` [(https://ydb.tech/docs/ru/devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii) settings in the dynamic configuration.
* [(https://github.com/ydb-platform/ydb/pull/7150) Added support for CDC for synchronous secondary indexes.
* Added the ability to change the retention period for records in [(https://ydb.tech/docs/ru/concepts/cdc.md) CDC topics.
* Added support for auto-increment for columns included in the primary key of a table.
* Added logging of audit events for user logins in YDB, user session termination events in the user interface, and backup and restore requests.
* Added a system view that allows you to get information about sessions established with the database using a query.
* Added support for default constant values for columns in string tables.
* Added support for the `RETURNING` `RETURNING` clause in queries.
* [(https://ydb.tech/docs/ru/yql/reference/builtins/basic.md#version) Added the built-in `version()` function.
* [(https://github.com/ydb-platform/ydb/pull/8708) Added start and end times and authors to the metadata for backup and restore operations from S3-compatible storage.
* Added support for backing up and restoring ACL for tables from/to S3-compatible storage.
* Added paths and decompression methods to the query plan for reading from S3.
* Added new parsing options for `timestamp` `timestamp` `datetime` `datetime` fields when reading data from S3.
* Added support for the `Decimal` `Decimal` type in [(https://ydb.tech/docs/ru/dev/primary-key/column-oriented#klyuch-particionirovaniya) partitioning keys.
* Improved diagnostics for storage issues in HealthCheck.
* **_(Experimental)_** [(https://ydb.tech/docs/ru/concepts/query_execution/optimizer#stoimostnoj-optimizator-zaprosov) Added a cost-based optimizer for complex queries involving [(https://ydb.tech/docs/ru/concepts/glossary#column-oriented-table) column-oriented tables. The cost-based optimizer considers a large number of alternative execution plans for each query and selects the best one based on the cost estimate for each option. Currently, this optimizer only works with plans that contain [(https://ydb.tech/docs/ru/yql/reference/syntax/join) `JOIN` operations.
* **_(Experimental)_** [(https://ydb.tech/docs/ru/dev/resource-consumption-management) Initial version of the workload manager has been implemented. It allows you to create resource pools with CPU, memory, and active query count limits. Resource classifiers have been implemented to assign queries to specific resource pools.
* **_(Experimental)_** [(https://ydb.tech/docs/ru/dev/secondary-indexes#avtomaticheskoe-ispolzovanie-indeksov-pri-vyborke) Implemented automatic index selection for queries, which can be enabled using the `index_auto_choose_mode` `index_auto_choose_mode` `table_service_config` `table_service_config` [(https://ydb.tech/docs/ru/devops/configuration-management/configuration-v1/dynamic-config#obnovlenie-dinamicheskoj-konfiguracii) setting in the dynamic configuration.

#### YDB UI

* [(https://github.com/ydb-platform/ydb-embedded-ui/issues/782) Supported creating and viewing information on asynchronous replication instances.
* [(https://github.com/ydb-platform/ydb-embedded-ui/issues/929) Added an indicator for columns with auto-increment.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1438) Added a tab with information about [(https://ydb.tech/docs/ru/concepts/glossary#tablet) tablets.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1289) Added a tab with information about [(https://ydb.tech/docs/ru/concepts/glossary#storage-group) distributed storage groups.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1218) Added a setting to trace all queries and display tracing results.
* Enhanced the PDisk page with [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1069) attributes, information about disk space consumption, and a button to initiate [(https://ydb.tech/docs/ru/devops/deployment-options/manual/decommissioning) disk decommissioning.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1313) Added information about currently running queries.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1291) Added a row limit setting for query editor output and a notification when results exceed the limit.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1049) Added a tab to display top CPU-consuming queries over the last hour.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1127) Added a search control on the history and saved queries pages.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1117) Added the ability to cancel query execution.
* [(https://github.com/ydb-platform/ydb-embedded-ui/issues/944) Added a shortcut to save queries in the editor.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1422) Separated donor disks from other disks in the UI.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1154) Added support for InterruptInheritance ACL and improved visualization of active ACLs.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/889) Added a display of the current UI version.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1229) Added a tab with information about the status of settings for enabling experimental functionality.

#### Performance

* [(https://github.com/ydb-platform/ydb/pull/7589) Accelerated recovery of tables with secondary indexes from backups by up to 20% according to our tests.
* [(https://github.com/ydb-platform/ydb/pull/9721) Optimized Interconnect throughput.
* Improved the performance of CDC topics with thousands of partitions.
* Enhanced the Hive tablet balancing algorithm.

#### Bug fixes

* [(https://github.com/ydb-platform/ydb/pull/6850) Fixed an issue that caused databases with a large number of tables or partitions to become non-functional during restoration from a backup. Now, if database size limits are exceeded, the restoration operation will fail, but the database will remain operational.
* [(https://github.com/ydb-platform/ydb/pull/11532) Implemented a mechanism to forcibly trigger background [(https://ydb.tech/docs/ru/concepts/glossary#compaction) compaction when discrepancies between the data schema and stored data are detected in [(https://ydb.tech/docs/ru/concepts/glossary#data-shard) DataShard. This resolves a rare issue with delays in schema changes.
* [(https://github.com/ydb-platform/ydb/pull/10447) Resolved duplication of authentication tickets, which led to an increased number of requests to authentication providers.
* [(https://github.com/ydb-platform/ydb/pull/9377) Fixed an invariant violation issue during the initial scan of CDC, leading to an abnormal termination of the `ydbd` server process.
* [(https://github.com/ydb-platform/ydb/pull/9446) Prohibited schema changes for backup tables.
* [(https://github.com/ydb-platform/ydb/pull/9509) Fixed an issue with an initial scan freezing during CDC when the table is frequently updated.
* [(https://github.com/ydb-platform/ydb/pull/9934) Excluded deleted indexes from the count against the [(https://ydb.tech/docs/ru/concepts/limits-ydb#schema-object) maximum index limit.
* [(https://github.com/ydb-platform/ydb/pull/8847) Fixed a bug in the display of the scheduled execution time for a set of transactions (planned step).
* [(https://github.com/ydb-platform/ydb/pull/9161) Fixed a problem with interruptions in blue-green deployment in large clusters caused by frequent updates to the node list.
* [(https://github.com/ydb-platform/ydb/pull/8925) Resolved a rare issue that caused transaction order violations.
* [(https://github.com/ydb-platform/ydb/pull/9841) Fixed an issue in the EvWrite API that resulted in incorrect memory deallocation.
* [(https://github.com/ydb-platform/ydb/pull/10698) Resolved a problem with volatile transactions hanging after a restart.
* Fixed a bug in the CDC, which in some cases leads to increased CPU consumption, up to a core per CDC partition.
* [(https://github.com/ydb-platform/ydb/pull/11061) Eliminated read delays occurring during and after the splitting of certain partitions.
* Fixed issues when reading data from S3.
* [(https://github.com/ydb-platform/ydb/pull/4793) Corrected the calculation of the AWS signature for S3 requests.
* Resolved false positives in the HealthCheck system during database backups involving a large number of shards.

### Version 24.2 {#24-2}

Release date: August 20, 2024.

#### Functionality

* [(https://ydb.tech/docs/ru/devops/deployment-options/manual/maintenance.md) Added the ability to set maintenance task priorities in the [(https://ydb.tech/docs/ru/concepts/glossary#cms) cluster management system.
* [(reference/configuration/node_broker_config.md#node-broker-config) Added a setting to enable stable names for cluster nodes within a tenant.
* [(https://ydb.tech/docs/ru/security/authentication.md#ldap) Enabled retrieval of nested groups from the LDAP server, improved host parsing in the [(reference/configuration/auth_config.md#ldap-auth-config) LDAP configuration, and added an option to disable built-in authentication via login and password.
* [(https://ydb.tech/docs/ru/concepts/glossary#dynamic) Added support for authenticating dynamic nodes using SSL certificates.
* [(https://ydb.tech/docs/ru/concepts/glossary#hive) Implemented the removal of inactive nodes from Hive without a restart.
* Improved management of inflight pings during Hive restarts in large clusters.
* [(https://github.com/ydb-platform/ydb/pull/6381) Changed the order of establishing connections with nodes during Hive restarts.

#### YDB UI

* [(https://github.com/ydb-platform/ydb/pull/7485) Added the option to set a TTL for user sessions in the configuration file.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/1028) Added an option to sort the list of queries by `CPUTime` `CPUTime`.
* [(https://github.com/ydb-platform/ydb/pull/7779) Fixed precision loss when working with `double` `double` `float` `float` data types.
* [(https://github.com/ydb-platform/ydb-embedded-ui/issues/958) Added support for creating directories in the UI.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/976) Added an auto-refresh control on all pages.
* [(https://github.com/ydb-platform/ydb-embedded-ui/pull/955) Improved ACL display.
* Enabled autocomplete in the queries editor by default.
* Added support for views.

#### Bug fixes

* Added a check on the size of the local transaction prior to its commit to fix [(https://github.com/db-platform/ydb/issues/6677) errors in scheme shard operations when exporting/backing up large databases.
* [(https://github.com/ydb-platform/ydb/pull/7709) Fixed an issue with duplicate results in SELECT queries when reducing quotas in [(https://ydb.tech/docs/ru/concepts/glossary#data-shard) DataShard.
* [(https://github.com/ydb-platform/ydb/pull/6461) Fixed errors occurring during [(https://ydb.tech/docs/ru/concepts/glossary#coordinator) coordinator state changes.
* [(https://github.com/ydb-platform/ydb/pull/5992) Fixed issues during the initial CDC scan.
* [(https://github.com/ydb-platform/ydb/pull/6615) Resolved race conditions in asynchronous change delivery (asynchronous indexes, CDC).
* [(https://github.com/ydb-platform/ydb/pull/5993) Fixed a crash that sometimes occurred during [(https://ydb.tech/docs/ru/concepts/ttl) TTL-based deletions.
* [(https://github.com/ydb-platform/ydb/pull/5760) Fixed an issue with PDisk status display in the [(https://ydb.tech/docs/ru/concepts/glossary#cms) CMS.
* [(https://github.com/ydb-platform/ydb/pull/6008) Fixed an issue that might cause soft tablet transfers (drain) from a node to hang.
* [(https://github.com/ydb-platform/ydb/pull/6445) Resolved an issue with the interconnect proxy stopping on a node that is running without restarts. The issue occurred when adding another node to the cluster.
* [(https://github.com/ydb-platform/ydb/pull/6695) Fixed an issue with managing free memory in the [(https://ydb.tech/docs/ru/concepts/glossary#actor-system-interconnect) interconnect.
* [(https://github.com/ydb-platform/ydb/issues/6405) Corrected UnreplicatedPhantoms and UnreplicatedNonPhantoms counters in VDisk.
* [(https://github.com/ydb-platform/ydb/issues/6398) Fixed an issue with handling empty garbage collection requests on VDisk.
* Fixed the management of TVDiskControls settings through the CMS.
* Fixed the error of loading data created by newer versions of VDisk.
* Fixed the error when executing a query with a default value.
* Fixed the error of executing queries that performed several left joins to the same string table.
* Fixed the loss of precision for certain types when using CDC.

## Version 24.1 {#24-1}

Release date: July 31, 2024.

### Functionality

* Implemented Knn UDF for precise search of the nearest vectors.
* Developed a gRPC QueryService that allows executing all types of queries (DML, DDL) and retrieving unlimited amounts of data.
* Implemented integration with the LDAP protocol and the ability to retrieve a list of groups from external LDAP directories.

### Embedded UI

* Added a resource consumption diagnostics dashboard, located on the database information tab, which helps determine the current state of resource consumption: CPU cores, RAM, and space in the distributed network storage.
* Added graphs for monitoring the main cluster performance indicators {{ ydb-short-name }}.

### Performance

* Optimized session timeouts for the coordination service from server to client. Previously, the timeout was 5 seconds, which in the worst case led to identifying a non-working client (and releasing the resources it held) within 10 seconds. In the new version, the check time depends on the session wait time, which ensures faster response when changing the leader or acquiring distributed locks.
* Optimized CPU consumption by SchemeShard replicas, especially when processing fast updates for tables with a large number of partitions.

### Bug fixes

* Fixed a potential queue overflow error, where CDC reserves queue capacity for changes during the initial scan.
* Fixed a potential deadlock between obtaining CDC records and sending them.
* Fixed an issue with the loss of the mediator task queue during mediator reconnection, allowing the mediator task queue to be processed during resynchronization.
* Fixed a rare error where, with enabled and used volatile transactions, a successful transaction confirmation result was returned before the transaction was successfully committed. Volatile transactions are disabled by default and are under development.
* Fixed a rare error that led to the loss of established locks and successful confirmation of transactions that should have resulted in a Transaction Locks Invalidated error.
* Fixed a rare error that could violate data integrity guarantees during concurrent write and read operations on a specific key.
* Fixed an issue where read replicas stopped processing requests.
* Fixed a rare error that could lead to the abnormal termination of database processes when there were uncommitted transactions on a table at the time of its renaming.
* Fixed an error in determining the status of a static group, where the static group was not marked as non-working when it should have been.
* Fixed an error of partial commit of a distributed transaction with uncommitted changes in case of certain race conditions with restarts.
* Fixed anomalies related to reading outdated data, detected using Jepsen.

## Version 23.4 {#23-4}

Release date: May 14, 2024.

### Performance

* Fixed an issue of increased CPU consumption by the topic actor `PERSQUEUE_PARTITION_ACTOR`.
* Optimized resource usage by SchemeBoard replicas. The greatest effect is noticeable when modifying the metadata of tables with a large number of partitions.

### Bug fixes

* Fixed a bug of possible partial commit of accumulated changes when using distributed transactions. This error occurs in an extremely rare combination of events, including restarting tablets that service the table partitions involved in the transaction.
* Fixed a race condition between the table merge and garbage collection processes, which could result in garbage collection ending with an invariant violation error, leading to an abnormal termination of the server process `ydbd`.
* Fixed a bug in Blob Storage where information about changes to the composition of a storage group might not be received in a timely manner by individual cluster nodes. As a result, reads and writes of data stored in the affected group could become blocked in rare cases, requiring manual intervention.
* Fixed a bug in Blob Storage where data storage nodes might not start despite the correct configuration. The error occurred on systems with the experimental "blob depot" feature explicitly enabled (this feature is disabled by default).
* Fixed a bug that sometimes occurred when writing to a topic with an empty `producer_id` with deduplication turned off. It could lead to abnormal termination of the server process `ydbd`.
* Fixed a bug that caused the process `ydbd` to crash due to an incorrect session state when writing to a topic.
* Fixed a bug in displaying the metric of the number of partitions in a topic, where it previously displayed an incorrect value.
* Fixed memory leaks that appeared when copying topic data between clusters {{ ydb-short-name }}. These could cause server processes `ydbd` to terminate due to out-of-memory issues.

## Version 23.3 {#23-3}

Release date: October 12, 2023.

### Functionality

* Implemented visibility of own changes within transactions. Previously, when trying to read data already modified in the current transaction, the query would fail. This led to the need to order reads and writes within the transaction. With the introduction of visibility of own changes, these restrictions are lifted, and queries can read rows modified in the given transaction.
* Added support for column tables. Column tables are well-suited for analytical queries (Online Analytical Processing) because only the columns directly involved in the query are read when executing it. YDB column tables allow creating analytical reports with performance comparable to specialized analytical DBMS.
* Added support for Kafka API for topics. YDB topics can now be accessed via a Kafka-compatible API designed for migrating existing applications. Support for the Kafka protocol is provided.
* Added the ability to write to a topic without deduplication. This type of writing is well-suited for cases where the order of message processing is not critical. Writing without deduplication is faster and consumes fewer server resources, but message ordering and deduplication on the server does not occur.
* YQL has added the capabilities to create, modify, and delete topics.
* Added the ability to grant and revoke access rights using the YQL GRANT and REVOKE commands.
* Added the ability to log DML operations in the audit log.
* **_(Experimental)_** When writing messages to a topic, it is now possible to pass metadata. To enable this functionality, add the necessary setting to the configuration file.
* **_(Experimental)_** Added the ability to read from topics and write to a table within a single transaction. This feature simplifies the scenario of transferring data from a topic to a table. To enable this feature, add the necessary setting to the configuration file.
* **_(Experimental)_** Added support for PostgreSQL compatibility. The new mechanism allows executing SQL queries in PostgreSQL dialect on YDB infrastructure using the PostgreSQL network protocol. You can use familiar PostgreSQL tools such as psql and drivers (pq for Golang and psycopg2 for Python), as well as develop queries using familiar PostgreSQL syntax with YDB's horizontal scalability and fault tolerance.
* **_(Experimental)_** Added support for federated queries. This allows retrieving information from various data sources without moving the data to YDB. Interaction with ClickHouse, PostgreSQL, and S3 is supported via YQL queries without duplicating data between systems.

### Embedded UI

* A new option has been added to the query type selector settings, which is available when the corresponding parameter is enabled. Also, the query history now takes into account the syntax used when executing the query.
* The YQL query template for creating a table has been updated. Added a description of the available parameters.
* Sorting and filtering for Storage and Nodes tables have been moved to the server. To use this functionality, you need to enable the corresponding parameter in the experiments section.
* Buttons for creating, modifying, and deleting topics have been added to the context menu.
* Added sorting by criticality for all issues in the tree in `Healthcheck`.

### Performance

* Implemented iterator reads. This functionality allows separating reads and computations. Iterator reads enable datashards to increase the throughput of read queries.
* Optimized the performance of writing to YDB topics.
* Improved tablet balancing when nodes are overloaded.

### Bug fixes

* Fixed an error that could block snapshots known to reading iterators but not to coordinators.
* Fixed a memory leak when closing a connection in Kafka proxy.
* Fixed an error where snapshots taken through reading iterators might not recover on restarts.
* Fixed an incorrect residual predicate for the condition `IS NULL` on a column.
* Fixed the triggering of verification `VERIFY failed: SendResult(): requirement ChunksLimiter.Take(sendBytes) failed`.
* Fixed `ALTER TABLE` for column-based tables `TTL`.
* Implemented `FeatureFlag`, which allows enabling/disabling work with `CS` and `DS`.
* Fixed a 50 ms time difference between coordinator times in 23-2 and 23-3.
* Fixed an error where the handle `storage` returned extra groups when the `node_id` parameter was present in the request `viewer backend`.
* Added a filter `usage` to `/storage` in `viewer backend`.
* Fixed an error in Storage v2 where an incorrect number was returned in `Degraded`.
* Fixed the cancellation of subscriptions from sessions in iterator reads during tablet restarts.
* Fixed an error where healthcheck alerts for storage flickered during rolling restarts when going through a load balancer.
* Updated metrics `cpu usage` in YDB.
* Fixed the ignoring of `NULL` when specifying `NOT NULL` in the table schema.
* Implemented logging of operations `DDL` in the common log.
* Implemented a restriction for the `ydb table attribute add/drop` command to work only with tables and not with other objects.
* Disabled `CloseOnIdle` for `interconnect`.
* Fixed the doubling of read speed in the UI.
* Fixed an error where data could be lost on `block-4-2`.
* Added a topic name validation check.
* Fixed a possible deadlock in the actor system.
* Fixed the test `KqpScanArrowInChanels::AllTypesColumns`.
* Fixed the test `KqpScan::SqlInParameter`.
* Fixed parallelism issues for OLAP queries.
* Fixed the insertion of `ClickBench parquet`.
* Added a missing call to `CheckChangesQueueOverflow` in the general `CheckDataTxReject`.
* Fixed an error that returned an empty status in calls to `ReadRows API`.
* Fixed incorrect retries in the final stage of export.
* Fixed an issue with an infinite quota for the number of records in a CDC topic.
* Fixed the import error of `string` and `parquet` into an `string` OLAP column.
* Fixed a crash in `KqpOlapTypes.Timestamp` under tsan.
* Fixed a crash in `viewer backend` when attempting to execute a query against the database due to version incompatibility.
* Fixed an error where `viewer` did not return a response from `healthcheck` due to a timeout.
* Fixed an error where incorrect `ExpectedSerial` values could be saved in Pdisks.
* Fixed an error where database nodes crashed due to a segfault in the S3 actor.
* Fixed a race condition in `ThreadSanitizer: data race KqpService::ToDictCache-UseCache`.
* Fixed a race condition in `GetNextReadId`.
* Fixed an inflated result in `SELECT COUNT(*)` immediately after import.
* Fixed an error where `TEvScan` could return an empty dataset in the case of shard splitting.
* Added a separate issue/error code for cases of exhausted available space.
* Fixed error `GRPC_LIBRARY Assertion failed`.
* Fixed an error where scanning queries on secondary indexes returned an empty result.
* Validation of `CommitOffset` in `TopicAPI` has been fixed.
* The consumption of `shared cache` has been reduced when approaching OOM.
* The logic of schedulers from `data executer` and `scan executer` has been merged into one class.
* Handles `discovery` and `proxy` have been added to the execution process of `query` in `viewer backend`.
* A bug has been fixed where the handle `/cluster` returns the name of the root domain of type `/ru` in `viewer backend`.
* A scheme for seamless updating of tablets for `QueryService` has been implemented.
* A bug has been fixed where `DELETE` returned data and did not delete it.
* A bug in the operation of `DELETE ON` in `query service` has been fixed.
* Unexpected disabling of batching in the default scheme settings has been fixed.
* The triggering check of `VERIFY failed: MoveUserTable(): requirement move.ReMapIndexesSize() == newTableInfo->Indexes.size()` has been fixed.
* The default timeout for grpc-streaming has been increased.
* Unused messages and methods have been removed from `QueryService`.
* Sorting by `Rack` has been added in `/nodes` in `viewer backend`.
* A bug has been fixed where a query with sorting returns an error when decreasing.
* The interaction between `QP` and `NodeWhiteboard` has been fixed.
* Support for old parameter formats has been removed.
* A bug has been fixed where `DefineBox` was not applied to disks with a static group.
* A bug `SIGSEGV` in data nodes when importing `CSV` via `YDB CLI` has been fixed.
* A bug with a crash when processing `NGRpcService::TRefreshTokenImpl` has been fixed.
* The `gossip` protocol for exchanging information about cluster resources has been implemented.
* Bug `DeserializeValuePickleV1(): requirement data.GetTransportVersion() == (ui32) NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0 failed` has been fixed.
* Auto-increment columns have been implemented.
* Use status `UNAVAILABLE` instead of `GENERIC_ERROR` when identifying a shard error.
* Support for `rope payload` in `TEvVGet` has been added.
* Ignoring outdated events has been added.
* The crash of write sessions on an invalid topic name has been fixed.
* Bug `CheckExpected(): requirement newConstr failed, message: Rewrite error, missing Distinct((id)) constraint in node FlatMap` has been fixed.
* `safe heal` has been enabled by default.

## Version 23.2 {#23-2}

Release date: August 14, 2023.

### Functionality

* **_(Experimentally)_** Visibility of own changes has been implemented. When this feature is enabled, you can read modified values from the current transaction, which has not yet been committed. This functionality also allows you to perform several modifying operations in one transaction on a table with secondary indexes. To enable this functionality, add `enable_kqp_immediate_effects: true` to the `table_service_config` section in the [configuration file](reference/configuration/index.md).
* **_(Experimentally)_** Iterator reads have been implemented. This functionality allows you to separate reads and computations from each other. Iterator reads allow date shards to increase the throughput of read queries. To enable this functionality, add `enable_kqp_data_query_source_read: true` to the `table_service_config` section in the [configuration file](reference/configuration/index.md).

### Built-in UI

* Navigation has been improved:
  * The buttons for switching between diagnostic and development modes have been moved to the left panel.
  * Breadcrumbs have been added to all pages.
  * Information about storage groups and database nodes has been moved to tabs on the database page.
* History and saved queries have been moved to tabs above the query editor.
* In the Info tabs for schema objects, settings are displayed in terms of the `CREATE` or `ALTER` constructs.
* Support for displaying [columnar tables](concepts/datamodel/table.md#column-table) in the schema tree has been added.

### Performance

* For scanning queries, the ability to efficiently search for individual rows using a primary key or secondary indexes has been implemented, which can significantly improve performance in many cases. As with regular queries, to use a secondary index, you must explicitly specify its name in the query text using the `VIEW` keyword.

* **_(Experimentally)_** The ability to manage system tablets of the database (SchemeShard, Coordinators, Mediators, SysViewProcessor) with its own Hive, instead of the root Hive, and do this immediately when creating a new database has been added. Without this flag, the system tablets of the new database are created in the root Hive, which can negatively affect its load. Enabling this flag makes databases completely isolated in terms of load, which can be especially relevant for installations consisting of a hundred or more nodes. To enable this functionality, add `alter_database_create_hive_first: true` to the `feature_flags` section in the [configuration file](reference/configuration/index.md).

### Bug fixes

* A bug in the auto-configuration of the actor system has been fixed, as a result of which the entire load falls on the system pool.
* A bug leading to a full scan when searching by the primary key prefix through `LIKE` has been fixed.
* Bugs in interaction with date shard replicas have been fixed.
* Bugs in working with memory in columnar tables have been fixed.
* Bugs in processing conditions for immediate transactions have been fixed.
* A bug in the operation of iterator reads on date shard replicas has been fixed.
* A bug leading to an avalanche-like reinstallation of data delivery sessions to asynchronous indexes has been fixed.
* Bugs in the optimizer in scanning queries have been fixed.
* A bug of incorrect calculation of hive storage consumption after database expansion has been fixed.
* A bug of operation hanging from non-existent iterators has been fixed.
* Bugs when reading a range on the `NOT NULL` column have been fixed.
* Fixed the VDisk replication freeze error
* Fixed the error in the operation of the `run_interval` option in TTL

## Version 23.1 {#23-1}

Release date: May 5, 2023. To update to version 23.1, go to the [Downloads](downloads/index.md#ydb-server) section.

### Functionality

* Added [initial table scanning](concepts/cdc.md#initial-scan) when creating a CDC change stream. Now you can download all the data that exists at the time of stream creation.
* Added the ability to [atomically replace an index](dev/secondary-indexes.md#atomic-index-replacement). Now you can atomically and transparently to the application replace one index with another pre-created index. The replacement is performed without downtime.
* Added [audit log](security/audit-log.md) — a stream of events that contains information about all operations on {{ ydb-short-name }} objects.

### Performance

* Improved data transfer formats between query execution stages, which sped up SELECT by 10% on queries with parameters and up to 30% on write operations.
* Added [automatic configuration](reference/configuration/index.md) of actor system pools depending on their load. This improves performance through more efficient sharing of CPU resources.
* Optimized the logic of applying predicates — applying restrictions using OR and IN with parameters is automatically moved to the DataShard side.
* (Experimentally) For scanning queries, the ability to efficiently search for individual rows using a primary key or secondary indexes has been implemented, which can significantly improve performance in many cases. As with regular queries, to use a secondary index, you must explicitly specify its name in the query text using the `VIEW` keyword.
* Implemented caching of the computation graph when executing queries, which reduces CPU consumption when building it.

### Bug fixes

* Fixed a number of errors in the implementation of the distributed data storage. We strongly recommend that all users update to the latest version.
* Fixed the error of building an index on not null columns.
* Fixed the statistics calculation with MVCC enabled.
* Fixed errors with backups.
* Fixed the race during the split and deletion of the table with CDC.

## Version 22.5 {#22-5}

Release date: March 7, 2023. To update to version **22.5**, go to the [Downloads](downloads/index.md#ydb-server) section.

### What's new

* Added [change stream configuration parameters](yql/reference/syntax/alter_table/changefeed.md) to pass additional information about changes to the topic.
* Added support for [renaming tables](concepts/datamodel/table.md#rename) with TTL enabled.
* Added [management of record storage time](concepts/cdc.md#retention-period) for the change stream.

### Bug fixes and improvements

* Fixed the error when inserting 0 rows with the BulkUpsert operation.
* Fixed the error when importing Date/DateTime columns from CSV.
* Fixed the error of importing data from CSV with a line break.
* Fixed the error of importing data from CSV with empty values.
* Improved the performance of Query Processing (WorkerActor was replaced with SessionActor).
* DataShard compaction now starts immediately after split or merge operations.

## Version 22.4 {#22-4}

Release date: October 12, 2022. To update to version **22.4**, go to the [Downloads](downloads/index.md#ydb-server) section.

### What's new

* {{ ydb-short-name }} Topics and Change Data Capture (CDC):

  * A new Topic API has been introduced. [Topic](concepts/datamodel/topic.md) {{ ydb-short-name }} is an entity for storing unstructured messages and delivering them to various subscribers.
  * Support for the new Topic API has been added to [{{ ydb-short-name }} CLI](reference/ydb-cli/topic-overview.md) and [SDK](reference/ydb-sdk/topic.md). The Topic API provides methods for streaming writing and reading messages, as well as managing topics.
  * The ability to [capture table data changes](concepts/cdc.md) and send messages about changes to the topic has been added.

* SDK:

  * The ability to interact with topics in {{ ydb-short-name }} SDK has been added.
  * Official support for the database/sql driver for working with {{ ydb-short-name }} in Golang has been added.

* Embedded UI:

  * The CDC change stream and secondary indexes are now displayed in the database schema hierarchy as separate objects.
  * The visualization of the graphical representation of query explain plans has been improved.
  * Problematic storage groups are now more noticeable.
  * Various improvements based on UX research.

* Query Processing:

  * Added Query Processor 2.0 — a new subsystem for executing OLTP queries with significant improvements over the previous version.
  * Write performance improvement was up to 60%, read performance up to 10%.
  * The ability to enable the NOT NULL constraint for primary keys in YDB during table creation has been added.
  * Support for renaming a secondary index online without stopping the service has been added.
  * The query explain view has been improved and now includes graphs for physical operators.

* Core:

  * Support for a consistent snapshot for read-only transactions that does not conflict with writing transactions has been added.
  * Added support for BulkUpsert for tables with asynchronous secondary indexes.
  * Added support for TTL for tables with asynchronous secondary indexes.
  * Added support for compression when exporting data to S3.
  * Added audit log for DDL statements.
  * Static credential authentication has been supported.
  * System views for query performance diagnostics have been added.
