# feature_flags

The `feature_flags` section enables or disables certain {{ ydb-short-name }} features using boolean flags. To enable a feature, set the corresponding feature flag to `true` in the cluster configuration. For example, to enable support for auto-partitioning of topics in CDC, add the following lines to the configuration:


```yaml
feature_flags:
  enable_topic_autopartitioning_for_cdc: true
```


## Feature flags

| Flag | Feature |
| --- | --- |
| `enable_fulltext_index` | [Full-text index](../../dev/fulltext-indexes.md) for full-text search |
| `enable_local_bloom_filter_index` | [Local Bloom index](../../dev/bloom-skip-indexes.md#types) of type `bloom_filter` |
| `enable_local_bloom_ngram_filter_index` | [Local Bloom index](../../dev/bloom-skip-indexes.md#types) of type `bloom_ngram_filter` |
| `enable_topic_autopartitioning_for_cdc` | [Auto-partitioning of topics](../../concepts/cdc.md#topic-partitions) in CDC for row tables |
| `enable_access_to_index_impl_tables` | Ability to [specify the number of replicas](../../yql/reference/syntax/alter_table/indexes.md) for a secondary index |
| `enable_changefeeds_export`, `enable_changefeeds_import` | Support for change feeds (changefeed) in backup and restore operations |
| `enable_view_export` | Support for views (`VIEW`) in backup and restore operations |
| `enable_export_auto_dropping` | Auto-deletion of temporary directories and tables when exporting to S3 |
| `enable_followers_stats` | System views with information about the [history of overloaded partitions](../../dev/system-views#top-overload-partitions) |
| `enable_strict_acl_check` | Prohibition on granting permissions to non-existent users and on deleting users who have been granted permissions |
| `enable_strict_user_management` | Strict administration rules for local users (i.e., only a cluster or database administrator can administer local users) |
| `enable_database_admin` | Adding the database administrator role |
| `enable_kafka_native_balancing` | Client-side partition balancing when reading via [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) |
| `enable_topic_compactification_by_key` | Enabling topic compaction in [YDB Topics Kafka API](../../reference/kafka-api/index.md) |
| `enable_kafka_transactions` | Enabling transactions in [YDB Topics Kafka API](../../reference/kafka-api/index.md) |
| `enable_external_data_sources` | Enabling [external data sources](../../concepts/datamodel/external_data_source.md) |
| `enable_grpc_audit` | Enabling [audit](../../security/audit-log.md#grpc-connection) of gRPC connection state changes |
| `enable_fs_backups` | Enabling [backup and restore operations to a network file system](../../concepts/backup.md#nfs) |
