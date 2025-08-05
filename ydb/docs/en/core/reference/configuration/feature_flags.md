# feature_flags

The `feature_flags` section enables or disables specific {{ ydb-short-name }} features using boolean flags. To enable a feature, set the corresponding feature flag to `true` in the cluster configuration. For example, to enable support for vector indexes and auto-partitioning of topics in the CDC, you need to add the following lines to the configuration:

```yaml
feature_flags:
  enable_vector_index: true
  enable_topic_autopartitioning_for_cdc: true
```

## Feature flags

| Flag          | Feature |
|---------------------------| ----------------------------------------------------|
| `enable_vector_index`                                    | Support for [vector indexes](../../dev/vector-indexes.md) for approximate vector similarity search |
| `enable_batch_updates`                                   | Support for `BATCH UPDATE` and `BATCH DELETE` statements |
| `enable_kafka_native_balancing`                          | Client balancing of partitions when reading using the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) |
| `enable_topic_autopartitioning_for_cdc`                  | [Auto-partitioning topics](../../concepts/cdc.md#topic-partitions) for row-oriented tables in CDC |
| `enable_access_to_index_impl_tables`                     | Support for [followers (read replicas)](../../yql/reference/syntax/alter_table/indexes.md) for covered secondary indexes |
| `enable_changefeeds_export`, `enable_changefeeds_import` | Support for changefeeds in backup and restore operations |
| `enable_view_export`                                     | Support for views in backup and restore operations |
| `enable_export_auto_dropping`                            | Automatic cleanup of temporary tables and directories during export to S3 |
| `enable_followers_stats`                                 | System views with information about [history of overloaded partitions](../../dev/system-views.md#top-overload-partitions) |
| `enable_strict_acl_check`                                | Strict ACL checks — do not allow granting rights to non-existent users and delete users with permissions |
| `enable_strict_user_management`                          | Strict checks for local users — only the cluster or database administrator can administer local users |
| `enable_database_admin`                                  | The role of a database administrator |