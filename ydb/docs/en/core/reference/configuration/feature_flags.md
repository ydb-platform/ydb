# feature_flags

The `feature_flags` section enables or disables certain {{ ydb-short-name }} features using boolean flags. To enable a feature, set the corresponding feature flag to `true` in the cluster configuration. For example, to enable support for auto-partitioning of topics in CDC, add the following lines to the configuration:

```yaml
feature_flags:
  enable_topic_autopartitioning_for_cdc: true
```

{% note warning %}

Change the `feature_flags` settings only if you are sure of the consequences. Incorrect configuration may disrupt the cluster.

{% endnote %}

## Feature flags

| Flag | Function |
| --- | --- |
| `enable_json_index` | [JSON indexes](../../dev/json-indexes.md) to speed up search in JSON fields |
| `enable_json_index_auto_select` | Automatic selection of [JSON indexes](../../dev/json-indexes.md) when executing queries |
| `enable_fulltext_index` | [Full-text index](../../dev/fulltext-indexes.md) for full-text search |
| `enable_local_bloom_filter_index` | [Local Bloom index](../../dev/bloom-skip-indexes.md#types) of type `bloom_filter` |
| `enable_local_bloom_ngram_filter_index` | [Local Bloom index](../../dev/bloom-skip-indexes.md#types) of type `bloom_ngram_filter` |
| `enable_topic_autopartitioning_for_cdc` | [Auto-partitioning of topics](../../concepts/cdc.md#topic-partitions) in CDC for row tables |
| `enable_access_to_index_impl_tables` | Ability to [specify the number of replicas](../../yql/reference/syntax/alter_table/indexes.md) for a secondary index |
| `enable_changefeeds_export`, `enable_changefeeds_import` | Support for change feeds (changefeed) in backup and restore operations |
| `enable_view_export` | Support for views (`VIEW`) in backup and restore operations |
| `enable_export_auto_dropping` | Auto-deletion of temporary directories and tables when exporting to S3 |
| `enable_followers_stats` | System views with information about [history of overloaded partitions](../../dev/system-views#top-overload-partitions) |
| `enable_strict_acl_check` | Prohibition on granting permissions to non-existent users and on deleting users who have been granted permissions |
| `enable_strict_user_management` | Strict rules for administering local users (i.e., only the cluster or database administrator can administer local users) |
| `enable_database_admin` | Adding the database administrator role |
| `enable_kafka_native_balancing` | Client-side partition balancing when reading via [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) |
| `enable_topic_compactification_by_key` | Enabling topic compaction in [YDB Topics Kafka API](../../reference/kafka-api/index.md) |
| `enable_kafka_transactions` | Enabling transactions in [YDB Topics Kafka API](../../reference/kafka-api/index.md) |
| `enable_external_data_sources` | Enabling [external data sources](../../concepts/datamodel/external_data_source.md) |
| `enable_grpc_audit` | Enabling [audit](../../security/audit-log.md#grpc-connection) of gRPC connection state changes |
| `enable_fs_backups` | Enabling [backup and restore operations to a network file system](../../concepts/backup.md#nfs) |

## Experimental Functionality {#experimental}

### Why a Flag May Be Disabled {#availability}

Some functionality is included in a release in an experimental state. It is still under development and is therefore disabled by default. Its behavior, syntax, API, or data storage format may change without backward compatibility. Such functionality may remain experimental for a long time or be removed without ever becoming generally available.

However, not every disabled flag indicates experimental functionality. Some flags control stable capabilities that not everyone needs. Check the status of a particular capability in the documentation for the relevant YDB version.

A capability being described in the documentation or its code being included in a release does not necessarily mean that it is enabled on your database's cluster.

### Risks of Enabling Experimental Functionality {#risks}

Stability, performance, and backward compatibility are not guaranteed for experimental functionality. Enabling its flag may lead to:

* errors when executing queries or cluster instability;
* reduced performance;
* data being written in a format that the previous YDB version does not support.

In the last case, disabling the flag does not necessarily restore the previous data format. You should therefore assess whether a version rollback is possible before enabling the functionality, rather than after a problem occurs.

Do not enable experimental functionality in production clusters. Use a separate test cluster with data you can afford to lose.
