# feature_flags

YDB implements feature flags following the established [feature toggling](https://en.wikipedia.org/wiki/Feature_toggle) approach.

Feature flags are settings that enable or disable individual YDB capabilities. They allow new functionality to be enabled gradually: its code may already be present in the installed YDB version while the functionality itself is still unavailable to users.

## Who Changes Feature Flags {#management}

Feature flags can be changed by the feature owner, who understands its internal implementation. Changing a flag is a complex process: it requires considering the cluster state, dependencies between capabilities, and compatibility between versions and data formats.

Do not change flags yourself if you do not have sufficient knowledge of the feature's internal implementation and the consequences of changing its state. Before changing a flag, read the documentation for your YDB version and, if necessary, discuss the change with the feature owner.

{% note warning %}

Enabling a flag may affect data compatibility and prevent rolling YDB back to a previous version. Such a rollback may be necessary if a critical error is discovered after an upgrade.

{% endnote %}

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

## How to Check Whether Functionality Is Available {#check-availability}

When checking availability, consider the installed YDB version and the cluster configuration.

1. Read the documentation and release notes for the relevant version. Check the functionality's limitations and any indication of its experimental status.
2. [Generate the final configuration](../../devops/configuration-management/configuration-v1/dynamic-config-selectors.md#selectors-resolve) for the nodes that serve the target database and check the corresponding flag's value. Flag descriptions are provided in the [configuration reference](#functional-flags).

The default flag value is determined by the code of the corresponding YDB version. The value in the cluster configuration may differ from the default. The flag reference therefore does not replace checking actual availability on your cluster.

## How Functionality Becomes Generally Available {#general-availability}

When experimental functionality is ready for general use, its flag is enabled by default in the corresponding YDB version. The experimental status warning is removed from that version's documentation, and the release notes announce the functionality's availability.

After upgrading the cluster, check its configuration: an explicitly set flag value may differ from the new default.

## Configuring Flags {#configuration}

The `feature_flags` section enables or disables certain {{ ydb-short-name }} features in the main cluster configuration. With [dynamic configuration selectors](../../devops/configuration-management/configuration-v1/dynamic-config-selectors.md), you can override flags for individual databases or node groups. To enable a feature, set the corresponding feature flag to `true`. For example, to enable auto-partitioning of topics in CDC, add the following lines to the configuration:

```yaml
feature_flags:
  enable_topic_autopartitioning_for_cdc: true
```

## Feature Flags {#functional-flags}

| Flag | Function |
| --- | --- |
| `enable_json_index` | [JSON indexes](../../dev/json-indexes.md) to speed up search in JSON fields |
| `enable_json_index_auto_select` | Automatic selection of [JSON indexes](../../dev/json-indexes.md) when executing queries |
| `enable_fulltext_index` | [Full-text index](../../dev/fulltext-indexes.md) for full-text search |
| `enable_local_bloom_filter_index` | [Local Bloom index](../../dev/bloom-skip-indexes.md#types) of type `bloom_filter` |
| `enable_local_bloom_ngram_filter_index` | [Local Bloom index](../../dev/bloom-skip-indexes.md#types) of type `bloom_ngram_filter` |
| `enable_local_min_max_index` | [Local min_max index](../../dev/min_max-skip-index.md) |
| `enable_topic_autopartitioning_for_cdc` | [Auto-partitioning of topics](../../concepts/cdc.md#topic-partitions) in CDC for row tables |
| `enable_access_to_index_impl_tables` | Ability to [specify the number of replicas](../../yql/reference/syntax/alter_table/indexes.md) for a secondary index |
| `enable_changefeeds_export`, `enable_changefeeds_import` | Support for change feeds (changefeed) in backup and restore operations |
| `enable_view_export` | Support for views (`VIEW`) in backup and restore operations |
| `enable_export_auto_dropping` | Automatic deletion of temporary directories and tables when exporting to S3 |
| `enable_followers_stats` | System views with information about the [history of overloaded partitions](../../dev/system-views.md#top-overload-partitions) |
| `enable_strict_acl_check` | Prohibition on granting rights to non-existent users and on deleting users who have been granted rights |
| `enable_strict_user_management` | Strict rules for administering local users (i.e., only a cluster or database administrator can administer local users) |
| `enable_database_admin` | Adding the database administrator role |
| `enable_kafka_native_balancing` | Client-side balancing of partitions when reading via the [Kafka protocol](https://kafka.apache.org/documentation/#consumerconfigs_partition.assignment.strategy) |
| `enable_topic_compactification_by_key` | Enabling topic compaction in the [YDB Topics Kafka API](../../reference/kafka-api/index.md) |
| `enable_kafka_transactions` | Enabling transactions in the [YDB Topics Kafka API](../../reference/kafka-api/index.md) |
| `enable_external_data_sources` | Enabling [external data sources](../../concepts/datamodel/external_data_source.md) |
| `enable_grpc_audit` | Enabling [audit](../../security/audit-log.md#grpc-connection) of gRPC connection state changes |
| `enable_fs_backups` | Enabling [backup and restore operations to a network file system](../../concepts/backup.md#nfs) |
| `switch_to_config_v2` | Switching from configuration V1 to [configuration V2](../../devops/configuration-management/migration/migration-to-v2.md) |
| `enable_streaming_queries` | Enabling [streaming queries](../../concepts/streaming-query/streaming-query.md) |
| `enable_stable_node_names` | Enabling [stable names for dynamic nodes](node_broker_config.md) |
| `alter_database_create_hive_first` | Creating a dedicated Hive for a database's system tablets when the database is created |
