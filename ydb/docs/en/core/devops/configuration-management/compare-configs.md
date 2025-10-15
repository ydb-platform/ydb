# Comparing {{ ydb-short-name }} Cluster Configurations: V1 and V2

In {{ ydb-short-name }}, there are two main approaches to cluster configuration management: [V1](../configuration-management/configuration-v1/index.md) and [V2](../configuration-management/configuration-v2/index.md). Starting from {{ ydb-short-name }} version 25.1, configuration V2 is supported, which unifies {{ ydb-short-name }} cluster management, allows working with configuration entirely through [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md), and also automates the most complex aspects of configuration (managing [static group](../../reference/configuration/index.md#blob_storage_config) and [State Storage](../../reference/configuration/index.md#domains-state)).

{% include [_](_includes/configuration-version-note.md) %}

This article describes the key differences between these two approaches.

| Characteristic                 | Configuration V1                                  | Configuration V2                                     |
| ------------------------------ | ------------------------------------------------ | -------------------------------------------------- |
| **Configuration structure**     | Separate: [static](../../devops/configuration-management/configuration-v1/static-config.md) and [dynamic](../../devops/configuration-management/configuration-v1/dynamic-config.md). | [**Unified**](../configuration-management/configuration-v2/config-overview.md) configuration. |
| **File management**         | Static: manual file placement on each node.<br>Dynamic: centralized upload via CLI. | Unified: [centralized upload](../configuration-management/configuration-v2/update-config.md) via CLI, automatic delivery to all nodes. |
| **Delivery and application mechanism** | Static: read and applied from a local file at startup.<br>Dynamic: through [`Console` tablet](../../concepts/glossary.md#console). | Fully automatic via the [distributed configuration](../../concepts/glossary.md#distributed-configuration) mechanism. |
| **State Storage and static group management** | **Manual**: mandatory [`domains_config`](../../reference/configuration/index.md#domains-state) and [`blob_storage_config`](../../reference/configuration/index.md#blob_storage_config) sections in static configuration. | **Automatic**: managed by the [distributed configuration](../../concepts/glossary.md#distributed-configuration) system. |
| **Recommended for {{ ydb-short-name }} versions** | All versions before 25.1.1.                             | Version 25.1 and above.                                |

## Configuration V1

[Configuration V1](../configuration-management/configuration-v1/index.md) of a {{ ydb-short-name }} cluster consists of two parts:

* **Static configuration:** manages key node parameters, including [State Storage](../../reference/configuration/index.md#domains-state) and [static group](../../reference/configuration/index.md#blob_storage_config) configuration (`domains_config` and `blob_storage_config` sections respectively). Requires manual placement of the same configuration file on each cluster node. The path to the configuration is specified when starting the node using the `--yaml-config` option.
* **Dynamic configuration:** manages other cluster parameters. Loaded centrally using the `ydb admin config replace` command and distributed to database nodes.

If your cluster is running on configuration V1, it is recommended to perform [migration to configuration V2](migration/migration-to-v2.md).

## Configuration V2

Starting from {{ ydb-short-name }} version 25.1, [configuration V2](../configuration-management/configuration-v2/config-overview.md) is supported. Key features:

* **Unified configuration file:** the entire cluster configuration is stored and managed as a single entity.
* **Centralized management:** configuration is loaded to the cluster using the [`ydb admin cluster config replace`](../configuration-management/configuration-v2/update-config.md) command and automatically delivered to all nodes by the {{ ydb-short-name }} cluster itself through the [distributed configuration](../../concepts/glossary.md#distributed-configuration) mechanism.
* **Early validation:** the configuration file is validated before delivering it to cluster nodes, not when restarting  the server process.
* **Automatic State Storage and static group management:** V2 supports [automatic configuration](../configuration-management/configuration-v2/config-overview.md), which allows skipping these sections in the configuration file.
* **Storage on nodes:** the current configuration is automatically saved by each node in a special directory (specified by the `--config-dir` option when starting `ydbd`) and used during subsequent restarts.

Using configuration V2 is recommended for all {{ ydb-short-name }} clusters version 25.1 and above.