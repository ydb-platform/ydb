# Migration to Configuration V1

This document contains instructions for migrating from [configuration V2](../../configuration-management/configuration-v2/config-overview.md) to [configuration V1](../../configuration-management/configuration-v1/index.md).

{% note info %}

This guide is intended for emergency situations when unexpected problems arise after [migrating to configuration V2](./migration-to-v2.md) and a rollback to configuration V1 is required, for example, for subsequent rollback to a {{ ydb-full-name }} version below v25-1. This procedure is not required in normal operation mode.

{% endnote %}

## Initial State

Migration to configuration V1 is only possible if the cluster uses [configuration V2](../../configuration-management/configuration-v2/config-overview.md). This can be achieved:

- as a result of [migration to configuration V2](migration-to-v2.md)
- during [initial deployment](../../deployment-options/manual/initial-deployment.md) of the cluster

You can determine the current configuration version on nodes using several methods described in the article [Checking Configuration Version](../check-config-version.md). Before starting the migration, ensure that the cluster is running on configuration V2.

## Instructions for Migration to Configuration V1

To migrate the {{ ydb-short-name }} cluster to configuration V1, you need to perform the following steps:

1. Get the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch --for-v1-migration > config.yaml
    ```

    {% cut "More details" %}

    The `--for-v1-migration` argument specifies that the full cluster configuration will be retrieved, including [State Storage](../../../reference/configuration/domains_config.md#domains-state) and [static group](../../../reference/configuration/blob_storage_config.md#blob_storage_config) configuration parameters.

    {% endcut %}

2. Modify the `config.yaml` configuration file by changing the `self_management_config.enabled` parameter value from `true` to `false`:

    ```yaml
    self_management_config:
      enabled: false
    ```

    {% cut "More details" %}

    This section is responsible for managing the [distributed configuration](../../../concepts/glossary.md#distributed-configuration) mechanism. Setting `enabled: false` disables this mechanism. Further management of State Storage and static group configuration will be performed manually through the `domains_config` and `blob_storage_config` sections respectively in the configuration file (these sections were obtained in the previous step when using the `--full` flag).

    {% endcut %}

3. Load the updated configuration file into the cluster using [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config replace -f config.yaml
    ```

4. Restart all cluster nodes using the [rolling restart](../../../maintenance/manual/node_restarting.md) procedure.

    {% cut "More details" %}

    After restarting the nodes, the cluster will be switched to manual State Storage and static group management mode, but will still use a unified configuration file delivered through the BSController tablet. Node configuration at startup will still be read from the directory specified in the `ydbd --config-dir` option and saved there.

    {% endcut %}

5. Get the current cluster configuration using `ydb admin cluster config fetch`:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```

    {% cut "More details" %}

    The obtained configuration will not contain the `domains_config` and `blob_storage_config` sections, as they are managed manually and should not be part of the dynamic configuration.

    {% endcut %}

6. Place the obtained `config.yaml` file (this will be your static configuration V1) in the file system of each cluster node.

7. Restart all cluster nodes using the [rolling-restart](../../../maintenance/manual/node_restarting.md) procedure, specifying the path to the static configuration file through the `ydbd --yaml-config` option and removing the `ydbd --config-dir` option:

    {% list tabs group=manual-systemd %}

    - Manual

        When starting manually, add the `--yaml-config` option to the `ydbd server` command, without specifying the `--config-dir` option:

        ```bash
        ydbd server --yaml-config /opt/ydb/cfg/config.yaml
        ```

    - Using systemd

        When using systemd, add the `--yaml-config` option to the `ydbd server` command in the systemd configuration file, and also remove the `--config-dir` option:

        ```ini
        ExecStart=/opt/ydb/bin/ydbd server --yaml-config /opt/ydb/config/config.yaml
        ```

        After updating the systemd file, run the following command to apply the changes:

        ```bash
        sudo systemctl daemon-reload
        ```

    {% endlist %}

You can verify the successful completion of the migration by checking the configuration version on the cluster nodes using one of the methods described in the article [{#T}](../check-config-version.md). All cluster nodes should use configuration `v1`.

## Result

As a result of the performed actions, the cluster will be migrated to configuration V1 mode. The configuration consists of two parts: static and dynamic, static group and State Storage management is performed manually.
