# Migration to Configuration V2

This document contains instructions for migrating from [configuration V1](../../configuration-management/configuration-v1/config-overview.md) to [configuration V2](../../configuration-management/configuration-v2/config-overview.md).

In configuration V1, there are two different mechanisms for applying configuration files:

- [static configuration](../../configuration-management/configuration-v2/config-overview.md#static-config) manages [storage nodes](../../../concepts/glossary.md#storage-node) of the {{ ydb-short-name }} cluster and requires manual placement of files on each cluster node;
- [dynamic configuration](../../configuration-management/configuration-v2/config-overview.md#dynamic-config) manages [database nodes](../../../concepts/glossary.md#database-node) of the {{ ydb-short-name }} cluster and is loaded into the cluster centrally using {{ ydb-short-name }} CLI commands.

In configuration V2, this process is unified: a single configuration file is loaded into the system through {{ ydb-short-name }} CLI commands, automatically delivered to all cluster nodes.

The [State Storage](../../../concepts/glossary.md#state-storage) and [static group](../../../concepts/glossary.md#static-group) components of the {{ ydb-short-name }} cluster are key to the cluster's correct operation. When working with configuration V1, these components are configured manually by specifying the `domains_config` and `blob_storage_config` sections in the configuration file.
In configuration V2, [automatic configuration](../../configuration-management/configuration-v2/config-overview.md) of these components is possible without specifying the corresponding sections in the configuration file.

## Initial State

Migration to configuration V2 can be performed if the following conditions are met:

1. The {{ ydb-short-name }} cluster has been [updated](../../deployment-options/manual/update-executable.md) to version 25.1 and higher.
1. The {{ ydb-short-name }} cluster is configured with a [configuration V1](../../configuration-management/configuration-v2/config-overview.md#static-config) file `config.yaml`, located in the node file system and connected through the `ydbd --yaml-config` argument.
1. The cluster configuration file contains the `domains_config` and `blob_storage_config` sections for configuring State Storage and static group respectively.

## Checking the Current Configuration Version

Before starting the migration, ensure that your cluster is running on configuration V1. You can find out the current configuration version on nodes using several methods described in the article [{#T}](../check-config-version.md).

You should continue following this instruction only if the nodes are running on configuration version V1. If all nodes already have version V2 enabled, migration is not required.

## Instructions for Migration to Configuration V2

To migrate the {{ ydb-short-name }} cluster to configuration V2, you need to perform the following steps:

1. Check for the presence of a [dynamic configuration](../../configuration-management/configuration-v2/config-overview.md#dynamic-config) file in the cluster. To do this, run the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```

    If no such configuration exists in the cluster, the command will output the message:

    ```bash
    No config returned.
    ```

    If a file is found, you should use it and skip the next step of this instruction.

2. If there is no dynamic configuration file in the cluster, run the dynamic configuration file generation command [ydb admin cluster config generate](../../../reference/ydb-cli/commands/configuration/cluster/generate.md). The file will be generated based on the static configuration file located on the cluster nodes.

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config generate > config.yaml
    ```

3. Add the following field to the `config.yaml` file obtained in step 1 or 2:

    ```yaml
    feature_flags:
        ...
        switch_to_config_v2: true
    ```

    {% cut "More details" %}

    Enabling this flag means that the [DS Controller](../../../concepts/glossary.md#ds-controller) tablet, not the [Console](../../../concepts/glossary.md#console) tablet, is now responsible for configuration storage and operations. This switches the main cluster configuration management mechanism.

    {% endcut %}

4. Place the `config.yaml` file on all cluster nodes, replacing the previous configuration file with it.

5. Create a directory for the {{ ydb-short-name }} node to work with configuration on each node. If running multiple cluster nodes on one host, create separate directories for each node. Initialize the directory by running the [ydb admin node config init](../../../reference/ydb-cli/commands/configuration/node/init.md) command on each node. In the `--from-config` parameter, specify the path to the `config.yaml` file placed on the nodes earlier.

    ```bash
    sudo mkdir -p /opt/ydb/config-dir
    sudo chown -R ydb:ydb /opt/ydb/config-dir
    ydb admin node config init --config-dir /opt/ydb/config-dir --from-config /opt/ydb/cfg/config.yaml
    ```

    {% cut "More details" %}

    In the future, the system will independently save the current configuration in the specified directories.

    {% endcut %}

6. Restart all cluster nodes using the [rolling-restart](../../../maintenance/manual/node_restarting.md) procedure, adding the `ydbd --config-dir` option when starting the node with the path to the directory specified, and removing the `ydbd --yaml-config` option.

    {% list tabs group=manual-systemd %}

    - Manual

        When starting manually, add the `--config-dir` option to the `ydbd server` command, without specifying the `--yaml-config` option:

        ```bash
        ydbd server --config-dir /opt/ydb/config-dir
        ```

    - Using systemd

        When using systemd, add the `--config-dir` option to the `ydbd server` command in the systemd configuration file, and also remove the `--yaml-config` option:

        ```ini
        ExecStart=/opt/ydb/bin/ydbd server --config-dir /opt/ydb/config-dir
        ```

        After updating the systemd file, run the following command to apply the changes:

        ```bash
        sudo systemctl daemon-reload
        ```

    {% endlist %}

7. Load the previously obtained configuration file `config.yaml` into the system using the [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md) command:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 cluster config replace -f config.yaml
    ```

    The command will request confirmation to perform the operation `This command may damage your cluster, do you want to continue? [y/N]`, in response to this request you need to agree and enter `y`.

    {% cut "More details" %}

    After executing the command, the configuration file will be loaded into the internal storage of the [DS Controller](../../../concepts/glossary.md#ds-controller) tablet and saved in the directories specified in the `--config-dir` option on each node. From this moment, any configuration change on existing nodes is performed using [special commands](../configuration-v2/update-config.md) of the {{ ydb-short-name }} CLI. Also, when starting a node, the current configuration will be automatically loaded from the configuration directory.

    {% endcut %}

8. Get the current cluster configuration using [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 admin cluster config fetch > config.yaml
    ```

    The `config.yaml` file should match the configuration files distributed across the cluster nodes, except for the `metadata.version` field, which should be one unit higher compared to the version on the cluster nodes.

9. Add the following block to `config.yaml` in the `config` section:

    ```yaml
    self_management_config:
      enabled: true
    ```

    {% cut "More details" %}

    This section is responsible for enabling the [distributed configuration](../../../concepts/glossary.md#distributed-configuration) mechanism in the cluster. Configuration storage and any operations on it will be performed through this mechanism.

    {% endcut %}

10. Load the updated configuration file into the cluster using [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 cluster config replace -f config.yaml
    ```

11. Restart all [storage nodes](../../../concepts/glossary.md#storage-node) of the cluster using the [rolling restart](../../../reference/ydbops/rolling-restart-scenario.md) procedure.

12. If there is a `config.domains_config.security_config` section in the `config.yaml` file, move it one level up — to the `config` section.

13. Remove the `config.blob_storage_config` and `config.domains_config` sections from the `config.yaml` file.

14. Load the updated configuration file into the cluster:

    ```bash
    ydb -e grpc://<node.ydb.tech>:2135 cluster config replace -f config.yaml
    ```

    {% cut "More details" %}

    After loading the configuration, the {{ ydb-short-name }} cluster will be switched to automatic configuration management mode for [State Storage](../../../reference/configuration/index.md#domains-state) and [static group](../../../reference/configuration/index.md#blob_storage_config) using the distributed configuration mechanism.

    {% endcut %}

You can verify the successful completion of the migration by checking the configuration version on the cluster nodes using one of the methods described in the article [{#T}](../check-config-version.md). On all cluster nodes, the `Configuration version` should be equal to `v2`.

## Result

As a result of the performed actions, the cluster will be migrated to configuration V2 mode. Unified configuration management is performed using [special commands](../configuration-v2/update-config.md) of the {{ ydb-short-name }} CLI, static group and State Storage are managed automatically by the system.
