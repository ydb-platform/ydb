# Removing a node from a cluster

{% include [_](../_includes/experimental_v2.md) %}

This article describes how to remove a [dynamic](../../../concepts/glossary.md#dynamic) or [static](../../../concepts/glossary.md#static-node) node from a {{ ydb-short-name }} cluster deployed manually on virtual machines or physical servers. For a Kubernetes deployment, remove nodes using the {{ ydb-short-name }} operator.

## Removing a dynamic node

Removing a dynamic node does not require changing the cluster configuration.

To remove a dynamic node without affecting query processing:

1. [Drain the tablets](../../../maintenance/manual/node_restarting.md#replace-hardware) from the node and wait for the operation to complete.
1. Stop the dynamic node process.

After stopping the process, check the **Nodes** tab on the [cluster monitoring page](../../../reference/embedded-ui/ydb-monitoring.md#node_list_page) and verify that the node is no longer shown as connected.

## Removing a static node {#remove-static-node}

Static nodes serve the storage system and are listed in the `hosts` section. A static node can contain VDisks of dynamic and static groups, as well as State Storage, Board, and SchemeBoard replicas. These resources must be moved before the node is removed from the configuration.

Before starting the procedure, use the [Embedded UI](../../../reference/embedded-ui/ydb-monitoring.md#node_storage_page) to check that the affected storage groups are healthy. The cluster must also have enough free slots to move the VDisks from the node while preserving the fault model. For details on calculating the required capacity margin, see [{#T}](../../concepts/capacity-planning.md#hardware-estimation).

[SelfHeal](../../../maintenance/manual/selfheal.md) is enabled for dynamic groups by default. If the node contains a static group VDisk, [enable static group SelfHeal](static-group-self-heal.md#on-off). If the node contains State Storage, Board, or SchemeBoard replicas, enable [Self Heal State Storage](../../../maintenance/manual/selfheal_statestorage.md#on-off).

To remove a static node:

1. If tablets are running on the node, [drain them](../../../maintenance/manual/node_restarting.md#replace-hardware).
1. [Check that the process can be stopped safely](../../../maintenance/manual/node_restarting.md#restart_process), then stop it.
1. Wait for SelfHeal to move the VDisks from the node. With the default settings, relocation starts approximately one hour after the node is stopped.
1. In the [Embedded UI](../../../reference/embedded-ui/ydb-monitoring.md#node_storage_page), check that no VDisks remain on the node and that the affected storage groups are healthy. If State Storage, Board, or SchemeBoard replicas were moved from the node, [check that the relocation is complete](../../../maintenance/manual/selfheal_statestorage.md#verify-result).
1. Fetch the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. If the node being removed is not the last entry in the [`hosts`](../../../reference/configuration/hosts.md) list, preserve the IDs of the nodes that follow it: add `node_id` to each entry where it is not specified. Use the entry's current one-based position in the list as the value. If the last entry is being removed, this step is not required.
1. Remove the node entry from the `hosts` section.
1. Apply the configuration using the [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md) command:

    ```bash
    ydb [global options...] admin cluster config replace -f config.yaml
    ```

    {% cut "If the command returns an error" %}

    If VDisks remain on the PDisks of the node being removed, the command returns an error similar to the following:

    ```text
    failed to remove PDisk# 1:1 as it has active VSlots
    ```

    In this case, wait for SelfHeal to move the remaining VDisks. Relocation time depends on the amount of data and disk performance. Monitor the relocation on the **Storage** tab of the node being removed in the [Embedded UI](../../../reference/embedded-ui/ydb-monitoring.md#node_storage_page). When no VDisks remain on the node, rerun the `config replace` command with the same file.

    If the VDisk list is not shrinking and replication is not in progress, [move the remaining VDisks manually](../../../maintenance/manual/moving_vdisks.md#removal_from_a_broken_device).

    {% endcut %}

After the configuration is applied successfully, the server and its disks can be decommissioned.
