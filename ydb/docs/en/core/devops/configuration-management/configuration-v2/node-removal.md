# Removing a node from a cluster

{% include [_](../_includes/experimental_v2.md) %}

This article describes how to remove a [dynamic](../../../concepts/glossary.md#dynamic) or [static](../../../concepts/glossary.md#static-node) node from a {{ ydb-short-name }} cluster deployed manually on virtual machines or physical servers. For a Kubernetes deployment, node removal is handled by the {{ ydb-short-name }} operator: reduce the number of replicas in the `Storage` or `Database` resource manifest and reapply it, and the operator will scale down the corresponding `StatefulSet` and remove the excess pods. For details, see [{#T}](../../deployment-options/kubernetes/initial-deployment.md).

## Removing a dynamic node

Removing a dynamic node does not require changing the cluster configuration.

To remove a dynamic node without affecting query processing:

1. [Drain the tablets](../../../maintenance/manual/node_restarting.md#replace-hardware) from the node and wait for the operation to complete.
1. Stop the {{ ydb-short-name }} process on the node. [Check first that the process can be stopped safely, then stop it](../../../maintenance/manual/node_restarting.md#restart_process).

After stopping the process, check the **Nodes** tab on the [cluster monitoring page](../../../reference/embedded-ui/ydb-monitoring.md#node_list_page): the removed node must no longer be present in the list. If the node is still shown there (possibly with a **Disconnected** status), make sure the process was stopped on the correct host and that it is not configured to restart automatically (for example, by a `systemd` unit or a supervisor), and stop it there as well.

## Removing a static node {#remove-static-node}

Static nodes serve the storage system and are listed in the `hosts` section. A static node can contain VDisks of dynamic and static groups, as well as State Storage, Board, and SchemeBoard replicas. These resources must be moved before the node is removed from the configuration.

Before starting the procedure, use the [Embedded UI](../../../reference/embedded-ui/ydb-monitoring.md#node_storage_page) to check that the affected storage groups are healthy, that is, all VDisks of these groups are shown in the `Ok` state (highlighted in green), with none in `Error` or `Degraded` state. The cluster must also have enough free slots to move the VDisks from the node while preserving the fault model: the remaining nodes must have free PDisk space and slots to accommodate the relocated VDisks without exceeding the number of failed fail domains/fail realms the configured erasure coding scheme can tolerate. For details on calculating the required capacity margin, see [{#T}](../../concepts/capacity-planning.md#hardware-estimation).

[SelfHeal](../../../maintenance/manual/selfheal.md) is enabled for dynamic groups by default. Before removing the node, make sure it is also enabled for any resources hosted on this node:

* If the node contains a static group VDisk, [enable static group SelfHeal](static-group-self-heal.md#on-off). Alternatively, you can move the static group VDisk off the node manually, see [{#T}](static-group-move.md).
* If the node contains State Storage, Board, or SchemeBoard replicas, enable [Self Heal State Storage](../../../maintenance/manual/selfheal_statestorage.md#on-off). Alternatively, you can move these replicas off the node manually, see [{#T}](state-storage-move.md).

To remove a static node:

1. If tablets are running on the node, [drain them](../../../maintenance/manual/node_restarting.md#replace-hardware).
1. [Check that the process can be stopped safely](../../../maintenance/manual/node_restarting.md#restart_process), then stop it.
1. Wait for SelfHeal to move the VDisks from the node. With the default settings, relocation starts approximately one hour after the node is stopped. To start the relocation immediately instead of waiting, mark the node's PDisks as `BROKEN` using [{{ ydb-short-name }} DSTool](../../../reference/ydb-dstool/index.md):

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk set --status BROKEN --unavail-as-offline --pdisk-ids "[NodeId:PDiskId]"
    ```

    This immediately triggers VDisk relocation from the specified PDisks instead of waiting for CMS Sentinel to detect the node as faulty. For details, see [Move VDisks from a broken/missing block store volume](../../../maintenance/manual/moving_vdisks.md#removal_from_a_broken_device).

1. In the [Embedded UI](../../../reference/embedded-ui/ydb-monitoring.md#node_storage_page), check that no VDisks remain on the node and that the affected storage groups are healthy (all VDisks are in the `Ok` state). If State Storage, Board, or SchemeBoard replicas were moved from the node, [check that the relocation is complete](../../../maintenance/manual/selfheal_statestorage.md#verify-result).
1. Fetch the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. If the node being removed is not the last entry in the [`hosts`](../../../reference/configuration/hosts.md) list, removing it shifts the positions of all subsequent entries. Since an entry's `node_id` defaults to its position in the list, to keep the `node_id` of the following nodes unchanged (if they are referenced elsewhere, for example, in `state_storage` or `groups` configuration), **explicitly set `node_id` for every entry that follows the one being removed**, using its current one-based position in the list as the value. If the last entry is being removed, this step is not required.

    {% note warning %}

    Correct node numbering is critical for cluster health. An error in `node_id` assignment can move VDisks, State Storage, Board, or SchemeBoard replicas to the wrong host and may lead to irreversible data loss.

    {% endnote %}

    For example, given the following configuration where the node `node3` is being removed:

    ```yaml
    hosts:
    - host: node1
    - host: node2
    - host: node3 # to be removed
    - host: node4
    - host: node5
    ```

    After removing `node3`, explicitly set `node_id` for `node4` and `node5` so that they keep their original IDs (4 and 5) instead of being renumbered to 3 and 4:

    ```yaml
    hosts:
    - host: node1
    - host: node2
    - host: node4
      node_id: 4
    - host: node5
      node_id: 5
    ```

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
