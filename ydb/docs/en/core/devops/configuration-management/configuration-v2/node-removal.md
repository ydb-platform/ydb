# Removing a node from a cluster

{% include [_](../_includes/experimental_v2.md) %}

This article describes how to remove a [dynamic](../../../concepts/glossary.md#dynamic) or [static](../../../concepts/glossary.md#static-node) node from a {{ ydb-short-name }} cluster deployed manually on virtual machines or physical servers. Removing nodes from a Kubernetes deployment is outside the scope of this procedure.

## Removing a dynamic node

Removing a dynamic node does not require changing the cluster configuration.

To remove a dynamic node without affecting query processing:

1. [Drain the tablets](../../../maintenance/manual/node_restarting.md#replace-hardware) from the node and wait for the operation to complete.
1. Stop the {{ ydb-short-name }} process on the node. [Check first that the process can be stopped safely, then stop it](../../../maintenance/manual/node_restarting.md#restart_process).

After stopping the process, check the **Nodes** tab on the [cluster monitoring page](../../../reference/ydb-ui/ydb-monitoring.md#node_list_page) and make sure that the removed node is no longer shown as active. A dynamic node may disappear from the list immediately or remain temporarily with the **Disconnected** status until its node ID lease in [NodeBroker](../../../concepts/glossary.md#node-broker) expires. If the node becomes active again, make sure that the process was stopped on the correct host and is not configured to restart automatically, for example, by a `systemd` unit or a supervisor.

## Removing a static node {#remove-static-node}

Static nodes serve the storage system and are listed in the [`hosts`](../../../reference/configuration/hosts.md) section. A static node can contain [VDisks](../../../concepts/glossary.md#vdisk) of [dynamic](../../../concepts/glossary.md#dynamic-group) and [static](../../../concepts/glossary.md#static-group) groups, as well as [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), and [SchemeBoard](../../../concepts/glossary.md#scheme-board) replicas. These resources must be moved before the node is removed from the configuration.

Before starting the procedure, use the [Embedded UI](../../../reference/ydb-ui/ydb-monitoring.md#node_storage_page) to check that the affected storage groups are healthy, that is, all VDisks of these groups are shown in the `Ok` state (highlighted in green), with none in `Error` or `Degraded` state.

The remaining nodes must have enough free [PDisk](../../../concepts/glossary.md#pdisk) space and slots for all VDisks from the node being removed. VDisk placement across [failure domains](../../../concepts/glossary.md#fail-domain) and [failure realms](../../../concepts/glossary.md#fail-realm) must comply with the configured [erasure coding scheme](../../../concepts/glossary.md#erasure-coding) to preserve group fault tolerance after node removal. For details on calculating the required capacity margin, see [{#T}](../../concepts/capacity-planning.md#hardware-estimation).

[SelfHeal](../../../maintenance/manual/selfheal.md) is enabled for dynamic groups by default. Before removing the node, make sure it is also enabled for any resources hosted on this node:

* If the node contains a static group VDisk, [enable static group SelfHeal](static-group-self-heal.md#on-off). Alternatively, you can move the static group VDisk off the node manually, see [{#T}](static-group-move.md).
* If the node contains State Storage, Board, or SchemeBoard replicas, enable [Self Heal State Storage](../../../maintenance/manual/selfheal_statestorage.md#on-off). Alternatively, you can move these replicas off the node manually, see [{#T}](state-storage-reconfiguration.md).

To remove a static node:

1. If tablets are running on the node, [drain them](../../../maintenance/manual/node_restarting.md#replace-hardware).
1. [Check that the process can be stopped safely](../../../maintenance/manual/node_restarting.md#restart_process), then stop it.
1. Wait for SelfHeal to move the VDisks from the node. With the default settings, relocation starts approximately one hour after the node is stopped. To start relocation immediately, first obtain the IDs of all PDisks on the node being removed using [{{ ydb-short-name }} DSTool](../../../reference/ydb-dstool/index.md):

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk list --columns NodeId:PDiskId FQDN Path
    ```

    `<bs_endpoint>` is the endpoint of any available storage node in the `[PROTOCOL://]HOST[:PORT]` format, for example, `http://node1.example.com:8765`.

    Select every row whose `FQDN` matches the host name of the node being removed. Verify the disk paths in the `Path` column and save all corresponding `NodeId:PDiskId` values.

    Then set all identified PDisks to `BROKEN` in one command:

    ```bash
    ydb-dstool -e <bs_endpoint> pdisk set --status BROKEN --unavail-as-offline --pdisk-ids "<pdisk_id_1>" ... "<pdisk_id_N>"
    ```

    Use the same `<bs_endpoint>`. Replace `"<pdisk_id_1>" ... "<pdisk_id_N>"` with a space-separated list of all saved disk IDs in the `[NodeId:PDiskId]` format, for example, `"[3:1]" "[3:2]"` for two disks on the node with `NodeId` equal to `3`. The `--unavail-as-offline` option treats PDisks unavailable through the Whiteboard monitoring service as offline.

    The command runs in the foreground. Wait for it to complete successfully, then verify that data relocation is complete in the next step. For details, see [Move VDisks from a broken/missing block store volume](../../../maintenance/manual/moving_vdisks.md#removal_from_a_broken_device).

1. In the [Embedded UI](../../../reference/ydb-ui/ydb-monitoring.md#node_storage_page), check that no VDisks remain on the node and that the affected storage groups are healthy (all VDisks are in the `Ok` state). If State Storage, Board, or SchemeBoard replicas were moved from the node, [check that the relocation is complete](../../../maintenance/manual/selfheal_statestorage.md#verify-result).
1. Fetch the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. If the node being removed is not the last entry in the [`hosts`](../../../reference/configuration/hosts.md) list, removing it shifts the positions of all subsequent entries. To preserve their identifiers, explicitly set `node_id` on each subsequent entry that does not already have one, using that entry's current identifier before removal: for an entry that relied on the default, this is its current one-based position in the list. If an entry already has an explicit `node_id`, preserve its existing value. If the last entry is being removed, this step is not required.

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
      node_id: 50
    ```

    Before removing `node3`, explicitly set `node_id: 4` for `node4` so that its ID does not change to `3`. Keep the existing `node_id: 50` for `node5`. After removing `node3`, the list will look like this:

    ```yaml
    hosts:
    - host: node1
    - host: node2
    - host: node4
      node_id: 4
    - host: node5
      node_id: 50
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

    In this case, wait for SelfHeal to move the remaining VDisks. Relocation time depends on the amount of data and disk performance. Monitor the relocation on the **Storage** tab of the node being removed in the [Embedded UI](../../../reference/ydb-ui/ydb-monitoring.md#node_storage_page). When no VDisks remain on the node, rerun the `config replace` command with the same file.

    If the VDisk list is not shrinking and replication is not in progress, [move the remaining VDisks manually](../../../maintenance/manual/moving_vdisks.md#removal_from_a_broken_device).

    {% endcut %}

After the configuration is applied successfully, the server and its disks can be decommissioned.
