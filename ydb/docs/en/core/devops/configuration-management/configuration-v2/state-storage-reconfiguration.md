# Configuring metadata distribution subsystems State Storage, Board, Scheme Board

Use it if you need to change the [configuration of metadata distribution subsystems](../../../reference/configuration/domains_config.md#domains-state) consisting of [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), [Scheme Board](../../../concepts/glossary.md#scheme-board) on the {{ ydb-short-name }} cluster.

When using configuration V2, the metadata distribution subsystem is partially supported automatically through the Self Heal mechanism — see [Self Heal State Storage](../../../maintenance/manual/selfheal_statestorage.md) (moving and adding replicas during topology changes). To disable this behavior, set `state_storage_self_heal_config.enable` to `false`, as described in the same section. Disabling is not required for the steps below: the configuration after `ydb admin cluster config replace` will be applied before the next automatic trigger.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

To manually change the State Storage configuration in the {{ ydb-short-name }} cluster, follow these steps.

1. Get the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:


```bash
  ydb [global options...] admin cluster config fetch --v2-internal-state > config.yaml
```


As a result of running this command, the current configuration will be saved to the file `config.yaml`

2. Make the required changes in the `domains_config` sections of the configuration file `config.yaml`:
   For rules on modifying the `domains_config` section, see the [State Storage Configuration Rules](#metadata-subsystems-reconfig-rules) section.
3. Apply the new cluster configuration using [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md):


```bash
ydb [global options...] admin cluster config replace -f config.yaml
```


## State Storage configuration rules {#metadata-subsystems-reconfig-rules}

The rules listed below apply to [`state_storage`](../../../reference/configuration/domains_config.md#domains-state) and to the separate fields `explicit_state_storage_config`, `explicit_state_storage_board_config`, `explicit_scheme_board_config` in the [`domains_config`](../../../reference/configuration/domains_config.md) section of the `config.yaml` file (see [State Storage configuration](../../../reference/configuration/domains_config.md#domains-state)). The `explicit_*` keys correspond individually to [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), and [Scheme Board](../../../concepts/glossary.md#scheme-board).

In the configuration, a ring refers to a `ring` block inside a list element `ring_groups` (see State Storage Configuration).

Configuration changes are made in several steps. First, a new ring group consisting of properly selected nodes (according to the failure model) is added, and then the old ring group is removed.

To avoid cluster unavailability, perform the removal and addition of ring groups strictly in the sequence of steps described below.

1. To change the configuration of the metadata distribution subsystem without cluster unavailability, you must do so by adding and removing ring groups.
2. You can only add and remove ring groups with the `WriteOnly: true` parameter.
3. The new configuration must always contain at least one ring group from the previous configuration without the `WriteOnly` parameter. Such a ring group must be first in the list.
4. If the same cluster nodes are used in different ring groups, add the `ring_group_actor_id_offset:1` parameter to the ring group. The value must be unique among ring groups.

   This parameter will make the replica identifiers in this ring group unique; they will not match identifiers from other groups, and this will allow placing multiple replicas of the same type on a single cluster node.
5. The transition to the new configuration is performed in 4 sequential steps. At each step, a new configuration is prepared and applied to the cluster.

   Newly created or ring groups ready for deletion are marked with the `WriteOnly: true` flag. This is necessary so that read requests are handled by the already deployed ring group while the new configuration propagates to the required number of nodes, new replicas are created, or old ones are deleted.

   Therefore, a pause of at least `1 minute` must be made between steps.

   - Add a new ring group with the `WriteOnly: true` parameter corresponding to the target configuration.
   - Remove the `WriteOnly` flag.
   - Set the `WriteOnly: true` flag on the ring group corresponding to the old configuration, move the new ring group to the beginning of the ring group list.
   - Delete the old ring group.

## Example

Consider the current configuration as an example:


```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring:
        nto_select: 5
        node: [1,2,3,4,5,6,7,8]
```


and the target configuration:


```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring:
        nto_select: 5
        node: [10,20,30,40,5,6,7,8]
```


We want to move some replicas to other cluster nodes.

**Step 1**
At the first step, prepare [`ring_groups`](../../../reference/configuration/domains_config.md#domains-state) following the [configuration rules](#metadata-subsystems-reconfig-rules): the first ring group matches the **current** configuration from the listings above, the second matches the **target** and is marked with `WriteOnly: true`. Specify the `ring_group_actor_id_offset` parameter as described in the same rules if the node sets of the groups match.


```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring_groups:
        - ring:
          nto_select: 5
          node: [1,2,3,4,5,6,7,8]
        - ring:
          nto_select: 5
          node: [10,20,30,40,5,6,7,8]
          write_only: true
          ring_group_actor_id_offset: 1
```


**Step 2**
Remove the `WriteOnly` flag.


```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring_groups:
        - ring:
          nto_select: 5
          node: [1,2,3,4,5,6,7,8]
        - ring:
          nto_select: 5
          node: [10,20,30,40,5,6,7,8]
          ring_group_actor_id_offset: 1
```


**Step 3**
Make the new ring group first in the list. Set the `WriteOnly: true` flag on the old configuration.


```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring_groups:
        - ring:
          nto_select: 5
          node: [10,20,30,40,5,6,7,8]
          ring_group_actor_id_offset: 1
        - ring:
          nto_select: 5
          node: [1,2,3,4,5,6,7,8]
          write_only: true
```


**Step 4**
Apply the target configuration to the cluster:


```yaml
config:
  domains_config:
    explicit_scheme_board_config:
      ring_groups:
        - ring:
          nto_select: 5
          node: [10,20,30,40,5,6,7,8]
          ring_group_actor_id_offset: 1
```


## Verifying the result {#verify-result}

You can verify that the changes have been applied in the `CMS` section of the cluster's [Embedded UI](../../../reference/embedded-ui/index.md) (available on port 8765): go to the `Tablets` tab and check the replicas of the metadata subsystem tablets to confirm that the configuration has been picked up.
