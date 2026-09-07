# Configuring the State Storage, Board, and Scheme Board metadata distribution subsystems

Applies if you need to change the [metadata distribution subsystem configuration](../../../reference/configuration/domains_config.md#domains-state) consisting of [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), and [Scheme Board](../../../concepts/glossary.md#scheme-board) on a {{ ydb-short-name }} cluster.

When using configuration V2, the metadata distribution subsystems are partially supported automatically through the Self Heal mechanism — see [Self Heal State Storage](../../../maintenance/manual/selfheal_statestorage.md) (moving and adding replicas on topology changes). To disable this behavior, set `state_storage_self_heal_config.enable` to `false`, as described in the same section. Disabling is not required for the steps below: the configuration after `ydb admin cluster config replace` will be applied before the next automatic trigger.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

To manually change the State Storage configuration in a {{ ydb-short-name }} cluster, perform the following steps.

1. Get the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:


  ```bash
    ydb [global options...] admin cluster config fetch --v2-internal-state > config.yaml
  ```


As a result of running this command, the current configuration will be saved to the file `config.yaml`.

2. Make the required changes to the section `domains_config` of the configuration file `config.yaml`:
   For rules on changing the `domains_config` section, see [State Storage configuration rules](#metadata-subsystems-reconfig-rules).
3. Apply the new cluster configuration using the [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md) command:


  ```bash
  ydb [global options...] admin cluster config replace -f config.yaml
  ```


## State Storage configuration rules {#metadata-subsystems-reconfig-rules}

The rules listed below apply to [`state_storage`](../../../reference/configuration/domains_config.md#domains-state) and to the separate fields `explicit_state_storage_config`, `explicit_state_storage_board_config`, `explicit_scheme_board_config` in the section [`domains_config`](../../../reference/configuration/domains_config.md) of the file `config.yaml` (see [State Storage configuration](../../../reference/configuration/domains_config.md#domains-state)). The `explicit_*` keys correspond individually to [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), and [Scheme Board](../../../concepts/glossary.md#scheme-board).

In the configuration, a ring refers to a `ring` block inside a `ring_groups` list element (see State Storage configuration).

Configuration changes are performed in several steps. First, a new group of rings is added, consisting of properly selected nodes (according to the failure model), and then the old group of rings is removed.

To avoid cluster unavailability, add and remove ring groups strictly in the sequence of steps described below.

1. To change the configuration of metadata distribution subsystems without cluster unavailability, do so by adding and removing ring groups.
2. You can only add and remove ring groups with the `WriteOnly: true` parameter.
3. The new configuration must always contain at least one ring group from the previous configuration without the `WriteOnly` parameter. Such a ring group must be first in the list.
4. If different ring groups use the same cluster nodes, add the `ring_group_actor_id_offset:1` parameter to the ring group. The value must be unique among ring groups.

   This parameter makes replica identifiers unique within a given ring group; they will not match identifiers from other groups, allowing multiple replicas of the same type to be placed on a single cluster node.
5. The transition to the new configuration is performed in 4 sequential steps. At each step, a new configuration is prepared and applied to the cluster.

   Newly created or ready-to-remove ring groups are marked with the `WriteOnly: true` flag. This is necessary so that read requests are handled by the already deployed ring group while the new configuration spreads to the required number of nodes, new replicas are created, or old ones are removed.

   Therefore, pause for at least `1 minute` between steps.

   - Add a new ring group with the `WriteOnly: true` parameter corresponding to the target configuration.
   - Remove the `WriteOnly` flag.
   - Set the `WriteOnly: true` flag on the ring group corresponding to the old configuration, and move the new ring group to the beginning of the ring group list.
   - Remove the old ring group.

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
At the first step, prepare [`ring_groups`](../../../reference/configuration/domains_config.md#domains-state) following the [configuration rules](#metadata-subsystems-reconfig-rules): the first ring group matches the **current** configuration from the listings above, the second matches the **target** one and is marked with `WriteOnly: true`. Specify the `ring_group_actor_id_offset` parameter as described in the same rules if the node sets of the groups match.


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

You can verify that the changes have been applied in the `CMS` section of the cluster's Embedded UI (available on port 8765): go to the `Tablets` tab and check the replicas of the metadata subsystem tablets to confirm that the configuration has been picked up.
