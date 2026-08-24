# Configuring the State Storage, Board, and Scheme Board metadata distribution subsystems

Applies if you need to change the [metadata distribution subsystem configuration](../../../reference/configuration/domains_config.md) consisting of [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), and [Scheme Board](../../../concepts/glossary.md#scheme-board) on a {{ ydb-short-name }} cluster.

{% include [warning-configuration-error](../configuration-v1/_includes/warning-configuration-error.md) %}

To change the metadata distribution subsystem configuration in a {{ ydb-short-name }} cluster, perform the following steps.

1. Make the required changes to the `domains_config` sections of the `config.yaml` configuration file on each node of the {{ ydb-short-name }} cluster:
   See [Metadata distribution subsystem configuration rules](#metadata-subsystems-reconfig-rules) for rules on changing the `domains_config` section.
2. Using the [rolling-restart](../../../maintenance/manual/node_restarting.md) procedure, sequentially restart all nodes of the {{ ydb-short-name }} cluster, both [static](../../../concepts/glossary.md#static-node) and [dynamic](../../../concepts/glossary.md#dynamic): metadata subsystem replicas are placed on static nodes, and tablets on dynamic nodes access them. For more on nodes, see [Cluster topology](../../../concepts/topology.md).
   Before restarting the next host, wait for the restart on the previous host to complete and the node to rejoin the cluster.

## Metadata distribution subsystem configuration rules {#metadata-subsystems-reconfig-rules}

The rules listed below apply to [`state_storage`](../../../reference/configuration/domains_config.md#domains-state) and to the separate fields `explicit_state_storage_config`, `explicit_state_storage_board_config`, `explicit_scheme_board_config` in the section [`domains_config`](../../../reference/configuration/domains_config.md) of the file `config.yaml` (see [State Storage configuration](../../../reference/configuration/domains_config.md#domains-state)). The `explicit_*` keys correspond individually to [State Storage](../../../concepts/glossary.md#state-storage), [Board](../../../concepts/glossary.md#board), and [Scheme Board](../../../concepts/glossary.md#scheme-board).

In the configuration, a ring refers to a `ring` block inside a `ring_groups` list element (see State Storage configuration).

Configuration changes are performed in several steps. First, a new group of rings is added, consisting of properly selected nodes (according to the failure model), and then the old group of rings is removed.

To avoid cluster unavailability, add and remove ring groups strictly in the sequence of steps described below.

1. To change the configuration of metadata distribution subsystems without cluster unavailability, do so by adding and removing ring groups.
2. You can only add and remove ring groups with the `WriteOnly: true` parameter.
3. The new configuration must always contain at least one ring group from the previous configuration without the `WriteOnly` parameter. Such a ring group must be first in the list.
4. If different ring groups use the same cluster nodes, add the `ring_group_actor_id_offset` parameter to the ring group with a unique value (for example, `1`, `2`, …). The value must be unique among ring groups.

   This parameter makes replica identifiers unique within a given ring group; they will not match identifiers from other groups, allowing multiple replicas of the same type to be placed on a single cluster node.
5. The transition to the new configuration is performed in 4 sequential steps. At each step, a new configuration is prepared and applied to the cluster.

   Newly created or ready-to-remove ring groups are marked with the `WriteOnly: true` flag. This is necessary so that read requests are handled by the already deployed ring group while the new configuration spreads to the required number of nodes, new replicas are created, or old ones are removed.

   Therefore, pause for at least `1 minute` between steps.

   - Add new group of rings with parameter `WriteOnly: true` corresponding target configuration.
   - Remove flag `WriteOnly`.
   - Set flag `WriteOnly: true` on group of rings corresponding old configuration, new group of rings move to beginning of the list of groups of rings.
   - Remove old group of rings.

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
