# Self Heal State Storage

{% note warning %}

These instructions apply only to {{ ydb-short-name }} clusters with **V2 configuration** and **distributed configuration**. On clusters with **V1 configuration**, these steps and commands (including obtaining configuration via `ydb admin cluster config fetch`) are unavailable or will not produce the expected result. Alternatives for V1 are not provided here — see [Migration to V2 configuration](../../devops/configuration-management/migration/migration-to-v2.md).

{% endnote %}

During cluster operation, nodes running {{ ydb-short-name }} may fail entirely.

Self Heal State Storage is the SelfHeal mechanism for metadata distribution subsystems. Storage SelfHeal (disks and storage groups) is described in [{#T}](selfheal.md).

Self Heal State Storage ensures the operability of the [metadata distribution subsystem](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), and [SchemeBoard](../../concepts/glossary.md#scheme-board) of the cluster if it is impossible to quickly restore failed nodes, and automatically increases the number of replicas of these subsystems when new nodes are added to the cluster.

Self Heal State Storage provides:

* Detection of faulty {{ ydb-short-name }} cluster nodes.
* Relocation of replicas of [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), and [SchemeBoard](../../concepts/glossary.md#scheme-board) to other nodes or addition of new replicas.

The Self Heal State Storage component is part of the cluster management system [CMS Sentinel](../../concepts/glossary.md#cms).

## Enabling and disabling Self Heal State Storage {#on-off}

You can enable and disable Self Heal State Storage by changing the configuration.
For the mechanism to work, both [CMS Sentinel](../../concepts/glossary.md#cms) and [distributed configuration](../../concepts/glossary.md#distributed-configuration) must be activated.

1. Get the current cluster configuration using the [ydb admin cluster config fetch](../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:


   ```bash
   ydb [global options...] admin cluster config fetch > config.yaml
   ```

2. Modify the configuration file `config.yaml` by changing the value of parameter `state_storage_self_heal_config.enable` to `true` or `false`:


   ```yaml
   config:
       self_management_config:
           enabled: true # Enabling distributed configuration
       cms_config:
           sentinel_config:
               enable: true # Enabling Sentinel
               state_storage_self_heal_config:
                   enable: true # Enabling self heal state storage
   ```


   For the mechanism to work, both [CMS Sentinel](../../concepts/glossary.md#cms) and [distributed configuration](../../concepts/glossary.md#distributed-configuration) must be activated. Make sure they are enabled.
   For more information about [migrating to configuration V2 and enabling distributed configuration](../../devops/configuration-management/migration/migration-to-v2.md).
   When the `state_storage_self_heal_config.enable` parameter is set to `true`, the mechanism for maintaining the operability and fault tolerance of [StateStorage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), [SchemeBoard](../../concepts/glossary.md#scheme-board) is enabled.
3. Update the cluster configuration taking into account the changes made using [ydb admin cluster config replace](../../reference/ydb-cli/commands/configuration/cluster/replace.md):


   ```bash
   ydb [global options...] admin cluster config replace -f config.yaml
   ```

## Managing automatic configuration changes {#automatic-management}

In addition to the general enable/disable of Self Heal State Storage (parameter `state_storage_self_heal_config.enable`, see [above](#on-off)), in the `self_management_config` section of the `config.yaml` configuration file, you can individually manage automatic configuration changes for each of the metadata distribution subsystems, as well as limit the set of nodes to which Self Heal can move replicas.


```yaml
config:
    self_management_config:
        enabled: true
        automatic_state_storage_management: true
        automatic_state_storage_board_management: true
        automatic_scheme_board_management: true
        state_storage_self_heal_allowed_nodes: [1, 2, 3, 4, 5, 6, 7, 8]
        state_storage_board_self_heal_allowed_nodes: [1, 2, 3, 4, 5, 6, 7, 8]
        scheme_board_self_heal_allowed_nodes: [1, 2, 3, 4, 5, 6, 7, 8]
```


| Parameter | Default value | Description |
| --- | --- | --- |
| `automatic_state_storage_management` | `true` | Allows Self Heal to automatically change the [State Storage](../../concepts/glossary.md#state-storage) configuration. When set to `false`, Self Heal does not change the current State Storage configuration. |
| `automatic_state_storage_board_management` | `true` | Same for [Board](../../concepts/glossary.md#board): allows or disallows Self Heal to automatically change its configuration. |
| `automatic_scheme_board_management` | `true` | Same for [SchemeBoard](../../concepts/glossary.md#scheme-board): allows or disallows Self Heal to automatically change its configuration. |
| `state_storage_self_heal_allowed_nodes` | `[]` (no restrictions) | List of node IDs to which Self Heal can move or on which it can add [State Storage](../../concepts/glossary.md#state-storage) replicas. An empty list means there are no restrictions and any cluster nodes can be used. |
| `state_storage_board_self_heal_allowed_nodes` | `[]` (no restrictions) | Same for [Board](../../concepts/glossary.md#board) replicas. |
| `scheme_board_self_heal_allowed_nodes` | `[]` (no restrictions) | Same for [SchemeBoard](../../concepts/glossary.md#scheme-board) replicas. |

## Additional Self Heal State Storage parameters {#self-heal-config-parameters}

In the `cms_config.sentinel_config.state_storage_self_heal_config` section of the `config.yaml` configuration file, you can configure additional parameters of the Self Heal State Storage mechanism that affect how quickly the mechanism responds to changes and how many replicas of the metadata distribution subsystems are created. In the example below, all parameters are shown with default values:


```yaml
config:
    cms_config:
        sentinel_config:
            enable: true
            state_storage_self_heal_config:
                enable: true
                wait_for_config_step: 60000000
                relax_time: 600000000
                pileup_replicas: false
                override_replicas_in_ring_count: 0
                override_rings_count: 0
                replicas_specific_volume: 200
```


| Parameter | Default value | Description |
| --- | --- | --- |
| `wait_for_config_step` | `60000000` (microseconds, 60 seconds) | Wait time between intermediate steps of applying a new configuration of the metadata distribution subsystems (adding/removing ring groups, clearing the `WriteOnly` flag, see Configuring State Storage). The value is specified in microseconds. |
| `relax_time` | `600000000` (microseconds, 600 seconds) | Minimum interval between two consecutive Self Heal State Storage activations. Until the specified time has elapsed since the previous activation, a repeated configuration change is not started, even if faulty nodes are detected. The value is specified in microseconds. |
| `pileup_replicas` | `false` | Allows placing replicas of different subsystems (State Storage, Board, SchemeBoard) on the same set of nodes. When set to `false`, Self Heal tries to use different nodes for replicas of different subsystems where possible; when set to `true`, nodes already occupied by one subsystem can be reused for the others. |
| `override_replicas_in_ring_count` | `0` (calculated automatically) | Forcibly sets the number of replicas in one ring. If the value is `0`, the number of replicas in the ring is calculated automatically based on `replicas_specific_volume` and the number of available nodes. |
| `override_rings_count` | `0` (calculated automatically) | Forcibly sets the number of rings in the configuration. If the value is `0`, the number of rings is calculated automatically based on the number of available nodes and the cluster topology. |
| `replicas_specific_volume` | `200` | Determines how many cluster nodes should correspond to one additional replica in the ring: one additional replica is added for every `replicas_specific_volume` nodes in the cluster. Used in automatic calculation of the number of replicas if `override_replicas_in_ring_count` is not set (equals `0`). |

## Checking the result {#verify-result}

You can check that the changes have been applied in the `CMS` section of the cluster [Embedded UI](../../reference/embedded-ui/index.md) (available on port 8765): go to the `Sentinel` tab to view the status of Sentinel and Self Heal State Storage.
