# self_management_config

The `self_management_config` section configures [distributed configuration V2](../../concepts/glossary.md#distributed-configuration) and automatic management of cluster components.

## Syntax


```yaml
self_management_config:
  enabled: true
  automatic_static_group_management: true
  static_group_self_heal_allowed_nodes:
  - 1
  - 2
  - 3
```


## Parameters {#parameters}

| Parameter | Default value | Description |
| --- | --- | --- |
| `enabled` | `false` | Enables the distributed configuration mechanism in configuration V2. |
| `automatic_static_group_management` | `false` | Allows distributed configuration to change static group VDisk placement automatically. |
| `static_group_self_heal_allowed_nodes` | `[]` | Restricts the set of nodes to which SelfHeal can move a static group VDisk. Specify a list of node IDs. An empty list means that no restrictions apply. |
