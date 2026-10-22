# Static group SelfHeal

{% include [_](../_includes/experimental_v2.md) %}

When using [configuration V2](index.md), [SelfHeal](../../../maintenance/manual/selfheal.md) can automatically move a static group VDisk from faulty PDisks and restore the group's fault tolerance.

{% note warning %}

On clusters with [configuration V1](../configuration-v1/config-overview.md), static group SelfHeal cannot be enabled.

{% endnote %}

The general SelfHeal mechanism detects a faulty PDisk and initiates VDisk relocation. For dynamic groups, the [Blob Storage Controller](../../../concepts/glossary.md#ds-controller) changes the configuration; for the static group, [distributed configuration](../../../concepts/glossary.md#distributed-configuration) changes it.

To allow distributed configuration to change the static group automatically, enable the [`self_management_config.automatic_static_group_management`](../../../reference/configuration/self_management_config.md#parameters) parameter. This parameter is disabled by default.

## Enabling and disabling static group SelfHeal {#on-off}

For static group SelfHeal to work, the following must be enabled:

* [distributed configuration](../../../concepts/glossary.md#distributed-configuration) V2 — [`self_management_config.enabled: true`](../../../reference/configuration/self_management_config.md#parameters);
* the general SelfHeal mechanism, which is [enabled by default](../../../maintenance/manual/selfheal.md#on-off).

The `self_management_config.enabled` parameter enables distributed configuration itself. The `self_management_config.automatic_static_group_management` parameter separately allows automatic relocation of static group VDisks.

To enable or disable automatic static group management:

1. Fetch the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

   ```bash
   ydb [global options...] admin cluster config fetch > config.yaml
   ```

1. In the `config.yaml` configuration file, set `self_management_config.enabled` and `self_management_config.automatic_static_group_management`:

   ```yaml
   config:
     self_management_config:
       enabled: true
       automatic_static_group_management: true
   ```

   The value `self_management_config.automatic_static_group_management: true` enables automatic static group management; `false` disables it.

1. Apply the updated configuration using the [ydb admin cluster config replace](../../../reference/ydb-cli/commands/configuration/cluster/replace.md) command:

   ```bash
   ydb [global options...] admin cluster config replace -f config.yaml
   ```

## Restricting the set of target nodes {#allowed-nodes}

By default, SelfHeal can move a static group VDisk to any suitable node. To restrict the set of target nodes, specify their IDs in the [`static_group_self_heal_allowed_nodes`](../../../reference/configuration/self_management_config.md#parameters) parameter:

```yaml
config:
  self_management_config:
    enabled: true
    automatic_static_group_management: true
    static_group_self_heal_allowed_nodes:
    - 1
    - 2
    - 3
```

An empty list means there are no restrictions. For each allowed node, choose a suitable PDisk and verify that its free capacity is at least the space occupied by the VDisk on the source PDisk, plus operational headroom up to the capacity warning threshold. There is no single numeric headroom value: use the warning threshold configured in your cluster monitoring. Placement after relocation must satisfy the [failure model](../../../concepts/topology.md#cluster-config).
