# Static group SelfHeal

{% include [_](../_includes/experimental_v2.md) %}

When using [configuration V2](index.md), [SelfHeal](../../../maintenance/manual/selfheal.md) can automatically move a static group VDisk from a faulty PDisk and restore the group's fault tolerance.

The general SelfHeal mechanism detects a faulty PDisk and initiates VDisk relocation. For dynamic groups, the Blob Storage Controller changes the configuration, while distributed configuration changes the static group configuration.

To allow distributed configuration to change the static group automatically, enable the [`automatic_static_group_management`](../../../reference/configuration/self_management_config.md#parameters) parameter. This parameter is disabled by default.

## Enabling and disabling static group SelfHeal {#on-off}

Static group SelfHeal requires the following components to be enabled:

* the distributed configuration mechanism in configuration V2 — [`self_management_config.enabled: true`](../../../reference/configuration/self_management_config.md#parameters);
* the general SelfHeal mechanism, which is [enabled by default](../../../maintenance/manual/selfheal.md#on-off).

To enable or disable automatic static group management:

1. Fetch the current cluster configuration using the [ydb admin cluster config fetch](../../../reference/ydb-cli/commands/configuration/cluster/fetch.md) command:

    ```bash
    ydb [global options...] admin cluster config fetch > config.yaml
    ```

1. Set the `automatic_static_group_management` parameter in the `config.yaml` configuration file:

    ```yaml
    config:
      self_management_config:
        enabled: true
        automatic_static_group_management: true
    ```

    `true` enables automatic static group management, while `false` disables it.

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

An empty list means that no restrictions apply. The allowed nodes must have suitable PDisks and enough free space to move the VDisk without violating the failure model.
