# Configuration V1

{% include [deprecated](_includes/deprecated.md) %}

This section of the {{ ydb-short-name }} documentation describes Configuration V1, which is the main way to configure {{ ydb-short-name }} clusters deployed using {{ ydb-short-name }} versions below v25.1.

Configuration V1 is a two-level {{ ydb-short-name }} cluster configuration system consisting of [static configuration](../configuration-v1/static-config.md) and [dynamic configuration](../configuration-v1/dynamic-config.md):

1. **Static configuration**: a YAML format file that is located locally on each static node and used when starting the `ydbd server` process. This configuration contains, among other things, [static group](../../../concepts/glossary.md#static-group) and [State Storage](../../../concepts/glossary.md#state-storage) settings.

2. **Dynamic configuration**: a YAML format file that is an extended version of static configuration. It is loaded via [CLI](../../../recipes/ydb-cli/index.md) and reliably stored in the [Console tablet](../../../concepts/glossary.md#console), which then distributes the configuration to all dynamic cluster nodes. Using dynamic configuration is optional.

You can learn more about Configuration V1 in the [{#T}](config-overview.md) section.

Starting from version v25.1, {{ ydb-short-name }} supports [Configuration V2](../configuration-v2/index.md), a unified approach to configuration in a single file format. When using Configuration V2, automatic configuration of [static group](../../../concepts/glossary.md#static-group) and [State Storage](../../../concepts/glossary.md#state-storage) becomes possible. When deploying new clusters on {{ ydb-short-name }} version v25.1 and above, it is recommended to use Configuration V2.

Main materials:

- [{#T}](config-overview.md)
- [{#T}](static-config.md)
- [{#T}](dynamic-config.md)
- [{#T}](dynamic-config-volatile-config.md)
- [{#T}](dynamic-config-selectors.md)
- [{#T}](cms.md)
- [{#T}](change_actorsystem_configs.md)
- [{#T}](cluster-expansion.md)
- [{#T}](state-storage-move.md)
- [{#T}](static-group-move.md)
- [{#T}](replacing-nodes.md)
- [{#T}](node-authorization.md)