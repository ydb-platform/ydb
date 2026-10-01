# Configuration V1

This section of the {{ ydb-short-name }} documentation describes Configuration V1, the primary way to configure {{ ydb-short-name }} clusters.

Configuration V1 is a two-level {{ ydb-short-name }} cluster configuration system consisting of [static configuration](../configuration-v1/static-config.md) and [dynamic configuration](../configuration-v1/dynamic-config.md):

1. **Static configuration**: a YAML format file that is located locally on each static node and used when starting the `ydbd server` process. This configuration contains, among other things, [static group](../../../concepts/glossary.md#static-group) and [State Storage](../../../concepts/glossary.md#state-storage) settings.
2. **Dynamic configuration**: a YAML format file that is an extended version of static configuration. It is loaded via [CLI](../../../recipes/ydb-cli/index.md) and reliably stored in the [Console tablet](../../../concepts/glossary.md#console), which then distributes the configuration to all dynamic cluster nodes. Using dynamic configuration is optional.

You can learn more about Configuration V1 in the [{#T}](config-overview.md) section.

Main materials:

- [{#T}](config-overview.md)
- [Static configuration](static-config.md)
- [{#T}](dynamic-config.md)
- [{#T}](dynamic-config-volatile-config.md)
- [Cluster DSL configuration](dynamic-config-selectors.md)
- [{#T}](cms.md)
- [{#T}](change_actorsystem_configs.md)
- [{#T}](cluster-expansion.md)
- [{#T}](state-storage-reconfiguration.md)
- [{#T}](state-storage-move.md)
- [{#T}](static-group-move.md)
- [Replacing node FQDN](replacing-nodes.md)
- [Authentication and authorization of database nodes](node-authorization.md)
