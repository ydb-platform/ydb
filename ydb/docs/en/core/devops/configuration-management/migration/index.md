# Cluster Configuration Migration

{{ ydb-short-name }} supports two configuration management mechanisms: [V1](../configuration-v1/index.md) and [V2](../configuration-v2/config-overview.md) (recommended, available from version 25.1). Key differences between them are described in the article [Comparing configurations V1 and V2](../compare-configs.md).

{% include [_](../_includes/configuration-version-note.md) %}

Depending on the current state of your cluster, you can perform migration:

* **[To configuration V2](migration-to-v2.md):** If your cluster is running under configuration V1 management (for example, was deployed on {{ ydb-short-name }} version before 25.1 and has been updated to 25.1), you can switch to the configuration V2 mechanism.
* **[To configuration V1](migration-to-v1.md):** If unexpected problems arose after switching to configuration V2, or you need to roll back the {{ ydb-short-name }} version below 25.1, you can perform reverse migration to manual configuration management (V1).

{% note info "Problems during migration?" %}

If unexpected problems arise when using migration instructions (especially when rolling back to V1), it is recommended to report them immediately {% if audience != "corp" %}as a [GitHub issue](https://github.com/ydb-platform/ydb/issues/new){% else %}to {{ ydb-full-name }} technical support{% endif %}, providing maximum context and diagnostics for reproduction.

{% endnote %}

Before performing migration, make sure to determine which configuration version is currently used in your cluster using the [version check instructions](../check-config-version.md).