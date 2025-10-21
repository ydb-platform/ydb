{% note tip %}

New {{ ydb-short-name }} clusters are recommended to be deployed using [configuration V2](../configuration-v2/index.md). If a cluster was deployed using [configuration V1](../configuration-v1/index.md), it will still use it after updating to {{ ydb-short-name }} version 25.1 or higher. After such an update, it is recommended to plan and perform [migration to V2](../migration/migration-to-v2.md), because support for V1 will be discontinued in future versions of {{ ydb-short-name }}. For the instructions on how to determine the configuration version of the cluster, see [{#T}](../check-config-version.md).

{% endnote %}