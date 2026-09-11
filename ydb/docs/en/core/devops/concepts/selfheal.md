# SelfHeal

SelfHeal automatically restores the fault tolerance of five types of {{ ydb-short-name }} cluster objects:

1. [Dynamic storage groups](../../concepts/glossary.md#dynamic-group).
2. The [static storage group](../../concepts/glossary.md#static-group).
3. [State Storage](../../concepts/glossary.md#state-storage) replicas.
4. [Board](../../concepts/glossary.md#board) replicas.
5. [SchemeBoard](../../concepts/glossary.md#scheme-board) replicas.

The documentation groups the settings for these objects into two areas: Storage SelfHeal for dynamic and static groups, and Metadata Distribution SelfHeal for State Storage, Board, and SchemeBoard.

When a node or disk fails, SelfHeal waits for the failure to persist before starting relocation. If the node or disk recovers before the mechanism is triggered, relocation does not start. For disks, the default waiting time is about one hour.

{% note info %}

SelfHeal for dynamic storage groups does not depend on the configuration version. SelfHeal for the static group and metadata distribution subsystems is available only with [configuration V2](../configuration-management/configuration-v2/config-overview.md) and distributed configuration enabled. Automatic static group management must also be allowed with the `automatic_static_group_management` parameter.

{% endnote %}

For more details about the mechanisms:

- [Storage SelfHeal](selfheal-storage.md)
- [Metadata Distribution SelfHeal](selfheal-state-storage.md)
