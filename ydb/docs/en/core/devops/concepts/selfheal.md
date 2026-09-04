# SelfHeal

{{ ydb-short-name }} has two automatic recovery (SelfHeal) mechanisms:

1. **Storage SelfHeal** — for disks and [storage groups](../../concepts/glossary.md#storage-group) that hold data.
2. **Self Heal State Storage** — for [State Storage](../../concepts/glossary.md#state-storage), [Board](../../concepts/glossary.md#board), and [SchemeBoard](../../concepts/glossary.md#scheme-board) replicas.

Both mechanisms restore cluster fault tolerance after prolonged failures. If a faulty node or disk is restored before the timeout expires (about one hour by default for disks), SelfHeal does not start relocation.

{% note info %}

Self Heal State Storage is available only with [configuration V2](../configuration-management/configuration-v2/config-overview.md).

Storage SelfHeal does not depend on the configuration version.

{% endnote %}

For more details about the mechanisms:

- [Storage SelfHeal](selfheal-storage.md)
- [Self Heal State Storage](selfheal-state-storage.md)
