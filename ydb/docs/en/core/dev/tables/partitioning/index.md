# How partitioning works {#how-partitioning-works}

A row-oriented table in {{ ydb-short-name }} can be split into **partitions** — non-overlapping ranges of the [primary key](../../../concepts/datamodel/table.md#primary-key). Each partition is served by a [Data shard](../../../concepts/glossary.md#data-shard) tablet; partitions can live on different cluster nodes and move during rebalancing and failures.

The authoritative description of the model, `WITH (...)` parameters, and scaling behavior is in [{#T}](../../../concepts/datamodel/table.md) — see [Row-oriented table partitioning](../../../concepts/datamodel/table.md#partitioning_row_table) and the `AUTO_PARTITIONING_*` parameter list.

In this subsection:

* [{#T}](auto/index.md) — **size-based** and **load-based** modes, plus limits of automatic partitioning;
* [{#T}](choosing-partition-count.md) — choosing minimum and maximum partition counts;
* [{#T}](anti-patterns/index.md) — common schema and configuration mistakes.

{% note info %}

Column-oriented tables partition differently and **do not support** the same automatic repartitioning workflow as row tables. See [{#T}](../../primary-key/column-oriented.md).

{% endnote %}
