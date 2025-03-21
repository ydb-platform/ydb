# Overloaded shards

[Data shards](../../../concepts/glossary.md#data-shard) serving [row-oriented tables](../../../concepts/datamodel/table.md#row-oriented-tables) may become overloaded for the following reasons:

* A table is created without the [AUTO_PARTITIONING_BY_LOAD](../../../concepts/datamodel/table.md#AUTO_PARTITIONING_BY_LOAD) clause.

    In this case, {{ ydb-short-name }} does not split overloaded shards.

    Data shards are single-threaded and process queries sequentially. Each data shard can accept up to 10,000 operations. Accepted queries wait for their turn to be executed. So the longer the queue, the higher the latency.

    If a data shard already has 10000 operations in its queue, new queries will return an "overloaded" error. Retry such queries using a randomized exponential back-off strategy. For more information, see [{#T}](../queries/overloaded-errors.md).

* A table was created with the [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT](../../../concepts/datamodel/table.md#AUTO_PARTITIONING_MAX_PARTITIONS_COUNT) setting and has already reached its partition limit.

* An inefficient [primary key](../../../concepts/glossary.md#primary-key) that causes an imbalance in the distribution of queries across shards. A typical example is ingestion with a monotonically increasing primary key, which may lead to the overloaded "last" partition. For example, this could occur with an autoincrementing primary key using the serial data type.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/overloaded-shards-diagnostics.md) %}

## Recommendations

### For table configuration {#table-config}

Consider the following solutions to address shard overload:

* If the problematic table is not partitioned by load, enable partitioning by load.

    {% note tip %}

    A table is not partitioned by load, if you see the `Partitioning by load: false` line on the **Diagnostics > Info** tab in the **Embedded UI** or the  `ydb scheme describe` command output.

    {% endnote %}

* If the table has reached the maximum number of partitions, increase the partition limit.

    {% note tip %}

    To determine the number of partitions in the table, see the `PartCount` value on the **Diagnostics > Info** tab in the **Embedded UI**.

    {% endnote %}


Both operations can be performed by executing an [`ALTER TABLE ... SET`](../../../yql/reference/syntax/alter_table/set.md) query.


### For the imbalanced primary key {#pk-recommendations}

Consider modifying the primary key to distribute the load evenly across table partitions. You cannot change the primary key of an existing table. To do that, you will have to create a new table with the modified primary key and then migrate the data to the new table.

{% note info %}

Also, consider changing your application logic for generating primary key values for new rows. For example, use hashes of values instead of values themselves.

{% endnote %}

## Example

For a practical demonstration of how to follow these instructions, see [{#T}](../../examples/schemas/overloaded-shard-simple-case.md).

