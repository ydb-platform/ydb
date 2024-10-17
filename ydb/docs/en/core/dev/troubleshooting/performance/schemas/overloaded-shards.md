# Overloaded shards

[Data shards](../../../../concepts/glossary.md#data-shard) in {{ ydb-short-name }} may become overloaded for the following reasons:

* A table is created without the [AUTO_PARTITIONING_BY_LOAD](../../../../concepts/datamodel/table.md#AUTO_PARTITIONING_BY_LOAD) clause.

    In this case, {{ ydb-short-name }} does not split overloaded shards. If a shard has to process too many queries, it will consume all its CPU resources, leading to increased query response times.

* A table was created with the [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT](../../../../concepts/datamodel/table.md#AUTO_PARTITIONING_MAX_PARTITIONS_COUNT) setting and has already reached its partition limit.

* An inefficient primary key that causes an imbalance in the distribution of queries across shards. A typical example is ingestion with a monotonically increasing primary key, which may overload the "last" partition. For example, this could occur with an autoincrementing primary key using the serial data type.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/overloaded-shards-diagnostics.md) %}

## Recommendations for table configuration {#table-config}

Consider the following solutions to address shard overload:

* If the problematic table is not partitioned by load, enable partitioning by load.

* If the table has reached the maximum number of partitions, increase the partition limit.

## Recommendations for the imbalanced primary key {#pk-recommendations}

Consider modifying the primary key to distribute the load evenly across table partitions. You cannot change the primary key of an existing table. To do that, you will have to create a new table with the modified primary key and then migrate the data to the new table.
