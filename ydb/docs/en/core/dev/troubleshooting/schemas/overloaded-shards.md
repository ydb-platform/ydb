# Overloaded shards

## Description

[Data shards](../../../concepts/glossary.md#data-shard) in {{ ydb-short-name }} might get overloaded for the following reasons:

* A table is created without the [AUTO_PARTITIONING_BY_LOAD](../../../concepts/datamodel/table.md#AUTO_PARTITIONING_BY_LOAD) clause.

    In this case {{ ydb-short-name }} does not split overloaded shards. If a shard has to process too many queries, it will use up all its CPU resources and, consequently, query response times will increase.

* A table was created with the [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT](../../../concepts/datamodel/table.md#AUTO_PARTITIONING_MAX_PARTITIONS_COUNT) setting and has already reached the partition limit.

## Diagnostics

{% include notitle [#](_includes/overloaded-shards-diagnostics.md) %}

## Recommendations

Consider the following solutions to address the shard overload issue:

* If the problem table is not partitioned by load, enable partitioning by load.

* If the table has reached the maximum number of partitions, increase the partition limit.