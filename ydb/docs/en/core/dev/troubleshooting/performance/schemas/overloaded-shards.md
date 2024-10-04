# Overloaded shards

## Description

[Data shards](../../../../concepts/glossary.md#data-shard) in {{ ydb-short-name }} might get overloaded for the following reasons:

* A table is created without the [AUTO_PARTITIONING_BY_LOAD](../../../../concepts/datamodel/table.md#AUTO_PARTITIONING_BY_LOAD) clause.

    In this case {{ ydb-short-name }} does not split overloaded shards. If a shard has to process too many queries, it will use up all its CPU resources and, consequently, query response times will increase.

* A table was created with the [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT](../../../../concepts/datamodel/table.md#AUTO_PARTITIONING_MAX_PARTITIONS_COUNT) setting and has already reached the partition limit.

* Inefficient primary key that results in the imbalance of queries between shards. A typical example is ingestion with a monotonically increasing (autoincrementing) primary key that might overload the "last" partition.

## Diagnostics

{% include notitle [#](_includes/overloaded-shards-diagnostics.md) %}

## Recommendations for table configuration {#table-config}

Consider the following solutions to address the shard overload issue:

* If the problem table is not partitioned by load, enable partitioning by load.

* If the table has reached the maximum number of partitions, increase the partition limit.

## Recommendations for the imbalanced primary key {#pk-recommendations}

Consider changing the primary key to evenly distribute the load between table partitions.