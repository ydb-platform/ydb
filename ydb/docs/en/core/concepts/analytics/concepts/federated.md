# Federated queries

[Federated queries](../../../concepts/federated_query/index.md) allow you to query data stored in external systems without first loading it (ETL) into {{ydb-short-name}}. The most popular use case is working with data in S3-compatible object storage.

## How it works

You can create an [external table](../../../concepts/datamodel/external_table.md) in {{ydb-short-name}} that references data in S3. When you execute a SELECT query against such a table, {{ydb-short-name}} initiates a parallel read from all compute nodes. Each node reads and processes only the portion of data it needs.

* Supported formats: [Parquet, CSV, JSON](../../../concepts/federated_query/s3/formats.md) with [various compression algorithms](../../../concepts/federated_query/s3/formats.md#compression).
* Read optimization: {{ydb-short-name}} uses S3 data read optimization mechanisms (partition pruning) for [Hive-style partitioning](../../../concepts/federated_query/s3/partitioning.md) and for [more complex partitioning schemes](../../../concepts/federated_query/s3/partition_projection.md).

![](_includes/s3_read.png){width=600}
