# Uploading data to {{ ydb-short-name }}

This section provides recommendations on efficiently uploading data to {{ ydb-short-name }}.

There are anti-patterns and non-optimal settings for uploading data. They don't guarantee acceptable data uploading performance.
To accelerate data uploads, consider the following recommendations:

* Shard a table when creating it. This lets you effectively use the system bandwidth as soon as you start uploading data.
   By default, a new table consists of a single shard. {{ ydb-short-name }} supports automatic table sharding by data volume. This means that a table shard is divided into two shards when it reaches a certain size.
   The acceptable size for splitting a table shard is 2 GB. As the number of shards grows, the data upload bandwidth increases, but it remains low for some time at first.
   Therefore, when uploading a large amount of data for the first time, we recommend initially creating a table with the desired number of shards. You can calculate the number of shards based on 1 GB of data per shard in a resulting set.
* Insert multiple rows in each transaction to reduce the overhead of the transactions themselves.
   Each transaction in {{ ydb-short-name }} has some overhead. It is recommended to make transactions that insert multiple rows to reduce the total overhead. Good performance indicators terminate a transaction when it reaches 1 MB of data or 100,000 rows.
    When uploading data, avoid transactions that insert a single row.
* Within each transaction, insert rows from the primary key-sorted set to minimize the number of shards affected by each transaction.
   In {{ ydb-short-name }}, transactions that span multiple shards have a higher overhead compared to transactions that involve exactly one shard. Moreover, this overhead increases with the growing number of table shards involved in the transaction.
   We recommend selecting rows to be inserted in a particular transaction so that they're located in a small number of shards, ideally, in one.
* If you need to push data to multiple tables, we recommend pushing data to a single table within a single query.
* If you need to push data to a table with a synchronous secondary index, we recommend that you first push data to a table and, when done, build a secondary index.
* You should avoid writing data sequentially in ascending or descending order of the primary key.
   Writing data to a table with a monotonically increasing key causes all new data to be written to the end of the table since all tables in YDB are sorted by ascending primary key. As YDB splits table data into shards based on key ranges, inserts are always processed by the same server responsible for the "last" shard. Concentrating the load on a single server will result in slow data uploading and inefficient use of a distributed system.
* Some use cases require writing the initial data (often large amounts) to a table before enabling OLTP workloads. In this case, transactionality at the level of individual queries is not required, and you can use `BulkUpsert` calls in the [CLI](../reference/ydb-cli/export-import/tools-restore.md), [SDK](../recipes/ydb-sdk/bulk-upsert.md) and API. Since no transactionality is used, this approach has a much lower overhead than YQL queries. In case of a successful response to the query, the `BulkUpsert` method guarantees that all data added within this query is committed.

{% note warning %}

The `BulkUpsert` method isn't supported for tables with secondary indexes.

{% endnote %}


We recommend the following algorithm for efficiently uploading data to {{ ydb-short-name }}:

1. Create a table with the desired number of shards based on 1 GB of data per shard.
2. Sort the source data set by the expected primary key.
3. Partition the resulting data set by the number of shards in the table. Each part will contain a set of consecutive rows.
4. Upload the resulting parts to the table shards concurrently.
5. Make a `COMMIT` after every 100,000 rows or 1 MB of data.
