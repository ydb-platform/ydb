# Uploading large data volumes

This section provides recommendations on how to efficiently upload large amounts of data to {{ ydb-short-name }}.

{% if oss == true %}

An example of uploading data in [C++](https://a.yandex-team.ru/arc/trunk/arcadia/kikimr/public/sdk/cpp/examples/batch_upload).

{% endif %}

There are anti-patterns and non-optimal settings for uploading data. They don't guarantee acceptable data uploading performance.
To accelerate data uploads, consider the following recommendations:

* Shard a table when creating it. This lets you effectively use the system bandwidth as soon as you start uploading data.
  By default, a new table consists of a single shard. {{ ydb-short-name }} supports automatic table sharding by data volume. This means that a table shard is divided into two shards when it reaches a certain size.
  The acceptable size for splitting a table shard is 2 GB. As the number of shards grows, the data upload bandwidth increases, but it remains low for some time at first.
  Therefore, when uploading a large amount of data for the first time, we recommend initially creating a table with the desired number of shards. You can calculate the number of shards based on 1 GB of data per shard in a resulting set.
* Insert multiple rows in each transaction to reduce the overhead of the transactions themselves.
  Each transaction in {{ ydb-short-name }} has some overhead. To reduce the total overhead, you should make transactions that insert multiple rows. Good performance indicators terminate a transaction when it reaches 1 MB of data or 100,000 rows.
  When uploading data, avoid transactions that insert a single row.
* Within each transaction, insert rows from the primary key-sorted set to minimize the number of shards that the transaction affects.
  In {{ ydb-short-name }}, transactions that span multiple shards have higher overhead compared to transactions that involve exactly one shard. Moreover, this overhead increases with the growing number of table shards involved in the transaction.
  We recommend selecting rows to be inserted in a particular transaction so that they're located in a small number of shards, ideally, in one.
* You should avoid writing data sequentially in ascending or descending order of the primary key.
  Writing data to a table with a monotonically increasing key causes all new data to be written to the end of the table, since all tables in YDB are sorted by ascending primary key. As YDB splits table data into shards based on key ranges, inserts are always processed by the same server that is responsible for the "last" shard. Concentrating the load on a single server results in slow data uploading and inefficient use of a distributed system.

{% note warning %}

When you create a table in the current version, you can only configure the sharding model (or Partitioning Policy) from the SDK for [Java](https://github.com/yandex-cloud/ydb-java-sdk) and [Python](https://github.com/yandex-cloud/ydb-python-sdk).

{% endnote %}

We recommend the following algorithm for efficiently uploading data to {{ ydb-short-name }}:

  1. Create a table with the desired number of shards based on 1 GB of data per shard.
  2. Sort the source data set by the expected primary key.
  3. Partition the resulting data set by the number of shards in the table. Each part will contain a set of consecutive rows.
  4. Upload the resulting parts to the table shards concurrently.
  5. Make a ```COMMIT``` after every 100,000 rows or 1 MB of data.
