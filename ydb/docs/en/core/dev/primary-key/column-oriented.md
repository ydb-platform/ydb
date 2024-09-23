# Selecting a primary key for maximum column-oriented table performance

Unlike row-oriented {{ ydb-short-name }} tables, you can't partition column-oriented tables by primary keys but only by specially designated partitioning keys. Inside each partition, data is distributed by the table's primary key.

In this context, to ensure high performance of column-oriented {{ ydb-short-name }} tables, you need to properly select both the primary key and partitioning key.

Because the partitioning key determines the partition where the data is stored, select it so that the Hash(partition_key) enables uniform distribution of data across partitions. The optimal situation is when the partitioning key includes 100 to 1,000 times more unique values than the number of partitions in the table.

Keys with many unique values have high cardinality. On the other hand, keys with a few unique values have low cardinality. Using a partitioning key with low cardinality might result in uneven distribution of data across partitions, and some partitions might become overloaded. Overloading of partitions might result in suboptimal query performance and/or capping of the maximum stream of inserted data.

The primary key determines how the data will be stored inside the partition. That's why, when selecting a primary key, you need to keep in mind both the effectiveness of reading data from the partition and the effectiveness of inserting data into the partition. The optimum insert use case is to write data to the beginning or end of the table, making rare local updates of previously inserted data. For example, an effective use case would be to store application logs by timestamps, adding records to the end of the partition using the current time in the primary key.

{% note warning %}

Currently, you need to select the primary key so that the first column in the primary key has high cardinality. An example of an effective first column is the column with the `Timestamp` type. For example, the first column is ineffective if it has the `Uint16` type and 1,000 unique values.

{% endnote %}

Column-oriented tables do not support automatic repartitioning at the moment. That's why it's important to specify a realistic number of partitions at table creation. You can evaluate the number of partitions you need based on the expected data amounts you are going to add to the table. The average insert throughput for a partition is 1 MB/s. The throughput is mostly affected by the selected primary keys (the need to sort data inside the partition when inserting data). We do not recommend setting up more than 128 partitions for small data streams.

Example:

When your data stream is 1 GB per second, an analytical table with 1,000 partitions is an optimal choice. Nevertheless, it is not advisable to create tables with an excessive number of partitions: this could raise resource consumption in the cluster and negatively impact the query rate.