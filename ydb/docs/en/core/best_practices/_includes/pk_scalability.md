# Choosing a primary key for maximum performance

Proper design of the table's primary key is important for the performance for both data access and load operations.

General recommendations for choosing a primary key:

* Avoid situations when the significant part of the workload falls on a single [partition](../../concepts/datamodel.md#partitioning) of a table. The more evenly the workload is distributed across the partitions, the higher the performance.
* Reduce the number of table partitions that are affected by a single request. Moreover, if the request affects no more than one partition, it is executed using a special simplified protocol. This significantly increases the speed of execution and conserves the resources.

All {{ ydb-short-name }} tables are sorted by primary key in ascending order. In a table with a monotonically increasing primary key, this will result in new data being added at the end of a table. As {{ ydb-short-name }} splits table data into partitions based on key ranges, inserts are always processed by the same server that is responsible for the "last" partition. Concentrating the load on a single server results in slow data uploading and inefficient use of a distributed system.
As an example, let's take logging of user events to a table with the ```( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )``` schema.

The values in the ```timestamp``` column increase monotonically resulting in all new records being added at the end of a table, and the final partition, which is responsible for this range of keys, handles all the table inserts. This makes the data ingestion process unscalable, because the performance will be limited by the single process servicing the trailing partition of the table.

{{ ydb-short-name }} supports automatic splitting of table partitions based on thresholds for the data volume or workload. However, in this scenario, once the split occurs, the new trailing partition will start handling all the inserts again, and the situation will recur.

## Techniques to evenly distribute the workload across table partitions {#balance-shard-load}

### Changing the sequence of key components {#key-order}

Writing data to a table with the ```( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )``` schema results in an uneven load on table partitions due to a monotonically increasing primary key. Changing the sequence of key components so that the monotonically increasing part isn't the first component can help distribute the workload more evenly. Redefining the table's primary key as ```PRIMARY KEY (userid, timestamp)``` allows {{ ydb-short-name }} to distribute writes with the same timestamp values across multiple table partitions, if there is a sufficient number of users generating events.

### Using a hash of key column values as a primary key {#key-hash}

To obtain a more even distribution of inserts across a table's partitions, make the primary key "prefix" (initial part) values more varied. To do this, make the primary key include the value of a hash of the entire primary key or a part of the primary key.

For instance, the table with the schema ```( timestamp, userid, userevent, PRIMARY KEY (userid, timestamp) )```  can be modified to include the additional field computed as a hash: ```userhash = HASH(userid)```. This would change the table schema as follows:

```
( userhash, userid, timestamp, userevent, PRIMARY KEY (userhash, userid, timestamp) )
```

With the proper choice of a hash function, table rows will be distributed evenly throughout the entire key space, which will result in a more even workload. At the same time, the fact that the key includes ```userid, timestamp``` after ```userhash``` keeps the data local and sorted by time for a specific user.

The ```userhash``` field in the example above must be computed by the application and specified explicitly both for inserting new records into the table and for data access by primary key.

### Reducing the number of partitions affected by a single query {#decrease-shards}

Let's assume that the main scenario for working with table data is to read all events by a specific ```userid```. Querying the table with schema ```( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )``` with the filter over the ```userid```  column requires to access all the partitions of the table. Moreover, each partition will have to be fully scanned, since the rows related to a specific ```userid``` are located in an order that isn't known in advance. Changing the sequence of ```( timestamp, userid, userevent, PRIMARY KEY (userid, timestamp) )``` key components causes all rows related to a specific ```userid``` to follow each other. This row distribution will be useful for reading data by ```userid``` and will reduce load.

## NULL value in a key column {#key-null}

In {{ ydb-short-name }}, all columns, including key ones, may contain a NULL value. Using NULL as values in key columns isn't recommended. According to the SQL standard (ISO/IEC 9075), NULL values cannot be compared with other values. Therefore, the use of concise SQL statements with simple comparison operators may lead, for example, to skipping rows containing NULL during filtering.

## Row size limit {#limit-string}

To achieve high performance, we don't recommend writing rows larger than 8 MB and key columns larger than 2 KB to the DB.
