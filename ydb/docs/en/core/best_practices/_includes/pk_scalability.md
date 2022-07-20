# Choosing a primary key for maximum performance

The choice of a table's primary key columns has a decisive impact on YDB load scalability and performance improvement.

General recommendations for choosing a primary key:

* Avoid situations where the main load falls on a single [partition](../../concepts/datamodel.md#partitioning) of a table. The more evenly the load is distributed across partitions, the higher the performance.
* Reduce the number of partitions that can be affected in a single request. Moreover, if the request affects no more than one partition, it is performed using a special simplified protocol. This significantly increases the speed and saves the resources.

All {{ ydb-short-name }} tables are sorted by primary key in ascending order. This means that writes to a data table with a monotonically increasing primary key will cause new data to be written to the end of the table. As {{ ydb-short-name }} splits table data into partitions based on key ranges, inserts are always processed by the same server that is responsible for the "last" partition. Concentrating the load on a single server results in slow data uploading and inefficient use of a distributed system.
As an example, let's take logging of user events to a table with the ```( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )``` schema.

```timestamp``` monotonically increases, and as a result, all records are written to the end of the table and the "last" partition, which is responsible for this range of keys, serves all records in the table. This will make it impossible to scale the write load, performance will be limited to a single process of servicing this partition and won't increase with the addition of servers to the cluster.

{{ ydb-short-name }} supports automatic partition splitting when the threshold size is reached. However, in our case, after splitting, a new partition starts taking all the write load again and the situation repeats.

## Techniques that let you evenly distribute load across table partitions {#balance-shard-load}

### Changing the sequence of key components {#key-order}

Writing data to a table with the ```( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )``` schema results in an uneven load on table partitions due to a monotonically increasing primary key. Changing the sequence of key components so that the monotonically increasing part isn't the first component can help distribute the load more evenly. If you change the table schema to ```( timestamp, userid, userevent, PRIMARY KEY (userid, timestamp) )```, then, with a sufficient number of users that generate events, writing to the database is distributed across the partitions more evenly.

### Using a hash of key column values as a primary key {#key-hash}

Let's take a table with the ```( timestamp, userid, userevent, PRIMARY KEY (userid, timestamp) )``` schema. As the entire primary key or its first component, you can use a hash of the source key. For example:

```
( timestamp, userid, userevent, PRIMARY KEY (HASH(userid), userid, timestamp) )
```

If you select the hashing function correctly, the rows are distributed evenly enough throughout the key space, which results in an even load on the system. In this case, if the ```userid, timestamp``` fields are present in the key after ```HASH(userid)```, this preserves data locality and sorting by time for a specific user.

### Reducing the number of partitions affected by a single query {#decrease-shards}

Let's assume that the main scenario for working with table data is to read all events by a specific ```userid```. Then, when you use the ```( timestamp, userid, userevent, PRIMARY KEY (timestamp, userid) )``` table schema, each read affects all the partitions of the table. Moreover, each partition is fully scanned, since the rows related to a specific ```userid``` are located in an order that isn't known in advance. Changing the sequence of ```( timestamp, userid, userevent, PRIMARY KEY (userid, timestamp) )``` key components causes all rows related to a specific ```userid``` to follow each other. This arrangement of rows has a positive effect on the speed of reading information on a particular ```userid```.

## NULL value in a key column {#key-null}

In {{ ydb-short-name }}, all columns, including key ones, may contain a NULL value. Using NULL as values in key columns isn't recommended. According to the SQL standard (ISO/IEC&nbsp;9075), you can't compare NULL with other values. Therefore, the use of concise SQL statements with simple comparison operators may lead, for example, to skipping rows containing NULL during filtering.

## Row size limit {#limit-string}

To achieve high performance, we don't recommend writing rows larger than 8 MB and key columns larger than 2 KB to the DB.

