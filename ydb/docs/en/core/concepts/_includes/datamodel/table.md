## Table {#table}

A table in YDB is a [relational table](https://en.wikipedia.org/wiki/Table_(database)) that contains a set of related data consisting of rows and columns. Each row is a set of cells designed to store values of certain types according to the data schema. The data schema defines the names and types of table columns. An example of a data schema is shown below.

![Datamodel_of_a_relational_table](../../_assets/datamodel_rtable.png)

<small>Figure 1. Sample table schema</small>

Figure 1 shows the schema of a ```Series``` table with four columns, named ```SeriesId```, ```ReleaseDate```, ```SeriesInfo```, and ```Title```, with the ```Uint64?``` data type for the first two columns and ```String?``` for the others. The ```SeriesId``` column is declared the primary key.

YDB uses [YQL]{% if audience != "external" %}(https://yql.yandex-team.ru/docs/ydb/types/){% else %}(../../datatypes.md){% endif %} data types. {% if audience != "external" %} [Simple YQL data types](https://yql.yandex-team.ru/docs/ydb/types/primitive/) {% else %} [Simple YQL data types](../../../yql/reference/types/primitive.md) {% endif %} can be used as column types. All columns may contain a special NULL value to indicate a missing value.

YDB tables always have one or more columns that make up the key ([primary key](https://en.wikipedia.org/wiki/Unique_key)). Each table row has a unique key value, so there can be no more than one row per key value. A YDB table is always ordered by key. This means that you can efficiently make point reads by key and range-based queries by key or key prefix (actually using an index). In the example above, the key columns are highlighted in gray and marked with a special sign. Tables consisting only of key columns are supported. However, you can't create tables without a primary key.

Often, when you design a table schema, you already have a set of fields, which can naturally serve as the primary key. Be careful when selecting the key to avoid hotspots.
For example, if you insert data into a table with a monotonically increasing key, you write the data to the end of the table. However, since YDB splits table data by key range, your inserts are always processed by the same server, so you lose the main benefits of a distributed database.
To distribute the load evenly across different servers and avoid hotspots when working with large tables, we recommend hashing the natural key and using the hash as the first component of the primary key, as well as changing the order of the primary key components.

### Partitioning tables {#partitioning}

A database table can be sharded by primary key value ranges. Each shard of the table is responsible for a specific range of primary keys. Key ranges served by different shards do not overlap. Different table shards can be served by different distributed database servers (including ones in different locations). They can also move independently between servers to enable rebalancing or ensure shard operability if servers or network equipment goes offline.

If there is not a lot of data, the table may consist of a single shard. As the amount of data served by the shard grows, YDB automatically splits the shard into two shards. The data is split by the median value of the primary key, depending on the amount of data: as soon as the shard exceeds the set data volume, it is split in two.

The shard split threshold and automatic splitting can be configured (enabled/disabled) individually for each database table.

In addition to automatically splitting shards, you can create an empty table with a predefined number of shards. You can manually set the exact shard key split range or evenly split into a predefined number of shards. In this case, ranges are created based on the first component of the primary key. You can set even splitting for tables that have an integer as the first component of the primary key.

The following table partitioning parameters are defined in the data schema:

| Parameter name | Description | Type | Acceptable values | Update possibility | Reset capability |
| ------------- | --------- | --- | ------------------- | --------------------- | ------------------ |
| ```AUTO_PARTITIONING_BY_SIZE``` | Automatic partitioning by partition size | Enum | ```ENABLED```, ```DISABLED``` | Yes | No |
| ```AUTO_PARTITIONING_PARTITION_SIZE_MB``` | Preferred size of each partition, in MB | Uint64 | Natural numbers | Yes | No |
| ```AUTO_PARTITIONING_MIN_PARTITIONS_COUNT``` | The minimum number of partitions before automatic partition merging stops | Uint64 | Natural numbers | Yes | No |
| ```AUTO_PARTITIONING_MAX_PARTITIONS_COUNT``` | The maximum number of partitions before automatic partitioning stops | Uint64 | Natural numbers | Yes | No |
| ```UNIFORM_PARTITIONS``` | The number of partitions for uniform initial table partitioning. The type of the primary key's first column must be Uint64 or Uint32 | Uint64 | Natural numbers | No | No |
| ```PARTITION_AT_KEYS``` | Boundary values of keys for initial table partitioning. It's a list of boundary values separated by commas and surrounded with brackets. Each boundary value can be either a set of values of key columns (also separated by commas and surrounded with brackets) or a single value if only the values of the first key column are specified. Examples: ```(100, 1000)```, ```((100, "abc"), (1000, "cde"))``` | Expression |  | No | No |

### Reading data from replicas {#read_only_replicas}

When making queries in YDB, the actual execution of a query to each shard is performed at a single point serving the distributed transaction protocol. By storing data in shared storage, you can run one or more shard followers without allocating additional space in the storage: the data is already stored in replicated format and you can serve more than one reader (but the writer is still strictly one at every moment).

Reading data from followers allows you:

* Serving clients demanding minimal delays otherwise unachievable in a multi-DC cluster. This is achieved by bringing the query execution point closer to the query formulation point, which eliminates the delay associated with inter-DC transfers. As a result, you can both preserve all the storage reliability guarantees of the multi-DC cluster and respond to occasional read queries in milliseconds.
* Handling read queries from followers without affecting the modifying queries running on the shard. This can be useful both for isolating different scenarios and for increasing the partition bandwidth.
* Ensuring continued maintenance when moving the partition leader (both in regular cases (balancing) and in an emergency). It lets the processes in the cluster survive without affecting the reading clients.
* Increasing the general limit of the shard data read performance if many read queries access the same keys.

You can enable running read replicas for each shard of the table in the table data schema. The read replicas (followers) are typically accessed without leaving the data center network, which ensures response delays in milliseconds.

| Parameter name | Description | Type | Acceptable values | Update possibility | Reset capability |
| ------------- | --------- | --- | ------------------- | --------------------- | ------------------ |
| ```READ_REPLICAS_SETTINGS``` | ```PER_AZ``` means using the specified number of replicas in each AZ and ```ANY_AZ``` in all AZs in total. | String | ```"PER_AZ:<count>"```, ```"ANY_AZ:<count>"```, where ```<count>``` is the number of replicas | Yes | No |

The internal state of each of the followers is restored exactly and fully consistently from the leader state.

Besides the data status in storage, followers also receive a stream of updates from the leader. Updates are sent in real time, immediately after the commit to the log. However, they are sent asynchronously, resulting in some delay (usually no more than dozens of milliseconds, but sometimes longer in the event of cluster connectivity issues) in applying updates to followers relative to their commit on the leader. Therefore, reading data from followers is only supported in the [transaction mode](../transactions#modes) `StaleReadOnly()`.

If there are multiple followers, their delay from the leader may vary: although each follower of each of the shards retains internal consistency, artifacts may be observed between different shards. Please allow for this in your application code. For that same reason, it's currently impossible to perform cross-shard transactions from followers.

### Deleting expired data (TTL) {#ttl}

YDB enables an automatic background delete of expired data. The table data schema may have a column with a Datetime or Timestamp value defined. Its value for all rows will be compared with the current time in the background. Rows for which the current time becomes greater than the column value, factoring in the specified delay, will be deleted.

| Parameter name | Type | Acceptable values | Update possibility | Reset capability |
| ------------- | --- | ------------------- | --------------------- | ------------------ |
| ```TTL``` | Expression | ```Interval("<literal>") ON <column>``` | Yes | Yes |

For more information about deleting expired data, see [Time to Live (TTL)](../../../concepts/ttl.md)

### Bloom filter {#bloom-filter}

Using a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) lets you more efficiently determine if some keys are missing in a table when making multiple point queries by primary key. This reduces the number of required disk I/O operations but increases the amount of memory consumed.

| Parameter name | Type | Acceptable values | Update possibility | Reset capability |
| ------------- | --- | ------------------- | --------------------- | ------------------ |
| ```KEY_BLOOM_FILTER``` | Enum | ```ENABLED```, ```DISABLED``` | Yes | No |

