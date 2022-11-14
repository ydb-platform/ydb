## Table {#table}

A table in {{ ydb-short-name }} is a [relational table](https://en.wikipedia.org/wiki/Table_(database)) containing a set of related data and made up of rows and columns. Each row is a set of cells that are used for storing specific types of values according to the data schema. The data schema defines the names and types of table columns. An example of a data schema is shown below. The `Series` table consists of four columns named `SeriesId`, `ReleaseDate`, `SeriesInfo`, and `Title` and holding data of type `Uint64?` for the first two and `String?` for the others. The `SeriesId` column is declared the primary key.

![Datamodel_of_a_relational_table](../../_assets/datamodel_rtable.png)

{{ ydb-short-name }} uses [YQL data types](../../datatypes.md). Columns may use [simple YQL data types](../../../yql/reference/types/primitive.md). All columns may contain the special `NULL` value to signify that a value is missing.

{{ ydb-short-name }} tables always have one or more columns that make up the [primary key](https://en.wikipedia.org/wiki/Unique_key). Each table row has a unique key value, so there can be no more than one row per key value. {{ ydb-short-name }} tables are always ordered by key. This means that you can efficiently make point reads by key and range-based queries by key or key prefix (actually using an index). In the example above, the key columns are highlighted in gray and marked with a special sign. Tables consisting only of key columns are supported. However, you can't create tables without a primary key.

Often, when you design a table schema, you already have a set of fields, which can naturally serve as the primary key. Be careful when selecting the key to avoid hotspots. For example, if you insert data into a table with a monotonically increasing key, you write the data to the end of the table. But since {{ ydb-short-name }} splits table data by key range, your inserts are always processed by the same server, so you lose the main benefits of a distributed database. To distribute the load evenly across different servers and to avoid hotspots when processing large tables, we recommend hashing the natural key and using the hash as the first component of the primary key as well as changing the order of the primary key components.

### Partitioning tables {#partitioning}

A database table can be sharded by primary key value ranges. Each shard of the table is responsible for a specific range of primary keys. Key ranges served by different shards do not overlap. Different table shards can be served by different distributed database servers (including ones in different locations). They can also move independently between servers to enable rebalancing or ensure shard operability if servers or network equipment goes offline.

If there is not a lot of data or load, the table may consist of a single shard. As the amount of data served by the shard or the load on the shard grows, {{ ydb-short-name }} automatically splits this shard into two shards. The data is split by the median value of the primary key if the shard size exceeds the threshold. If partitioning by load is used, the shard first collects a sample of the requested keys (that can be read, written, and deleted) and, based on this sample, selects a key for partitioning to evenly distribute the load across new shards. So in the case of load-based partitioning, the size of new shards may significantly vary.

The size-based shard split threshold and automatic splitting can be configured (enabled/disabled) individually for each database table.

In addition to automatically splitting shards, you can create an empty table with a predefined number of shards. You can manually set the exact shard key split range or evenly split it into a predefined number of shards. In this case, ranges are created based on the first component of the primary key. You can set even splitting for tables that have a Uint64 or Uint32 integer as the first component of the primary key.

Partitioning parameters refer to the table itself rather than to secondary indexes built on its data. Each index is served by its own set of shards and decisions to split or merge its partitions are made independently based on the default settings. These settings may become available to users in the future like the settings of the main table.

A split or a merge usually takes about 500 milliseconds. During this time, the data involved in the operation becomes temporarily unavailable for reads and writes. Without raising it to the application level, special wrapper methods in the {{ ydb-short-name }} SDK make automatic retries when they discover that a shard is being split or merged. Please note that if the system is overloaded for some reason (for example, due to a general shortage of CPU or insufficient DB disk throughput), split and merge operations may take longer.

The following table partitioning parameters are defined in the data schema:

#### AUTO_PARTITIONING_BY_SIZE

* Type: `Enum` (`ENABLED`, `DISABLED`).
* Default value: `ENABLED`.

Automatic partitioning by partition size. If a partition size exceeds the value specified by the [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](#auto_partitioning_partition_size_mb) parameter, it is enqueued for splitting. If the total size of two or more adjacent partitions is less than 50% of the [`AUTO_PARTITIONING_PARTITION_SIZE_MB`](#auto_partitioning_partition_size_mb) value, they are enqueued for merging.

#### AUTO_PARTITIONING_BY_LOAD

* Type: `Enum` (`ENABLED`, `DISABLED`).
* Default value: `DISABLED`.

Automatic partitioning by load. If a shard consumes more than 50% of the CPU for a few dozens of seconds, it is enqueued for splitting. If the total load on two or more adjacent shards uses less than 35% of a single CPU core within an hour, they are enqueued for merging.

Performing split or merge operations uses the CPU and takes time. Therefore, when dealing with a variable load, we recommend both enabling this mode and setting [`AUTO_PARTITIONING_MIN_PARTITIONS_COUNT`](#auto_partitioning_min_partitions_count) to a value other than 1 so that decreased load does not cause the number of partitions to drop below the required value resulting in a need to split them again when load increases.

When choosing the minimum number of partitions, it makes sense to consider that one table partition can only be hosted on one server and use no more than 1 CPU core for data update operations. Hence, you can set the minimum number of partitions for a table on which a high load is expected to at least the number of nodes (servers) or, preferably, to the number of CPU cores allocated to the database.

#### AUTO_PARTITIONING_PARTITION_SIZE_MB

* Type: `Uint64`.
* Default value: `2000 MB` ( `2 GB` ).

Partition size threshold in MB. If exceeded, a shard splits. Takes effect when the [`AUTO_PARTITIONING_BY_SIZE`](#auto_partitioning_by_size) mode is enabled.

#### AUTO_PARTITIONING_MIN_PARTITIONS_COUNT

* Type: `Uint64`.
* Default value: `1`.

Partitions are only merged if their actual number exceeds the value specified by this parameter. When using automatic partitioning by load, we recommend that you set this parameter to a value other than 1, so that periodic load drops don't lead to a decrease in the number of partitions below the required one.

#### AUTO_PARTITIONING_MAX_PARTITIONS_COUNT

* Type: `Uint64`.
* Default value: `50`.

Partitions are only split if their number doesn't exceed the value specified by this parameter. With any automatic partitioning mode enabled, we recommend that you set a meaningful value for this parameter and monitor when the actual number of partitions approaches this value, otherwise splitting of partitions will stop sooner or later under an increase in data or load, which will lead to a failure.

#### UNIFORM_PARTITIONS

* Type: `Uint64`.
* Default value: Not applicable.

The number of partitions for uniform initial table partitioning. The primary key's first column must have type `Uint64` or `Uint32`. A created table is immediately divided into the specified number of partitions.

When automatic partitioning is enabled, make sure to set the correct value for [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT](#auto_partitioning_min_partitions_count) to avoid merging all partitions into one immediately after creating the table.

#### PARTITION_AT_KEYS

* Type: `Expression`.
* Default value: Not applicable.

Boundary values of keys for initial table partitioning. It's a list of boundary values separated by commas and surrounded with brackets. Each boundary value can be either a set of values of key columns (also separated by commas and surrounded with brackets) or a single value if only the values of the first key column are specified. Examples: `(100, 1000)`, `((100, "abc"), (1000, "cde"))`.

When automatic partitioning is enabled, make sure to set the correct value for [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT](#auto_partitioning_min_partitions_count) to avoid merging all partitions into one immediately after creating the table.

### Reading data from replicas {#read_only_replicas}

When making queries in {{ ydb-short-name }}, the actual execution of a query to each shard is performed at a single point serving the distributed transaction protocol. By storing data in shared storage, you can run one or more shard followers without allocating additional storage space: the data is already stored in replicated format, and you can serve more than one reader (but there is still only one writer at any given moment).

Reading data from followers allows you:

* To serve clients demanding minimal delay, which is otherwise unachievable in a multi-DC cluster. This is accomplished by executing queries soon after they are formulated, which eliminates the delay associated with inter-DC transfers. As a result, you can both preserve all the storage reliability guarantees of a multi-DC cluster and respond to point read queries in milliseconds.
* To handle read queries from followers without affecting modifying queries running on a shard. This can be useful both for isolating different scenarios and for increasing the partition bandwidth.
* To ensure continued service when moving a partition leader (both in a planned manner for load balancing and in an emergency). It lets the processes in the cluster survive without affecting the reading clients.
* To increase the overall shard read performance if many read queries access the same keys.

You can enable running read replicas for each shard of the table in the table data schema. The read replicas (followers) are typically accessed without leaving the data center network, which ensures response delays in milliseconds.

| Parameter name | Description | Type | Acceptable values | Update possibility | Reset capability |
| ------------- | --------- | --- | ------------------- | --------------------- | ------------------ |
| `READ_REPLICAS_SETTINGS` | `PER_AZ` means using the specified number of replicas in each AZ and `ANY_AZ` in all AZs in total. | String | `"PER_AZ:<count>"`, `"ANY_AZ:<count>"`, where `<count>` is the number of replicas | Yes | No |

The internal state of each of the followers is restored exactly and fully consistently from the leader state.

Besides the data state in storage, followers also receive a stream of updates from the leader. Updates are sent in real time, immediately after the commit to the log. However, they are sent asynchronously, resulting in some delay (usually no more than dozens of milliseconds, but sometimes longer in the event of cluster connectivity issues) in applying updates to followers relative to their commit on the leader. Therefore, reading data from followers is only supported in the [transaction mode](../transactions.md#modes) `StaleReadOnly()`.

If there are multiple followers, their delay from the leader may vary: although each follower of each of the shards retains internal consistency, artifacts may be observed from shard to shard. Please provide for this in your application code. For that same reason, it's currently impossible to perform cross-shard transactions from followers.

### Deleting expired data (TTL) {#ttl}

{{ ydb-short-name }} supports automatic background deletion of expired data. A table data schema may define a column containing a `Datetime` or a `Timestamp` value. A comparison of this value with the current time for all rows will be performed in the background. Rows for which the current time becomes greater than the column value plus specified delay, will be deleted.

| Parameter name | Type | Acceptable values | Update possibility | Reset capability |
| ------------- | --- | ------------------- | --------------------- | ------------------ |
| `TTL` | Expression | `Interval("<literal>") ON <column>` | Yes | Yes |

For more information about deleting expired data, see [Time to Live (TTL)](../../../concepts/ttl.md).

### Renaming a table {#rename}

{{ ydb-short-name }} lets you rename an existing table, move it to another directory of the same database, or replace one table with another, deleting the data in the replaced table. Only the metadata of the table is changed by operations (for example, its path and name). The table data is neither moved nor overwritten.

Operations are performed in isolation, the external process sees only two states of the table: before and after the operation. This is critical, for example, for table replacement: the data of the replaced table is deleted by the same transaction that renames the replacing table. During the replacement, there might be errors in queries to the replaced table that have [retryable statuses](../../../reference/ydb-sdk/error_handling.md#termination-statuses).

The speed of renaming is determined by the type of data transactions currently running against the table and doesn't depend on the table size.

* [Renaming a table in YQL](../../../yql/reference/syntax/alter_table.md#rename)
* [Renaming a table via the CLI](../../../reference/ydb-cli/commands/tools/rename.md)

### Bloom filter {#bloom-filter}

Using a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) lets you more efficiently determine if some keys are missing in a table when making multiple point queries by primary key. This reduces the number of required disk I/O operations but increases the amount of memory consumed.

| Parameter name | Type | Acceptable values | Update possibility | Reset capability |
| ------------- | --- | ------------------- | --------------------- | ------------------ |
| `KEY_BLOOM_FILTER` | Enum | `ENABLED`, `DISABLED` | Yes | No |

