# Table partitioning recommendations

YDB tables are sorted by primary key in ascending order. Tables are partitioned by splitting the range of key values into consecutive non-overlapping ranges called partitions.

Thanks to partitioning, table data can be distributed across multiple storage devices and the load from operations on a table can use more processor cores and network bandwidth. However, too many partitions in a table may add overhead on RAM and CPU time usage. So optimal partitioning directly affects the efficiency of query execution. To achieve optimal partitioning, YDB provides tools for the initial partitioning of a table when creating it as well as methods for subsequent automatic partitioning.

When creating a table, you can set the initial partitioning in one of two ways:

* Set up even partitioning by the first column of the primary key if this column is UInt32 or UInt64. In this case, you need to explicitly specify the number of partitions.
* Set the exact partitioning keys that determine the number and boundaries of the original partitions.

YDB supports two methods for automatic table partitioning:

* By data size.
* By load on a datashard (a system component that serves a table partition).

Automatic partitioning modes can be enabled separately or together. Both modes can split one partition into two and merge several partitions into one. Splitting or merging is limited by the ```MIN_PARTITIONS``` and ```MAX_PARTITIONS``` table settings. When splitting a partition, its key range is divided into two new ranges, while data from these ranges forms new partitions. A merge can be done for several neighboring ranges, while data from several partitions is logically merged into a single partition.

Automatic partitioning by size is parameterized by the value of the partition size setting. When that value is reach, the partition is split. The default value is 2 GB. A key for splitting is selected based on the histogram of data size distribution across partitions by key sub-ranges. If the total data size in adjacent partitions becomes less than half of the setting size, these partitions are merged.

Automatic partitioning by load is triggered based on CPU usage by the datashard serving the partition. All datashards monitor their CPU usage. If a high (>50%) level of usage is detected at some point in time, the partition is split. A key is selected using statistics on accessing the keys of a datashard's own partition.

