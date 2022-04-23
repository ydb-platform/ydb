## Partitions {#partitions}

* **partition_stats**

A system view that provides detailed information about individual partitions of all DB tables. Contains information about instant metrics, such as CPU load or count of in-flight transactions, as well as cumulative counters of a variety of operations on a partition (for example, total number of rows read). Primarily designed for detecting various irregularities in the load on a table partition or in the size of table partition data.

Table structure:

| **Field** | **Type** | **Key** | **Value** |
| --- | --- | --- | --- |
| OwnerId | Uint64 | 0 | ID of the SchemeShard serving the table |
| PathId | Uint64 | 1 | Path ID in the SchemeShard |
| PartIdx | Uint64 | 2 | Partition sequence number |
| DataSize | Uint64 |  | Approximate partition size in bytes |
| RowCount | Uint64 |  | Approximate number of rows |
| IndexSize | Uint64 |  | Partition index size in a tablet |
| CPUCores | Double |  | Instant value of load per partition (CPU share) |
| TabletId | Uint64 |  | ID of the tablet serving the partition |
| Path | Utf8 |  | Full table path |
| NodeId | Uint32 |  | ID of the node that the partition is being served on |
| StartTime | Timestamp |  | Last time when the tablet serving the partition was launched |
| AccessTime | Timestamp |  | Last time when data from the partition was read |
| UpdateTime | Timestamp |  | Last time when data was written to the partition |
| RowReads | Uint64 |  | Number of point reads since the start of the partition tablet |
| RowUpdates | Uint64 |  | Number of rows written since the start |
| RowDeletes | Uint64 |  | Number of rows deleted since the start |
| RangeReads | Uint64 |  | Number of row ranges read since the start |
| RangeReadRows | Uint64 |  | Number of rows read in the ranges since the start |
| InFlightTxCount | Uint64 |  | Number of in-flight transactions |
| ImmediateTxCompleted | Uint64 |  | Number of one-shard transactions completed since the start |
| CoordinatedTxCompleted | Uint64 |  | Number of coordinated transactions completed since the start |
| TxRejectedByOverload | Uint64 |  | Number of transactions rejected due to overload (since the start) |
| TxRejectedByOutOfStorage | Uint64 |  | Number of transactions rejected due to lack of storage space (since the start) |

Restrictions:

* Cumulative fields (RowReads, RowUpdates, and so on) store the accumulated values since the last start of the tablet serving the partition

