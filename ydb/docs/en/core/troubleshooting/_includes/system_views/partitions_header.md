## Partitions {#partitions}

The following system view stores detailed information about individual [partitions](../../../concepts/datamodel/table.md#partitioning) of all DB tables:

* `partition_stats`: Contains information about instant metrics and cumulative operation counters. Instant metrics are, for example, CPU load or count of in-flight [transactions](../../../concepts/transactions.md). Cumulative counters, for example, count the total number of rows read.

The system view is designed to detect various irregularities in the load on a table partition or show the size of table partition data.

Cumulative fields (`RowReads`, `RowUpdates`, and so on) store the accumulated values since the last start of the tablet serving the partition.

Table structure:

| Field | Description |
--- | ---
| `OwnerId` | ID of the SchemeShard serving the table.<br>Type: `Uint64`.<br>Key: `0`. |
| `PathId` | ID of the SchemeShard path.<br>Type: `Uint64`.<br>Key: `1`. |
| `PartIdx` | Partition sequence number.<br>Type: `Uint64`.<br>Key: `2`. |
| `DataSize` | Approximate partition size in bytes.<br>Type: `Uint64`. |
| `RowCount` | Approximate number of rows.<br>Type: `Uint64`. |
| `IndexSize` | Partition index size in a tablet.<br>Type: `Uint64`. |
| `CPUCores` | Double Instant value of load per partition (CPU share) |
| `TabletId` | ID of the tablet serving the partition.<br>Type: `Uint64`. |
| `Path` | Full path to the table.<br>Type: `Utf8`. |
| `NodeId` | ID of the node that the partition is being served on.<br>Type: `Uint32`. |
| `StartTime` | Last time when the tablet serving the partition was started.<br>Type: `Timestamp`. |
| `AccessTime` | Last time when data from the partition was read.<br>Type: `Timestamp`. |
| `UpdateTime` | Last time when data was written to the partition.<br>Type: `Timestamp`. |
| `RowReads` | Number of point reads since the start of the partition tablet.<br>Type: `Uint64`. |
| `RowUpdates` | Number of rows written since the start.<br>Type: `Uint64`. |
| `RowDeletes` | Number of rows deleted since the start.<br>Type: `Uint64`. |
| `RangeReads` | Number of row range reads since the start.<br>Type: `Uint64`. |
| `RangeReadRows` | Number of rows read in the ranges since the start.<br>Type: `Uint64`. |
| `InFlightTxCount` | Number of in-flight transactions.<br>Type: `Uint64`. |
| `ImmediateTxCompleted` | Number of one-shard transactions completed since the start.<br>Type: `Uint64`. |
| `CoordinatedTxCompleted` | Number of coordinated transactions completed since the start.<br>Type: `Uint64`. |
| `TxRejectedByOverload` | Number of transactions rejected due to overload (since the start).<br>Type: `Uint64`. |
| `TxRejectedByOutOfStorage` | Number of transactions rejected due to lack of storage space (since the start).<br>Type: `Uint64`. |
