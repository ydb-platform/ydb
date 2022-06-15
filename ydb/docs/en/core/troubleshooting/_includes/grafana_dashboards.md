## {{ ydb-full-name }} {#ydb}

### DB status {#dbstatus}

Database overview.

### Actors {#actors}

CPU consumption in actor system.

| **Name** | **Description** |
|---|---|
| CPU by execution pool (us) | CPU consumption in different execution pools on all nodes, measured in microseconds per second (one million is one core) |
| Actor count | Number of actors (by actor type) |
| CPU | CPU consumption in different execution pools (by actor type) |
| Events | Metrics of event processing in actor system |

### gRPC {#grpc}

gRPC level metrics.

| **Name** | **Description** |
|---|---|
| Requests | Number of requests received by the database per second (by gRPC method) |
| Request bytes | Size of requests received by the database in bytes per second (by gRPC method) |
| Response bytes | Size of responses sent by the database in bytes per second (by gRPC method) |
| Dropped requests | Number of requests dropped at the transport layer due to an error (by gRPC method) |
| Dropped responses | Number of responses dropped at the transport layer due to an error (by gRPC method) |
| Requests in flight | Number of requests that a database is simultaneously handling (by gRPC method) |
| Request bytes in flight | Size of requests that a database is simultaneously handling (by gRPC method) |

### Query engine {#queryengine}

Query engine processing details.

| **Name** | **Description** |
|---|---|
| Requests | Number of incoming requests per second (by request type) |
| Request bytes | Size of incoming requests in bytes per second (query, parameters, total) |
| Responses | Number of responses per second (by response type) |
| Response bytes | Size of responses in bytes per second (total, query result) |
| Sessions | Details about established sessions |
| Latencies | Histogram latencies of requests for different request types |

### TxProxy {#txproxy}

Details from DataShard transaction proxy level.

| **Name** | **Description** |
|---|---|
| Transactions | Metrics of DataShard transactions |
| Latencies | Histogram latencies of DataShard transaction phases |

### DataShard {#datashard}

DataShard tablet metrics.

| **Name** | **Description** |
|---|---|
| Operations | Statistics of DataShard operations for different operation types |
| Transactions | Information about DataShard transactions (by transaction type) |
| Latencies | Histograms of execution times of different phases in user transaction processing |
| Tablet latencies | Histograms of execution times of generic tablet transactions |
| Compactions | Details of LSM compaction operations |
| ReadSets | Details of ReadSets sent during user transaction execution |
| Other | Other metrics |

