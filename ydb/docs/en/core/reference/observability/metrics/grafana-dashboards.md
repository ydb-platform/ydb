# Grafana dashboards for {{ ydb-short-name }}

This page describes Grafana dashboards for {{ ydb-short-name }}. For information about how to install dashboards, see [{#T}](../../../devops/observability/monitoring.md#prometheus-grafana).

## DB status {#dbstatus}

General database dashboard.

Download the [dbstatus.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/dbstatus.json) file with the **DB status** dashboard.


## DB overview {#dboverview}

General database dashboard by categories:

- Health
- API
- API details
- CPU
- CPU pools
- Memory
- Storage
- DataShard
- DataShard details
- Latency

Download the [dboverview.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/dboverview.json) file with the **DB overview** dashboard.

## Actors {#actors}

CPU utilization in an actor system.

| Name | Description |
|---|---|
| CPU by execution pool (us) | CPU utilization in different execution pools across all nodes, microseconds per second (one million indicates utilization of a single core) |
| Actor count | Number of actors (by actor type) |
| CPU | CPU utilization in different execution pools (by actor type) |
| Events | Actor system event handling metrics |

Download the [actors.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/actors.json) file with the **Actors** dashboard.

## CPU {#cpu}

CPU utilization in execution pools.

| Name | Description |
|---|---|
| CPU by execution pool | CPU utilization in different execution pools across all nodes, microseconds per second (one million indicates utilization of a single core) |
| Actor count | Number of actors (by actor type) |
| CPU | CPU utilization in each execution pool |
| Events | Event handling metrics in each execution pool |

Download the [cpu.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/cpu.json) file with the **CPU** dashboard.

## gRPC {#grpc}

gRPC layer metrics.

| Name | Description |
|---|---|
| Requests | Number of requests received by a database per second (by gRPC method type) |
| Request bytes | Size of database requests, bytes per second (by gRPC method type) |
| Response bytes | Size of database responses, bytes per second (by gRPC method type) |
| Dropped requests | Number of requests per second with processing terminated at the transport layer due to an error (by gRPC method type) |
| Dropped responses | Number of responses per second with sending terminated at the transport layer due to an error (by gRPC method type) |
| Requests in flight | Number of requests that a database is simultaneously handling (by gRPC method type) |
| Request bytes in flight | Size of requests that a database is simultaneously handling (by gRPC method type) |

Download the [grpc.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/grpc.json) file with the **gRPC API** dashboard.

## Query engine {#queryengine}

Information about the query engine.

| Name | Description |
|---|---|
| Requests | Number of incoming requests per second (by request type) |
| Request bytes | Size of incoming requests, bytes per second (query, parameters, total) |
| Responses | Number of responses per second (by response type) |
| Response bytes | Response size, bytes per second (total, query result) |
| Sessions | Information about running sessions |
| Latencies | Request execution time histograms for different types of requests |

Download the [queryengine.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/queryengine.json) file with the **Query engine** dashboard.

## TxProxy {#txproxy}

Information about transactions from the DataShard transaction proxy layer.

| Name | Description |
|---|---|
| Transactions | Datashard transaction metrics |
| Latencies | Execution time histograms for different stages of datashard transactions |

Download the [txproxy.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/txproxy.json) file with the **TxProxy** dashboard.

## DataShard {#datashard}

DataShard tablet metrics.

| Name | Description |
|---|---|
| Operations | Datashard operation statistics for different types of operations |
| Transactions | Information about datashard tablet transactions (by transaction type) |
| Latencies | Execution time histograms for different stages of custom transactions |
| Tablet latencies | Tablet transaction execution time histograms |
| Compactions | Information about LSM compaction operations performed |
| ReadSets | Information about ReadSets that are sent when executing a customer transaction |
| Other | Other metrics |

Download the [datashard.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/datashard.json) file with the **DataShard** dashboard.

## Database Hive {#database-hive-detailed}

[Hive](../../../contributor/hive.md) metrics for the selected database.

The dashboard includes the following filters:

* database – selects the database for which metrics are displayed;
* ds – selects the Prometheus data source the dashboard will use;
* Tx type – determines the transaction type for which "`{Tx type}` average time" panel is displayed.

| Name | Description |
|---|---|
| CPU usage by HIVE_ACTOR, HIVE_BALANCER_ACTOR | CPU time utilized by `HIVE_ACTOR` and `HIVE_BALANCER_ACTOR`, two of the most important actors of the Hive tablet. |
| Self-ping time | Time it takes Hive to respond to itself. High values indicate heavy load (and low responsiveness) of the Hive. |
| Local transaction times | CPU time utilized by various local transaction types in Hive. Shows the structure of Hive load based on different activities. |
| Tablet count | Total number of tablets in the database. |
| Event queue size | Size of the incoming event queue in Hive. Consistently high values indicate Hive cannot process events fast enough. |
| `{Tx type}` average time | Average execution time of a single local transaction of the type specified in the `Tx type` selector on the dashboard. |
| Versions | Versions of {{ ydb-short-name }} running on cluster nodes. |
| Hive node | Node where the database Hive is running. |

Download the [database-hive-detailed.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/database-hive-detailed.json) file with the **Database Hive** dashboard.
