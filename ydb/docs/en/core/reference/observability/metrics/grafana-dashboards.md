# Grafana dashboards for {{ ydb-short-name }}

This page describes Grafana dashboards for {{ ydb-short-name }}.

Instructions on how to install and configure dashboards are provided in the [YDB cluster monitoring setup](../../../devops/observability/monitoring.md#prometheus-grafana) section.

## DB status {#dbstatus}

General database dashboard.

Download the **DB status** dashboard template: [dbstatus.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/dbstatus.json).

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

Download the **DB overview** dashboard template: [dboverview.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/dboverview.json).

## YDB Essential Metrics {#ydbessentials}

Dashboard for monitoring key database metrics.

### Health section {#ydbessentials-health}

This section contains charts showing the status of cluster and database components.

| Name | Description |
| --- | --- |
| Nodes count | Number of running YDB nodes, pcs. |
| Nodes uptime | Uptime of each node since startup; helps detect restarts and unstable nodes, in seconds. |
| VDisks count | Number of available VDisks in the cluster, pcs. |

### Saturation section {#ydbessentials-saturation}

This section contains charts reflecting database resource utilization.

| Name | Description |
| --- | --- |
| CPU by thread pool (dynnodes) | CPU consumption by dynamic nodes per [execution pool](../../../devops/configuration-management/configuration-v2/config-settings.md#tuneconfig), in CPU cores. |
| CPU utilization (dynnodes) | CPU utilization by dynamic nodes per [execution pool](../../../devops/configuration-management/configuration-v2/config-settings.md#tuneconfig), in %. |
| Elapsed Time vs CPU Time | Ratio of real operation execution time (`ElapsedMicrosec`) to CPU time (`CpuMicrosec`) by node. A sustained excess above 100% means sessions are spending time waiting rather than actively working: typically, this is I/O wait or CPU overcommit on the hypervisor side. |
| RSS size by node | Amount of RAM (Resident set size) consumed by each dynamic node, showing cgroup memory limits, in bytes. |
| Storage usage | Logical database size and its configured limit, in bytes. |
| Overloaded shard count | Number of [overloaded DataShards](../../../troubleshooting/performance/schemas/overloaded-shards.md) by CPU load ranges — from 50% to 100%, pcs. |

### Traffic section {#ydbessentials-traffic}

This section contains charts characterizing database load.

| Name | Description |
| --- | --- |
| Queries per second by latency buckets | Number of queries per second broken down by latency ranges (from 1 ms to +∞). Each range is highlighted with a separate color — from green for fast queries to purple for slow ones. Allows you to estimate latency distribution and overall RPS, in qps. |
| Transactions per second by latency buckets | Number of transactions per second broken down by latency ranges (from 1 ms to +∞). Each range is highlighted with a separate color — from green for fast transactions to purple for slow ones. Allows you to estimate latency distribution and overall TPS, in tps. |
| Rows read, uploaded, updated, deleted | Number of table row operations per second: read, create, update, and delete, in ops/s. |
| Session count by dynnode | Number of active sessions on each dynamic node, pcs. |

### Latency section {#ydbessentials-latency}

This section contains charts showing query and transaction execution times.

| Name | Description |
| --- | --- |
| Query latency percentiles (ms) | Database query execution time at percentiles p50, p90, p95, p99, in milliseconds. |
| Transaction latency percentiles (ms) | Database transaction execution time at percentiles p50, p90, p95, p99, in milliseconds. |

### Errors section {#ydbessentials-errors}

This section contains charts showing the number of errors occurring.

| Name | Description |
| --- | --- |
| YQL Issues per second | Number of YQL query execution errors by error type, in errors/s. |
| gRPC response errors per second | Number of gRPC responses with errors broken down by status, in errors/s. |

Download the **YDB Essential Metrics** dashboard template: [ydb-essentials.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/ydb-essentials.json).

## Actors {#actors}

CPU consumption in the actor system.

| Name | Description |
| --- | --- |
| CPU by execution pool (us) | CPU consumption in various execution pools on all nodes, microseconds per second (one million corresponds to one core consumption). |
| Actor count | Number of actors (by actor type). |
| CPU | CPU consumption in various execution pools (by actor type). |
| Events | Event processing metrics in the actor system. |

Download the **Actors** dashboard template: [actors.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/actors.json).

## CPU {#cpu}

CPU consumption in [execution pools](../../../devops/configuration-management/configuration-v2/config-settings.md#tuneconfig).

| Name | Description |
| --- | --- |
| CPU by execution pool | CPU consumption in various execution pools on all nodes, microseconds per second (one million corresponds to consumption of one core) |
| Actor count | Number of actors (by actor type) |
| CPU | CPU consumption in various execution pools |
| Events | Event processing metrics in various execution pools |

Download the **CPU** dashboard template: [cpu.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/cpu.json).

## gRPC {#grpc}

gRPC layer metrics.

| Name | Description |
| --- | --- |
| Requests | Number of requests received by the database per second (by gRPC method type) |
| Request bytes | Size of requests received by the database, bytes per second (by gRPC method type) |
| Response bytes | Size of responses sent by the database, bytes per second (by gRPC method type) |
| Dropped requests | Number of requests per second whose processing was terminated at the transport layer due to an error (by gRPC method type) |
| Dropped responses | Number of responses per second whose sending was terminated at the transport layer due to an error (by gRPC method type) |
| Requests in flight | Number of requests being processed simultaneously by the database (by gRPC method type) |
| Request bytes in flight | Size of requests being processed simultaneously by the database (by gRPC method type) |

Download the **gRPC** dashboard template: [grpc.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/grpc.json).

## Query engine {#queryengine}

Information about the query execution engine.

| Name | Description |
| --- | --- |
| Requests | Number of incoming requests per second (by query type) |
| Request bytes | Size of incoming requests, bytes per second (`query, parameters, total`) |
| Responses | Number of responses per second (by response type) |
| Response bytes | Response sizes, bytes per second (`total, query result`) |
| Sessions | Information about established sessions |
| Latencies | Histograms of query execution times for various query types |

Download the **Query engine** dashboard template: [queryengine.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/queryengine.json).

## TxProxy {#txproxy}

Information about transactions from the `DataShard transaction proxy` level.

| Name | Description |
| --- | --- |
| Transactions | Metrics of datashard transactions |
| Latencies | Histograms of execution times of various stages of datashard transactions |

Download the **TxProxy** dashboard template: [txproxy.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/txproxy.json).

## DataShard {#datashard}

Metrics of the `DataShard` tablet.

| Name | Description |
| --- | --- |
| Operations | Statistics of operations with the datashard for different operation types |
| Transactions | Information about transactions of the datashard tablet (by transaction types) |
| Latencies | Histograms of execution times of various stages of user transactions |
| Tablet latencies | Histograms of execution times of tablet transactions |
| Compactions | Information about performed LSM compaction operations |
| ReadSets | Information about transferred ReadSets during execution of a user transaction |
| Other | Other metrics |

Download the **DataShard** dashboard template: [datashard.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/datashard.json).

## Database Hive {#database-hive-detailed}

Metrics of the [Hive](../../../contributor/hive.md) tablet of the selected database.

The dashboard contains the following filters:

- `database` — used to select the database whose metrics should be displayed;
- `ds` — used to select the Prometheus source whose data should be displayed on the dashboard;
- `Tx type` — determines the transaction type for which graphs will be displayed on the "`{Tx type}` `average time`" panel.

| Name | Description |
| --- | --- |
| CPU usage by HIVE_ACTOR, HIVE_BALANCER_ACTOR | CPU time consumed by `HIVE_ACTOR` and `HIVE_BALANCER_ACTOR` — the two most important Hive actors. |
| Self-ping time | Response time of the Hive tablet to its own requests. High values indicate heavy load (and slow responsiveness) of Hive. |
| Local transaction times | CPU time consumed for executing various types of local transactions in Hive. Displays the load structure on Hive. |
| Tablet count | Total number of tablets in the database. |
| Event queue size | Size of the incoming event queue. Consistently high values indicate that Hive is not keeping up with processing events at the required speed. |
| {Tx type} average time | Average execution time of one local transaction of the type selected in the `Tx type` filter. |
| Versions | Versions of {{ ydb-short-name }} running on cluster nodes. |
| Hive node | Node on which Hive is running. |

Download the **Database Hive** dashboard template: [database-hive-detailed.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/database-hive-detailed.json).

## Topic {#topic}

The dashboard displays graphs for metrics of a single topic. The topic name is set in the `topic` filter at the top of the dashboard. Below are the panels and metric descriptions.

| Name | Description |
| --- | --- |
| Total incoming records (bytes) per second | Number of bytes per second written to the topic using the `Ydb::TopicService::StreamWrite` method |
| Total incoming records (count) per second | Number of messages per second written using the `Ydb::TopicService::StreamWrite` method |
| Write latency | Write latency: time from message creation to its write to the topic. Percentage of messages for which write latency falls within intervals <100 ms, <200 ms, etc. |
| Partition throttling | Write throttling duration - waiting for available write quota. Percentage of messages for which write throttling duration falls within intervals <1 ms, <5 ms, etc. |
| Partition quota usage | Utilization of topic partition write quotas, % |
| Write sessions active | Number of open write sessions to the topic |
| Write sessions created | Number of write sessions created per second to the topic |

Download the **Topic** dashboard template: [topic.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/topic.json).

## Topic — Consumer {#topic-consumer}

The dashboard displays graphs for the metrics of a single topic and its associated consumer. The topic is selected in the `topic` filter, and the consumer in the `consumer` filter. The panels and metric descriptions are listed below.

| Name | Description |
| --- | --- |
| Total incoming records (bytes) per second | Number of bytes per second written to the topic using the `Ydb::TopicService::StreamWrite` method |
| Total outgoing records (bytes) per second | Number of bytes per second read from the topic by the consumer using the `Ydb::TopicService::StreamRead` method |
| Total incoming records (count) per second | Number of messages per second written to the topic using the `Ydb::TopicService::StreamWrite` method |
| Total outgoing records (count) per second | Number of messages per second read from the topic by the consumer using the `Ydb::TopicService::StreamRead` method |
| End-to-end latency | End-to-end latency: time from message creation to its read. Percentage of messages for which end-to-end latency falls within intervals <100 ms, <200 ms, etc. |
| Read latency max | Maximum (across all partitions) difference between the current time and the write time of the last message in the topic, ms |
| Unread messages max | Maximum (across all partitions) difference between the last offset in the partition and the last read offset, in messages |
| Read idle time max | Maximum idle time (how long the consumer has not read from the partition) across all topic partitions, ms |
| Uncommitted messages max | Maximum (across all partitions) difference between the last offset in the partition and the last committed offset, in messages |
| Committed read lag max | Maximum (across all partitions) difference between the current time and the write time of the last committed message in the topic, ms |
| Partition sessions started | Number of topic read sessions started by the consumer per second |

Download the **Topic — Consumer** dashboard template: [topic-consumer.json](https://raw.githubusercontent.com/ydb-platform/ydb/refs/heads/main/ydb/deploy/helm/ydb-prometheus/dashboards/topic-consumer.json).
