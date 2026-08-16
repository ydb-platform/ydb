# Spilling

## Spilling in general

**Spilling** is a memory management mechanism in which intermediate data generated during query execution that exceeds the available RAM of a node is temporarily offloaded to external storage. In {{ ydb-short-name }}, disk is currently used for spilling. Spilling ensures the execution of user queries that require processing large volumes of data exceeding the available node memory.

In data processing systems, including {{ ydb-short-name }}, spilling is important for:

- Processing queries with large volumes of data when intermediate results do not fit into memory.
- Executing complex analytical operations (aggregations, table joins) on large datasets
- Query performance optimization by intermediate materialization of some data in external memory, which in certain scenarios can speed up the overall execution time.

Spilling operates based on the principle of memory hierarchy:

1. **RAM (Random Access Memory)** is a fast but limited resource.
2. **External memory** — slower, but more capacious.

When memory usage approaches the limit, the system:

- Serializes part of the data.
- Saves them in external memory.
- frees the corresponding memory
- continues processing the query, using data in memory.
- if necessary, loads data back into memory to continue computations.

## Spilling in {{ ydb-short-name }} {#architecture}

{{ ydb-short-name }} implements the spilling mechanism via **Spilling Service** — an [actor service](../glossary.md#actor-service) that provides temporary storage for data blobs. Spilling is performed only on [database nodes](../glossary.md#database-node). Detailed technical information about Spilling Service is available in the [Spilling Service](../../contributor/spilling-service.md) section.

### Types of spilling in {{ ydb-short-name }}

{{ ydb-short-name }} implements two main types of spilling, operating at different levels of the computing process:

* [Spilling in computations](#computation-spilling)
* [Spilling in transport](#transport-spilling)

These types operate independently of each other and can be activated simultaneously within a single query, providing comprehensive memory management.

#### Spilling in computations {#computation-spilling}

Compute cores {{ ydb-short-name }} automatically spill intermediate data to disk when executing operations that require significant memory. This type of spilling is implemented at the level of individual compute operations and is activated when memory limits are reached.

**Main use cases:**

* **Aggregations** — when grouping large volumes of data, the system offloads intermediate hash tables to disk
* **Join operations** — when joining large tables, the Grace Hash Join algorithm partitions data and spills it to disk.

##### How it works

Compute nodes contain specialized objects for monitoring memory usage. When the data volume approaches the set limit:

1. The system switches to spilling mode
2. The data is serialized and split into blocks (buckets)
3. Some blocks are transferred to the Spilling Service for saving to disk
4. Metadata about data location is stored in memory
5. The system continues processing the data remaining in memory, which allows freeing up additional space
6. If necessary, the data is loaded back and processed

#### Spilling in transport {#transport-spilling}

This type of spilling operates at the data transfer level between different query execution stages. The data transfer subsystem automatically buffers and spills data when buffers overflow. This helps avoid blocking operations that generate data, even in situations where the receiving side is temporarily unable to receive data.

##### How it works

The data transfer subsystem constantly monitors its state:

1. **Buffering**: Incoming data accumulates in the internal buffers of the subsystem
2. **Fill control**: The system monitors the fill level of buffers
3. **Automatic spilling**: When limits are reached, data is automatically serialized and transferred to the Spilling Service
4. **Continued operation**: The subsystem continues to accept new data after freeing up memory space
5. **Recovery**: When the next stage is ready, data is read from external storage and passed on.

## Interaction with the memory controller

When executing queries, {{ ydb-short-name }} tries to stay within the specified memory limit set by the [memory controller](../../reference/configuration/memory_controller_config.md). To continue fitting within this limit even as intermediate computations grow, spilling is used.

At the Resource Manager level, the threshold for transitioning to spilling is set by the [`resource_manager.spilling_percent`](../../reference/configuration/table_service_config.md#spilling-percent) parameter: when the Query Processor memory pool on a node is filled to the specified percentage, compute operations start preferring offloading data to disk over further growth of RAM consumption. The pool size is set by the [`query_execution_limit_percent` / `query_execution_limit_bytes`](../../reference/configuration/memory_controller_config.md#query-execution-limit) parameters.

For more details on spilling settings and related memory limits, see the [Memory management](../../reference/configuration/table_service_config.md#memory-management) and [Query execution memory limits](../../reference/configuration/table_service_config.md#query-execution-memory-limits) sections.

## See also

- [Spilling Service](../../contributor/spilling-service.md)
- [Spilling configuration](../../reference/configuration/table_service_config.md)
- [Monitoring {{ ydb-short-name }}](../../devops/observability/monitoring.md)
- [Performance diagnostics](../../troubleshooting/performance/index.md)
