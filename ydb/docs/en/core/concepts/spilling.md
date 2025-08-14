# Spilling

## Spilling in General

**Spilling** is a memory management mechanism that temporarily offloads intermediate data arising from computations and exceeding available node RAM capacity to external storage. In {{ ydb-short-name }}, disk storage is currently used for spilling. Spilling enables execution of user queries that require processing large data volumes exceeding available node memory.

In data processing systems, including {{ ydb-short-name }}, spilling is essential for:

- processing queries with large data volumes when intermediate results don't fit in RAM
- executing complex analytical operations (aggregations, table joins) over large datasets
- optimizing query performance through intermediate materialization of part of the data in external memory, which in certain scenarios can accelerate overall execution time


Spilling operates based on the memory hierarchy principle:

1. **Random Access Memory (RAM)** — fast but limited.
2. **External storage** — slower but more capacious.

When memory usage approaches the limit, the system:

- serializes part of the data
- saves it to external storage
- frees the corresponding memory
- continues processing the query using data remaining in memory
- loads data back into memory, when necessary, to continue computations


## Spilling in {{ ydb-short-name }}

{{ ydb-short-name }} implements the spilling mechanism through the **Spilling Service**, an [actor service](glossary.md#actor-service) that provides temporary storage for data blobs. Spilling is only performed on [database nodes](glossary.md#database-node). Detailed technical information about it is available in [{#T}](../contributor/spilling-service.md).

### Types of Spilling in {{ ydb-short-name }}

{{ ydb-short-name }} implements two primary types of spilling that operate at different levels of the computational process:

* [Compute Node Spilling](#compute-node-spilling)
* [Channel Spilling](#channel-spilling)

These types work independently and can activate simultaneously within a single query, providing comprehensive memory management.

#### Compute Node Spilling {#compute-node-spilling}

{{ ydb-short-name }} compute cores automatically offload intermediate data to disk when executing operations that require significant memory. This type of spilling is implemented at the level of individual computational operations and activates when memory limits are reached.

Main usage scenarios:

* **Aggregations** — when grouping large data volumes, the system offloads intermediate hash tables to disk.
* **Join operations** — when joining large tables, the [Grace Hash Join](https://en.wikipedia.org/wiki/Hash_join#Grace_hash_join) algorithm is used with data partitioning and offloading to disk.

##### Operation Mechanism

Compute nodes contain specialized objects for monitoring memory usage. When the data volume approaches the set limit:

1. The system switches to spilling mode.
2. Data is serialized and divided into blocks (buckets).
3. Part of the blocks is transferred to the Spilling Service for disk storage.
4. Metadata about the data location is kept in memory.
5. The system continues processing the data remaining in memory, which frees additional space.
6. When necessary, data is loaded back and processed.


#### Channel Spilling {#channel-spilling}

This type of spilling operates at the level of data transfer between different query execution stages. Data transfer channels automatically buffer and offload data when buffers overflow. This helps avoid blocking the execution of the data-generating node, even when one of the receiving nodes is not ready to accept data.

##### Operation Mechanism

Data transfer channels continuously monitor their state:

1. **Buffering**: Incoming data accumulates in the channel’s internal buffers  
2. **Fill control**: The system tracks buffer fill levels  
3. **Automatic spilling**: When limits are reached, data is automatically serialized and transferred to the Spilling Service  
4. **Continued operation**: The channel continues accepting new data after freeing memory space  
5. **Recovery**: When the next stage is ready, data is read from external storage and passed further  

## Interaction with Memory Controller

When executing queries, {{ ydb-short-name }} tries to stay within the memory limit set by the [memory controller](../../reference/configuration/index.md#memory-controller). To continue fitting within this limit even as intermediate computations grow, spilling is used. For more details, see the [Memory Management section](../reference/configuration/table_service_config.md#memory-management).

## See Also

- [Spilling Service](../contributor/spilling-service.md)
- [Spilling configuration](../reference/configuration/table_service_config.md)
- [{{ ydb-short-name }} monitoring](../devops/observability/monitoring.md)
- [Performance diagnostics](../troubleshooting/performance/index.md)