# table_service_config

The `table_service_config` section contains configuration parameters for the table service, including spilling settings.

## resource_manager {#resource-manager}

The `resource_manager` subsection specifies resource management parameters for the table service.

### Vector index level table cache

The vector index level table (`indexImplLevelTable`) [`vector_kmeans_tree`](../../dev/vector-indexes-kmeans-tree-type.md#index-structure) stores centroids that {{ ydb-short-name }} accesses at each step of descending the cluster tree during vector search. To avoid re-reading these centroids from the distributed storage on every query, {{ ydb-short-name }} can cache them in memory on each database node.

The cache is created separately on each node, uses LRU eviction, and gradually grows its size up to the specified limit.


```yaml
table_service_config:
  resource_manager:
    kqp_level_cache_max_size_bytes: 0
    kqp_level_cache_increase_batch_size_bytes: 33554432
```


#### resource_manager.kqp_level_cache_max_size_bytes

**Type:** `uint64`  
**Default:** `0` (cache disabled)  
**Description:** Maximum size of the vector index level table cache in bytes per database node. Setting a non-zero value enables the cache.

#### resource_manager.kqp_level_cache_increase_batch_size_bytes

**Type:** `uint64`  
**Default:** `33554432` (32 MiB)  
**Description:** Step size for increasing the level table cache. The cache grows in chunks of this size up to the value of `kqp_level_cache_max_size_bytes` and decreases by the same chunks when the limit is lowered.

### Query execution memory limits {#query-execution-memory-limits}

The `resource_manager` subsection sets the spilling threshold relative to the query memory pool on the [node](../../concepts/glossary.md#node). The size of this pool is controlled by the [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit) parameters in `memory_controller_config`.


```yaml
table_service_config:
  resource_manager:
    spilling_percent: 80
```


#### resource_manager.spilling_percent {#spilling-percent}

**Type:** `double`
**Default:** `80`
**Description:** The fill threshold of the query memory pool at which {{ ydb-short-name }} starts considering [spilling](../../concepts/query_execution/spilling.md) the preferred memory management method. The basis for calculation is the Query Processor pool, whose size is set by [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit).

When the total memory consumption by queries on a node exceeds `spilling_percent` percent of the available pool, computational operations that support spilling (Grace Hash Join, aggregations, etc.) receive a signal to offload intermediate data to disk instead of further increasing RAM consumption.

For example, with the default value of `80`, spilling is activated when the query pool is about 80% full.

The threshold applies:

- To the total query memory pool on the node (size — see [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit)).
- To the [resource pool](../../concepts/glossary.md#resource-pool) if the query is executed in a workload pool with the `total_memory_limit_percent_per_node` limit.

{% note info %}

`spilling_percent` does not set a limit on the size of spilling files on disk. Disk quotas are handled by [`local_file_config.max_total_size`](#local-file-config-max-total-size) in the `spilling_service_config` section.

{% endnote %}

##### Interaction with other parameters

| Parameter | Level | Role |
| --- | --- | --- |
| `query_execution_limit_percent` / `query_execution_limit_bytes` | Node (QP) | Query memory pool size |
| `spilling_percent` | Query / pool | Pool fill threshold after which spilling is preferred |
| `activities_limit_percent` | Node | Total memory limit for all activity components (QP, compaction, etc.) |

`spilling_percent` determines when the query pool on a node is so full that further growth in RAM should give way to spilling. `activities_limit_percent` limits the memory of activities overall and indirectly affects the available RAM, but does not replace the pool relative to which `spilling_percent` is calculated.

##### Recommendations

- Decrease `spilling_percent` (for example, to `70`) if you need to offload heavy queries to disk earlier and reduce the risk of exhausting the memory pool.
- Increase `spilling_percent` (for example, to `90`) if disk spilling too often degrades performance and the node has enough RAM.
- Coordinate `spilling_percent` with [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit): when increasing the query pool limit, you can raise the spilling threshold if the node has enough RAM.

## spilling_service_config

[Spilling](../../concepts/query_execution/spilling.md) is a memory management mechanism in {{ ydb-short-name }} that temporarily saves data to disk when RAM is insufficient.

### Enabling {#enable}

Spilling is enabled by default. The following parameter controls enabling and disabling the spilling service.

#### local_file_config.enable {#local-file-config-enable}

**Location:** `table_service_config.spilling_service_config.local_file_config.enable`
**Type:** `boolean`
**Default:** `true`
**Description:** Enables or disables the spilling service. When disabled (`false`), [spilling](../../concepts/query_execution/spilling.md) does not function, which may lead to errors when processing large amounts of data.

##### Possible errors

- `Spilling Service not started` / `Service not started` — attempt to use spilling when the Spilling Service is disabled. See [{#T}](../../troubleshooting/spilling/service-not-started.md)


```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
```


### Main configuration parameters


```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 21474836480
```


### Directory configuration

#### local_file_config.root

**Type:** `string`  \n**Default:** `""` (temporary directory)  \n**Description:** File directory for storing spilling files.

For each `ydbd` process, a separate directory with a unique name is created. Spilling directories have the following name format:

`node_<node_id>_<spilling_service_id>`

Where:

- `node_id`: [node](../../concepts/glossary.md#node) identifier
- `spilling_service_id`: unique instance identifier created once during [Spilling Service](../../contributor/spilling-service.md) initialization when the ydbd process starts

Spilling files are stored inside each such directory.

Example of a full path to a spilling directory:


```bash
/tmp/spilling-tmp-<username>/node_1_32860791-037c-42b4-b201-82a0a337ac80
```


Where:

- `/tmp`: value of the `root` parameter
- `<username>`: username under which the `ydbd` process runs

**Important notes:**

- When the process starts, all existing spilling directories in the specified directory are automatically deleted. Spilling directories have a special name format that includes an instance identifier generated once when the ydbd process starts. When a new process starts, all directories in the spilling directory that match the name format but have a different `spilling_service_id` from the current one are deleted.
- The directory must have sufficient read and write permissions for the user under which ydbd runs.

{% note info %}

Spilling is performed only on [database nodes](../../concepts/glossary.md#database-node).

{% endnote %}

##### Possible errors

- `Permission denied`: insufficient directory access permissions. See [{#T}](../../troubleshooting/spilling/permission-denied.md)

#### local_file_config.max_total_size {#local-file-config-max-total-size}

**Type:** `uint64` \n**Default:** `21474836480` (20 GiB) \n**Description:** Maximum total size of all spilling files on each [node](../../concepts/glossary.md#node). If the limit is exceeded, spilling operations fail with an error. The total spilling limit across the entire cluster equals the sum of `max_total_size` values from all nodes.

##### Recommendations

- Set the value based on available disk space

##### Possible errors

- `Total size limit exceeded: X/YMb`: maximum total size of spilling files exceeded. See [{#T}](../../troubleshooting/spilling/total-size-limit-exceeded.md)

### Memory management {#memory-management}

#### Relationship with memory_controller_config

Spilling activation is closely related to the memory controller settings. Detailed configuration of `memory_controller_config` is described in a [separate article](memory_controller_config.md).

The key parameter for spilling is **`activities_limit_percent`**, which determines the amount of memory allocated for query processing activities. This parameter affects the memory available for user queries and, consequently, the frequency of spilling activation.

#### Spilling threshold in resource_manager

The actual threshold at which computational operations switch to spilling is controlled by the [`resource_manager.spilling_percent`](#spilling-percent) parameter. It determines at what fill level of the Query Processor memory pool the intermediate query data starts being offloaded to disk. The pool size is set in [`memory_controller_config`](memory_controller_config.md#query-execution-limit). For more details, see the [Query execution memory limits](#query-execution-memory-limits) section.

#### Impact on spilling

- When `activities_limit_percent` increases, more memory is available for queries → spilling is activated less often
- When `activities_limit_percent` decreases, less memory is available for queries → spilling is activated more often
- When `spilling_percent` decreases, spilling is triggered at a lower fill level of the query pool
- When `spilling_percent` increases, tasks take longer to increase RAM consumption before switching to spilling

{% note warning %}

It is important to note that spilling itself also requires memory. If `activities_limit_percent` is set too high, memory may still run out despite spilling, because the spilling mechanism itself consumes memory resources.

{% endnote %}

### File system requirements

#### File descriptors

{% note info %}

For information on configuring file descriptor limits during initial deployment, see the [File descriptor limits](../../devops/deployment-options/manual/initial-deployment/deployment-preparation.md#file-descriptors) section.

{% endnote %}

### Configuration examples

#### High-load system

For maximum performance in high-load systems, it is recommended to increase the spilling size:


```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 107374182400   # 100 GiB
```


#### Limited resources

For systems with limited resources, it is recommended to use conservative settings:


```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 5368709120     # 5 GiB
```


### Full example


```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/spilling"
      max_total_size: 53687091200    # 50 GiB
```


## See also

- [Spilling concept](../../concepts/query_execution/spilling.md)
- [Spilling Service architecture](../../contributor/spilling-service.md)
- [Spilling troubleshooting](../../troubleshooting/spilling/index.md)
- [Memory controller configuration](memory_controller_config.md)
- [Monitoring {{ ydb-short-name }}](../../devops/observability/monitoring.md)
- [Performance diagnostics](../../troubleshooting/performance/index.md)
