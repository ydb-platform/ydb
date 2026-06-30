# `table_service_config` configuration section

The `table_service_config` section contains configuration parameters for the table service, including spilling settings.

## resource_manager {#resource-manager}

The `resource_manager` subsection contains resource management parameters for the table service.

### Vector index level cache

The level table (`indexImplLevelTable`) of a [`vector_kmeans_tree`](../../dev/vector-indexes-kmeans-tree-type.md#index-structure) vector index stores the centroids that {{ ydb-short-name }} reads on every step down the cluster tree during a vector search. To avoid re-reading these centroids from distributed storage on each query, {{ ydb-short-name }} can cache them in memory on each database node.

The cache is per-node, uses LRU eviction, and grows incrementally up to the configured cap.

```yaml
table_service_config:
  resource_manager:
    kqp_level_cache_max_size_bytes: 0
    kqp_level_cache_increase_batch_size_bytes: 33554432
```

#### resource_manager.kqp_level_cache_max_size_bytes

**Type:** `uint64`  
**Default:** `0` (cache disabled)  
**Description:** Maximum size of the vector index level table cache, in bytes, per database node. Setting this to a non-zero value enables the cache.

#### resource_manager.kqp_level_cache_increase_batch_size_bytes

**Type:** `uint64`  
**Default:** `33554432` (32 MiB)  
**Description:** Granularity of incremental memory allocation for the level cache. The cache grows by this amount at a time, up to `kqp_level_cache_max_size_bytes`, and shrinks by the same amount when the cap is reduced.

### Query execution memory limits {#query-execution-memory-limits}

The `resource_manager` subsection defines the spilling threshold relative to the query memory pool on a [node](../../concepts/glossary.md#node). The pool size is controlled by [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit) in `memory_controller_config`.

```yaml
table_service_config:
  resource_manager:
    spilling_percent: 80
```

#### resource_manager.spilling_percent {#spilling-percent}

**Type:** `double`  
**Default:** `80`  
**Description:** Query memory pool fill threshold at which {{ ydb-short-name }} starts treating [spilling](../../concepts/query_execution/spilling.md) as the preferred way to manage memory. The calculation is based on the Query Processor pool size set by [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit).

When total query memory consumption on a node exceeds `spilling_percent` percent of the available pool, compute operations that support spilling (Grace Hash Join, aggregations, and others) are signaled to offload intermediate data to disk instead of further growing RAM usage.

For example, with the default value of `80`, spilling is triggered when the query pool is approximately 80% full.

The threshold applies to:

- the shared query memory pool on the node (size — see [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit));
- a [resource pool](../../concepts/glossary.md#resource-pool), if the query runs in a workload pool with `total_memory_limit_percent_per_node`.

{% note info %}

`spilling_percent` does not limit the total size of spilling files on disk. Disk quotas are controlled by [`local_file_config.max_total_size`](#local-file-config-max-total-size) in the `spilling_service_config` section.

{% endnote %}

##### Interaction with other parameters

| Parameter | Scope | Role |
| --- | --- | --- |
| `query_execution_limit_percent` / `query_execution_limit_bytes` | Node (QP) | Query memory pool size |
| `spilling_percent` | Query / pool | Pool fill threshold after which spilling is preferred |
| `activities_limit_percent` | Node | Shared memory limit for all activity components (QP, compaction, etc.) |

`spilling_percent` defines when the query pool on the node is full enough that further RAM growth should give way to spilling. `activities_limit_percent` limits memory for activities overall and indirectly affects available RAM, but does not replace the pool against which `spilling_percent` is calculated.

##### Recommendations

- Decrease `spilling_percent` (for example, to `70`) to move heavy queries to disk earlier and reduce the risk of exhausting the memory pool.
- Increase `spilling_percent` (for example, to `90`) if disk spilling hurts performance too often while the node has enough RAM.
- Align `spilling_percent` with [`query_execution_limit_percent` / `query_execution_limit_bytes`](memory_controller_config.md#query-execution-limit): when you increase the query pool limit, you can raise the spilling threshold if the node has enough RAM.

## spilling_service_config

[Spilling](../../concepts/query_execution/spilling.md) is a memory management mechanism in {{ ydb-short-name }} that temporarily saves data to disk when the system runs out of RAM.

### Enable {#enable}

Spilling is enabled by default. The following parameter controls enabling and disabling the spilling service.

#### local_file_config.enable {#local-file-config-enable}

**Location:** `table_service_config.spilling_service_config.local_file_config.enable`  
**Type:** `boolean`  
**Default:** `true`  
**Description:** Enables or disables the spilling service. When disabled (`false`), [spilling](../../concepts/query_execution/spilling.md) does not function, which may lead to errors when processing large data volumes.

##### Possible errors

- `Spilling Service not started` / `Service not started` — attempt to use spilling when Spilling Service is disabled. See [{#T}](../../troubleshooting/spilling/service-not-started.md)

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
```

### Primary Configuration Parameters

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 21474836480
```

### Directory Configuration

#### local_file_config.root

**Type:** `string`  
**Default:** `""` (temporary directory)  
**Description:** A filesystem directory for saving spilling files.

For each `ydbd` process, a separate directory is created with a unique name. Spilling directories have the following name format:

`node_<node_id>_<spilling_service_id>`

Where:

- `node_id` — [node](../../concepts/glossary.md#node) identifier
- `spilling_service_id` — unique instance identifier that is created when initializing the [Spilling Service](../../contributor/spilling-service.md) one time when the ydbd process starts

Spilling files are stored inside each such directory.

Example of a complete spilling directory path:

```bash
/tmp/spilling-tmp-<username>/node_1_32860791-037c-42b4-b201-82a0a337ac80
```

Where:

- `/tmp` — value of the `root` parameter
- `<username>` — username under which the `ydbd` process is running

**Important notes:**

- At process startup, all existing spilling directories in the specified directory are automatically deleted. Spilling directories have a special name format that includes an instance identifier, which is generated once when the ydbd process starts. When a new process starts, all directories in the spilling directory that match the name format but have a different `spilling_service_id` from the current one are deleted.
- The directory must have sufficient write and read permissions for the user under which ydbd is running

{% note info %}

Spilling is only performed on [database nodes](../../concepts/glossary.md#database-node).

{% endnote %}

##### Possible errors

- `Permission denied` — insufficient directory access permissions. See [{#T}](../../troubleshooting/spilling/permission-denied.md)

#### local_file_config.max_total_size {#local-file-config-max-total-size}

**Type:** `uint64`  
**Default:** `21474836480` (20 GiB)  
**Description:** Maximum total size of all spilling files on each [node](../../concepts/glossary.md#node). When the limit is exceeded, spilling operations fail with an error. The total spilling limit across the entire cluster is the sum of `max_total_size` values from all nodes.

##### Recommendations

- Set the value based on available disk space

##### Possible errors

- `Total size limit exceeded: X/YMb` — maximum total size of spilling files exceeded. See [{#T}](../../troubleshooting/spilling/total-size-limit-exceeded.md)

### Memory Management {#memory-management}

#### Relationship with memory_controller_config

Spilling activation is closely related to memory controller settings. Detailed `memory_controller_config` configuration is described in a [separate article](memory_controller_config.md).

The key parameter for spilling is **`activities_limit_percent`**, which determines the amount of memory allocated for query processing activities. This parameter affects the available memory for user queries and, accordingly, the frequency of spilling activation.

#### Spilling threshold in resource_manager

The direct runtime threshold at which compute operations switch to spilling is set by [`resource_manager.spilling_percent`](#spilling-percent). It defines at what fill level of the Query Processor memory pool intermediate query data starts being offloaded to disk. The pool size is set in [`memory_controller_config`](memory_controller_config.md#query-execution-limit). For details, see [Query execution memory limits](#query-execution-memory-limits).

#### Impact on spilling

- When increasing `activities_limit_percent`, more memory is available for queries → spilling activates less frequently
- When decreasing `activities_limit_percent`, less memory is available for queries → spilling activates more frequently
- When decreasing `spilling_percent`, spilling starts at a lower query pool fill level
- When increasing `spilling_percent`, tasks keep growing RAM usage longer before switching to spilling

{% note warning %}

However, it's important to consider that spilling itself requires memory. If you set `activities_limit_percent` too high, memory may still be exhausted despite spilling, as the spilling mechanism itself consumes memory resources.

{% endnote %}

### File System Requirements

#### File Descriptors

{% note info %}

For information about configuring file descriptor limits during initial deployment, see the [File Descriptor Limits](../../devops/deployment-options/manual/initial-deployment/deployment-preparation.md#file-descriptors) section.

{% endnote %}

### Configuration Examples

#### High-load System

For maximum performance in high-load systems, it is recommended to increase the spilling size:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 107374182400   # 100 GiB
```

#### Limited Resources

For systems with limited resources, it is recommended to use conservative settings:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 5368709120     # 5 GiB
```

### Complete Example

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/spilling"
      max_total_size: 53687091200    # 50 GiB
```

## See Also

- [Spilling Concept](../../concepts/query_execution/spilling.md)
- [Spilling Service Architecture](../../contributor/spilling-service.md)
- [Spilling Troubleshooting](../../troubleshooting/spilling/index.md)
- [Memory Controller Configuration](memory_controller_config.md)
- [{{ ydb-short-name }} Monitoring](../../devops/observability/monitoring.md)
- [Performance Diagnostics](../../troubleshooting/performance/index.md)
