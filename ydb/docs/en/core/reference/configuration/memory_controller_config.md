# memory_controller_config

There are many components inside {{ ydb-short-name }} [database nodes](../../concepts/glossary.md#database-node) that utilize memory. Most of them need a fixed amount, but some are flexible and can use varying amounts of memory, typically to improve performance. If {{ ydb-short-name }} components allocate more memory than is physically available, the operating system is likely to [terminate](https://en.wikipedia.org/wiki/Out_of_memory#Recovery) the entire {{ ydb-short-name }} process, which is undesirable. The memory controller's goal is to allow {{ ydb-short-name }} to avoid out-of-memory situations while still efficiently using the available memory.

Examples of components managed by the memory controller:

- [Shared cache](../../concepts/glossary.md#shared-cache): stores recently accessed data pages read from [distributed storage](../../concepts/glossary.md#distributed-storage) to reduce disk I/O and accelerate data retrieval.
- [MemTable](../../concepts/glossary.md#memtable): holds data that has not yet been flushed to [SST](../../concepts/glossary.md#sst).
- [KQP](../../concepts/glossary.md#kqp): stores intermediate query results.
- Allocator caches: keep memory blocks that have been released but not yet returned to the operating system.

Memory limits can be configured to control overall memory usage, ensuring the database operates efficiently within the available resources.

## Hard Memory Limit {#hard-memory-limit}

The hard memory limit specifies the total amount of memory available to the {{ ydb-short-name }} process.

By default, the hard memory limit for the {{ ydb-short-name }} process is set to its [cgroups](https://en.wikipedia.org/wiki/Cgroups) memory limit.

In environments without a cgroups memory limit, the default hard memory limit equals the host's total available memory. This configuration allows the database to utilize all available resources but may lead to resource competition with other processes on the same host. Although the memory controller attempts to account for this external consumption, such a setup is not recommended.

Additionally, the hard memory limit can be specified in the configuration. Note that the database process may still exceed this limit. Therefore, it is highly recommended to use cgroups memory limits in production environments to enforce strict memory control.

Most of other memory limits can be configured either in absolute bytes or as a percentage relative to the hard memory limit. Using percentages is advantageous for managing clusters with nodes of varying capacities. If both absolute byte and percentage limits are specified, the memory controller uses a combination of both (maximum for lower limits and minimum for upper limits).

Example of the `memory_controller_config` section with a specified hard memory limit:

```yaml
memory_controller_config:
  hard_limit_bytes: 16106127360
```

## Soft Memory Limit {#soft-memory-limit}

The soft memory limit specifies a dangerous threshold that should not be exceeded by the {{ ydb-short-name }} process under normal circumstances.

If the soft limit is exceeded, {{ ydb-short-name }} gradually reduces the [shared cache](../../concepts/glossary.md#shared-cache) size to zero. Therefore, more database nodes should be added to the cluster as soon as possible, or per-component memory limits should be reduced.

## Target Memory Utilization {#target-memory-utilization}

The target memory utilization specifies a threshold for the {{ ydb-short-name }} process memory usage that is considered optimal.

Flexible cache sizes are calculated according to their limit thresholds to keep process consumption around this value.

For example, in a database that consumes a little memory on query execution, caches consume memory around this threshold, and other memory stays free. If query execution consumes more memory, caches start to reduce their sizes to their minimum threshold.

## Per-Component Memory Limits

There are two different types of components within {{ ydb-short-name }}.

The first type, known as cache components, functions as caches, for example, by storing the most recently used data. Each cache component has minimum and maximum memory limit thresholds, allowing them to adjust their capacity dynamically based on the current {{ ydb-short-name }} process consumption.

The second type, known as activity components, allocates memory for specific activities, such as query execution or the [compaction](../../concepts/glossary.md#compaction) process. Each activity component has a fixed memory limit. Additionally, there is a total memory limit for these activities from which they attempt to draw the required memory.

Many other auxiliary components and processes operate alongside the {{ ydb-short-name }} process, consuming memory. Currently, these components do not have any memory limits.

### Cache Components Memory Limits

The cache components include:

- Shared cache
- MemTable

Each cache component's limits are dynamically recalculated every second to ensure that each component consumes memory proportionally to its limit thresholds while the total consumed memory stays close to the target memory utilization.

The minimum memory limit threshold for cache components isn't reserved, meaning the memory remains available until it is actually used. However, once this memory is filled, the components typically retain the data, operating within their current memory limit. Consequently, the sum of the minimum memory limits for cache components is expected to be less than the target memory utilization.

If needed, both the minimum and maximum thresholds should be overridden; otherwise, any missing threshold will have a default value.

Example of the `memory_controller_config` section with specified shared cache limits:

```yaml
memory_controller_config:
  shared_cache_min_percent: 10
  shared_cache_max_percent: 30
```

### Activity Components Memory Limits

The activity components include:

- KQP

The memory limit for each activity component specifies the maximum amount of memory it can attempt to use. However, to prevent the {{ ydb-short-name }} process from exceeding the soft memory limit, the total consumption of activity components is further constrained by an additional limit known as the activities memory limit. If the total memory usage of the activity components exceeds this limit, any additional memory requests will be denied. When query execution approaches memory limits, {{ ydb-short-name }} activates [spilling](../../concepts/spilling.md) to temporarily save intermediate data to disk, preventing memory limit violations.

As a result, while the combined individual limits of the activity components might collectively exceed the activities memory limit, each component's individual limit should be less than this overall cap. Additionally, the sum of the minimum memory limits for the cache components, plus the activities memory limit, must be less than the soft memory limit.

There are some other activity components that currently do not have individual memory limits.

Example of the `memory_controller_config` section with a specified KQP limit:

```yaml
memory_controller_config:
  query_execution_limit_percent: 25
```

## Configuration Parameters

Each configuration parameter applies within the context of a single database node.

As mentioned above, the sum of the minimum memory limits for the cache components plus the activities memory limit should be less than the soft memory limit.

This restriction can be expressed in a simplified form:

$shared\_cache\_min\_percent + mem\_table\_min\_percent + activities\_limit\_percent < soft\_limit\_percent$

Or in a detailed form:

$Max(shared\_cache\_min\_percent * hard\_limit\_bytes / 100, shared\_cache\_min\_bytes) + Max(mem\_table\_min\_percent * hard\_limit\_bytes / 100, mem\_table\_min\_bytes) + Min(activities\_limit\_percent * hard\_limit\_bytes / 100, activities\_limit\_bytes) < Min(soft\_limit\_percent * hard\_limit\_bytes / 100, soft\_limit\_bytes)$

| Parameter | Default | Description |
| --- | --- | --- |
| `hard_limit_bytes` | CGroup&nbsp;memory&nbsp;limit&nbsp;/<br/>Host memory | Hard memory usage limit. |
| `soft_limit_percent`&nbsp;/<br/>`soft_limit_bytes` | 75% | Soft memory usage limit. |
| `target_utilization_percent`&nbsp;/<br/>`target_utilization_bytes` | 50% | Target memory utilization. |
| `activities_limit_percent`&nbsp;/<br/>`activities_limit_bytes` | 30% | Activities memory limit. |
| `shared_cache_min_percent`&nbsp;/<br/>`shared_cache_min_bytes` | 20% | Minimum threshold for the shared cache memory limit. |
| `shared_cache_max_percent`&nbsp;/<br/>`shared_cache_max_bytes` | 50% | Maximum threshold for the shared cache memory limit. |
| `mem_table_min_percent`&nbsp;/<br/>`mem_table_min_bytes` | 1% | Minimum threshold for the MemTable memory limit. |
| `mem_table_max_percent`&nbsp;/<br/>`mem_table_max_bytes` | 3% | Maximum threshold for the MemTable memory limit. |
| `query_execution_limit_percent`&nbsp;/<br/>`query_execution_limit_bytes` | 20% | KQP memory limit. |
