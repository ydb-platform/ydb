# Spilling configuration

## Overview

[Spilling](../../../concepts/spilling.md) is a memory management mechanism in {{ ydb-short-name }} that allows temporarily storing data to disk when RAM is insufficient. This section describes configuration parameters for setting up spilling in production environments.

All spilling settings are located in the `table_service_config` section, which is at the same level as `host_configs`.

```yaml
table_service_config:
  enable_query_service_spilling: true
  enable_spilling_nodes: "All"
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/ydb/spill"
      max_total_size: 21474836480    # 20 GiB
      max_file_size: 5368709120      # 5 GiB
      max_file_part_size: 104857600  # 100 MB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
  resource_manager:
    spilling_percent: 80.0
```

## Global spilling settings

### enable_query_service_spilling

**Location:** `table_service_config.enable_query_service_spilling`  
**Type:** `boolean`  
**Default:** `true`  
**Description:** Global option that enables spilling in data transfer channels between tasks.

```yaml
table_service_config:
  enable_query_service_spilling: true
```

**Important:** This setting works together with the local spilling service configuration. When disabled (`false`), channel spilling does not function even with enabled `spilling_service_config`.

### enable_spilling_nodes

**Location:** `table_service_config.enable_spilling_nodes`  
**Type:** `string`  
**Possible values:** `"All"` | `"GraceJoin"` | `"Aggregate"` | `"None"`  
**Default:** `"All"`  
**Description:** Controls spilling activation in compute nodes.

```yaml
table_service_config:
  enable_spilling_nodes: "All"
```

## Main configuration parameters

### Spilling service (spilling_service_config)

**Location:** `table_service_config.spilling_service_config`

Main spilling service configuration is defined in the `spilling_service_config` section:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/ydb/spill"
      max_total_size: 21474836480    # 20 GiB
      max_file_size: 5368709120      # 5 GiB
      max_file_part_size: 104857600  # 100 MB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
```

#### Enable (required parameter)

**Type:** `boolean`  
**Default:** `true`  
**Description:** Enables or disables the spilling service. When disabled (`false`), [spilling](../../../concepts/spilling.md) does not function, which may lead to Out of Memory errors when processing large data volumes.

**Recommendations:**
- Always set to `true` for production environments
- Disable only for testing or debugging

#### Root

**Type:** `string`  
**Default:** `""` (automatic detection)  
**Description:** Directory for saving spilling files. With empty value, the system automatically creates a directory in format `{TMP}/spilling-tmp-<username>`.

**Important features:**
- At process startup, all existing spilling files in the specified directory are automatically deleted
- This prevents file accumulation during emergency process termination
- Directory must have sufficient write permissions

**Recommendations:**
- Use a separate disk or partition for spilling
- Preferably use fast storage (SSD/NVMe)
- Ensure sufficient free space

#### MaxTotalSize

**Type:** `uint64`  
**Default:** `21474836480` (20 GiB)  
**Description:** Maximum total size of all spilling files. When the limit is exceeded, spilling operations fail with error.

**Recommendations:**
- Set value based on available disk space
- Leave reserve for other system operations
- Monitor disk space usage

#### MaxFileSize

**Type:** `uint64`  
**Default:** `5368709120` (5 GiB)  
**Description:** Maximum size of one spilling file. Corresponds to the limit on spilled data block (bucket) size.

**Usage context:**
- One WideCombiner task can create up to 128 files
- One GraceJoin task can create up to 64 files (32 × 2 join sides)
- One data transfer channel creates 1 file

#### MaxFilePartSize

**Type:** `uint64`  
**Default:** `104857600` (100 MB)  
**Description:** Maximum size of one file part. Spilling files can consist of several parts, each up to `MaxFilePartSize` in size. Total size of all parts must not exceed `MaxFileSize`.

### Thread pool configuration (TIoThreadPoolConfig)

#### WorkersCount

**Type:** `uint32`  
**Default:** `2`  
**Description:** Number of worker threads for processing spilling I/O operations.

**Recommendations:**
- Increase for high-load systems
- Consider CPU core count on server
- Start with default value and adjust based on monitoring

#### QueueSize

**Type:** `uint32`  
**Default:** `1000`  
**Description:** Size of spilling operations queue. Each task sends only one data block for spilling simultaneously, so large values are usually not required.

## Memory management

### Connection with memory_controller_config

Spilling activation is closely related to memory controller settings. Detailed `memory_controller_config` configuration is described in a [separate article](../../../reference/configuration/index.md#memory-controller-config).

The key parameter for spilling is **`activities_limit_percent`** (default 30%), which determines memory volume allocated for background activities. This parameter affects available memory for user queries and, consequently, spilling activation frequency.

**Impact on spilling:**
- When increasing `activities_limit_percent`, less memory remains for query execution → spilling activates more frequently
- When decreasing `activities_limit_percent`, more memory is available for queries → spilling activates less frequently

### spilling_percent in resource_manager

**Location:** `table_service_config.resource_manager.spilling_percent`  
**Type:** `double`  
**Default:** `80.0`  
**Description:** Percentage of query execution limit usage at which the system starts activating spilling.

```yaml
table_service_config:
  resource_manager:
    spilling_percent: 80.0  # 80% of available memory for queries
```

**Performance impact:**

- **Lower values (60-70%):**
  - Spilling activates earlier and more frequently
  - More reliable operation with limited memory
  - Potentially slower query execution due to frequent I/O operations
  - Recommended for mission-critical systems

- **Higher values (85-95%):**
  - Spilling activates later, with higher memory utilization
  - Faster query execution under normal conditions
  - Increased risk of out-of-memory errors under peak loads
  - Recommended for high-performance systems with sufficient memory

**Configuration recommendations:**
- Start with default value (80%)
- Decrease with frequent out-of-memory errors
- Increase with stable operation to improve performance
- Monitor memory usage and spilling activation frequency

### Memory settings and spilling interaction

Spilling activation threshold depends on `memory_controller_config` settings and `spilling_percent`:

1. **Available memory for queries** is determined by parameters in `memory_controller_config`
2. **Spilling threshold** = available memory × `spilling_percent` / 100

**Example:**
If after `memory_controller_config` setup, 32 GiB memory is available for queries and `spilling_percent = 80`, spilling will start activating at ~25.6 GiB memory usage.

## File system requirements

### File descriptors

Correct spilling operation requires increasing the limit of simultaneously open file descriptors.

**File count calculation:**
- WideCombiner: up to 128 files per task
- GraceJoin: up to 64 files per task (32 × 2)
- Channels: 1 file per channel

**Recommended system settings:**
```bash
# In /etc/security/limits.conf
ydb soft nofile 65536
ydb hard nofile 65536

# Or via systemd
LimitNOFILE=65536
```

## Monitoring and diagnostics

### Key metrics

Recommended metrics to track:

- Disk space usage in spilling directory
- Number of active spilling operations
- I/O operation execution time
- Limit trigger frequency

### Logging

Enable spilling operation logging for diagnostics:

```yaml
logging:
  level: INFO
  components:
    - name: "SPILLING"
      level: DEBUG
```

## Configuration examples

### High-load system

```yaml
table_service_config:
  enable_query_service_spilling: true
  enable_spilling_nodes: "All"
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/ssd/ydb/spill"
      max_total_size: 107374182400   # 100 GiB
      max_file_size: 10737418240     # 10 GiB
      max_file_part_size: 1073741824 # 1 GiB
      io_thread_pool:
        workers_count: 8
        queue_size: 2000
  resource_manager:
    spilling_percent: 85.0
```

### Limited resources

```yaml
table_service_config:
  enable_query_service_spilling: true
  enable_spilling_nodes: "GraceJoin"  # Only for join operations
  spilling_service_config:
    local_file_config:
      enable: true
      root: "/var/ydb/spill"
      max_total_size: 5368709120     # 5 GiB
      max_file_size: 1073741824      # 1 GiB
      max_file_part_size: 52428800   # 50 MB
      io_thread_pool:
        workers_count: 1
        queue_size: 500
  resource_manager:
    spilling_percent: 75.0
```

## Troubleshooting

### Common issues

1. **Disk space shortage errors**
   - Increase `MaxTotalSize` or free space in spilling directory
   - Check log rotation settings

2. **High operation latency**
   - Move spilling directory to faster disk
   - Increase `WorkersCount` in thread pool

3. **File descriptor errors**
   - Increase system limits on open file count
   - Optimize `MaxFilePartSize` to reduce file count

## See also

- [Spilling concept](../../../concepts/spilling.md)
- [Memory controller configuration](../../../reference/configuration/index.md#memory-controller-config)
- [{{ ydb-short-name }} monitoring](../../observability/monitoring.md)
- [Performance tuning](../performance-tuning.md) 