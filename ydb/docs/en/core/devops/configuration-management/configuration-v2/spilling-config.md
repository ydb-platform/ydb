# Spilling Configuration

## Overview

[Spilling](../../../concepts/spilling.md) is a memory management mechanism in {{ ydb-short-name }} that allows temporarily saving data to disk when running out of RAM. This section describes configuration parameters for setting up spilling in production environments.

All spilling settings are located in the `table_service_config` section, which is at the same level as `host_configs`.

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 21474836480    # 20 GiB
      max_file_size: 5368709120      # 5 GiB
      max_file_part_size: 104857600  # 100 MB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
```

## Main Configuration Parameters

### Spilling Service (spilling_service_config)

**Location:** `table_service_config.spilling_service_config`

The main configuration of the spilling service is defined in the `spilling_service_config` section:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 21474836480    # 20 GiB
      max_file_size: 5368709120      # 5 GiB
      max_file_part_size: 104857600  # 100 MB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
```

#### Root

**Type:** `string`  
**Default:** `""` (automatic detection)  
**Description:** Directory for saving spilling files. When empty, the system automatically creates a directory in the format `{TMP}/spilling-tmp-<username>`.

**Important features:**

- At process startup, all existing spilling files in the specified directory are automatically deleted
- The directory must have sufficient write permissions

**Recommendations:**

- Use a separate disk or partition for spilling
- Preferably use fast storage devices (SSD/NVMe)
- Ensure sufficient free space is available

#### MaxTotalSize

**Type:** `uint64`  
**Default:** `21474836480` (20 GiB)  
**Description:** Maximum total size of all spilling files. When the limit is exceeded, spilling operations fail with an error.

**Recommendations:**

- Set the value based on available disk space

#### MaxFileSize

{% note warning %}

This option is deprecated and will be removed in future versions.

{% endnote %}

**Type:** `uint64`  
**Default:** `5368709120` (5 GiB)  
**Description:** Maximum size of a single spilling file.

#### MaxFilePartSize

{% note warning %}

This option is deprecated and will be removed in future versions.

{% endnote %}

**Type:** `uint64`  
**Default:** `104857600` (100 MB)  
**Description:** Maximum size of one file part. Spilling files can consist of multiple parts, each up to `MaxFilePartSize` in size. The total size of all parts must not exceed `MaxFileSize`.

### Thread Pool Configuration (TIoThreadPoolConfig)

#### WorkersCount

**Type:** `uint32`  
**Default:** `2`  
**Description:** Number of worker threads for processing spilling I/O operations.

**Recommendations:**

- Increase for high-load systems
- Consider the number of CPU cores on the server

#### QueueSize

**Type:** `uint32`  
**Default:** `1000`  
**Description:** Size of the spilling operations queue. Each task sends only one data block to spilling at a time, so large values are usually not required.

## Memory Management

### Relationship with memory_controller_config

Spilling activation is closely related to memory controller settings. Detailed `memory_controller_config` configuration is described in a [separate article](../../../reference/configuration/index.md#memory-controller-config).

The key parameter for spilling is **`activities_limit_percent`**, which determines the amount of memory allocated for background activities. This parameter affects the available memory for user queries and, accordingly, the frequency of spilling activation.

**Impact on spilling:**

- When increasing `activities_limit_percent`, less memory remains for query execution → spilling activates more frequently
- When decreasing `activities_limit_percent`, more memory is available for queries → spilling activates less frequently

## File System Requirements

### File Descriptors

For proper spilling operation, it is necessary to increase the limit of simultaneously open file descriptors to 10000.

## Configuration Examples

### High-load System

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 107374182400   # 100 GiB
      max_file_size: 10737418240     # 10 GiB
      max_file_part_size: 1073741824 # 1 GiB
      io_thread_pool:
        workers_count: 8
        queue_size: 2000
```

### Limited Resources

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 5368709120     # 5 GiB
      max_file_size: 1073741824      # 1 GiB
      max_file_part_size: 52428800   # 50 MB
      io_thread_pool:
        workers_count: 1
        queue_size: 500
```

## Complete Configuration

### Enabling and Disabling Spilling

The following parameters control the enabling and disabling of various spilling types. They should typically only be changed when there are specific system requirements.

#### enable_query_service_spilling

**Location:** `table_service_config.enable_query_service_spilling`  
**Type:** `boolean`  
**Default:** `true`  
**Description:** Global option that enables spilling in data transfer channels between tasks.

```yaml
table_service_config:
  enable_query_service_spilling: true
```

**Important:** This setting works in conjunction with the local spilling service configuration. When disabled (`false`), channel spilling does not function even with enabled `spilling_service_config`.

#### enable_spilling_nodes

**Location:** `table_service_config.enable_spilling_nodes`  
**Type:** `string`  
**Possible values:** `"All"` | `"GraceJoin"` | `"Aggregate"` | `"None"`  
**Default:** `"All"`  
**Description:** Controls enabling spilling in compute nodes.

```yaml
table_service_config:
  enable_spilling_nodes: "All"
```

#### Enable (in spilling_service_config)

**Type:** `boolean`  
**Default:** `true`  
**Description:** Enables or disables the spilling service. When disabled (`false`), [spilling](../../../concepts/spilling.md) does not function, which may lead to errors when processing large data volumes.

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
```

### Complete Configuration Example

```yaml
table_service_config:
  enable_query_service_spilling: true
  enable_spilling_nodes: "All"
  spilling_service_config:
    local_file_config:
      enable: true
      root: ""
      max_total_size: 21474836480    # 20 GiB
      max_file_size: 5368709120      # 5 GiB
      max_file_part_size: 104857600  # 100 MB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
```

## Troubleshooting

### Common Issues

1. **Service not started...**
    Attempt to enable spilling with disabled Spilling Service.
    - Set `table_service_config.enable_query_service_spilling: true`
    Read more about spilling architecture in the section [Spilling Architecture in {{ ydb-short-name }}](../../../concepts/spilling.md#spilling-architecture-in-ydb)

2. **Total size limit exceeded...**
   - Increase `MaxTotalSize`

## See Also

- [Spilling Concept](../../../concepts/spilling.md)
- [Memory Controller Configuration](../../../reference/configuration/index.md#memory-controller-config)
- [{{ ydb-short-name }} Monitoring](../../observability/monitoring.md)
- [Performance Diagnostics](../../../troubleshooting/performance/index.md) 