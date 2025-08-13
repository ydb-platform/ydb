# Spilling Configuration

[Spilling](../../concepts/spilling.md) is a memory management mechanism in {{ ydb-short-name }} that temporarily saves data to disk when the system runs out of RAM. This section describes configuration parameters for setting up spilling in production environments.


## Primary Configuration Parameters

### Spilling Service (spilling_service_config)

**Location:** `table_service_config.spilling_service_config`

The main configuration of the spilling service is defined in the `spilling_service_config` section:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 21474836480    # 20 GiB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
```

#### root

**Type:** `string`  
**Default:** `""` (automatic detection)  
**Description:** A filesystem directory for saving spilling files. When empty, the system automatically creates a directory in the format `{TMP}/spilling-tmp-<username>`.

- `{TMP}` — system temporary directory, determined from the `TMPDIR` environment variable or standard system paths
- `<username>` — username under which the `ydbd` process is running

Spilling files have the following name format:

`node_<node_id>_<session_id>`

Where:

- `node_id` — node identifier
- `session_id` — unique session identifier that is created when initializing the [Spilling Service](../../contributor/spilling-service.md) once when the ydbd process starts

Example of a complete spilling file path:

```bash
/tmp/spilling-tmp-user/node_1_32860791-037c-42b4-b201-82a0a337ac80
```

**Important notes:**

- At process startup, all existing spilling files in the specified directory are automatically deleted. Spilling files have a special name format that includes a session identifier, which is generated once when the ydbd process starts. When a new process starts, all files in the spilling directory that match the name format but have a different session identifier from the current one are deleted.
- The directory must have sufficient write and read permissions for the user under which ydbd is running

**Recommendations:**

- Use a separate disk or partition for spilling
- Preferably use fast storage devices (SSD/NVMe)
- Ensure sufficient free space is available

**Possible errors:**

- **Permission denied** — insufficient directory access permissions. See [Spilling Troubleshooting](../../troubleshooting/spilling.md#permission-denied)

#### max_total_size

**Type:** `uint64`  
**Default:** `21474836480` (20 GiB)  
**Description:** Maximum total size of all spilling files. When the limit is exceeded, spilling operations fail with an error.

##### Recommendations

- Set the value based on available disk space

**Possible errors:**

- `Total size limit exceeded: X/YMb` — maximum total size of spilling files exceeded. See [{#T}](../../troubleshooting/spilling.md#total-size-limit-exceeded)

### Thread Pool Configuration (TIoThreadPoolConfig)

{% note info %}

I/O pool threads for spilling are created in addition to the threads allocated to the [actor system](../../concepts/glossary.md#actor-system). When planning the number of threads, consider the overall system load.

{% endnote %}

#### WorkersCount

**Type:** `uint32`  
**Default:** `2`  
**Description:** Number of worker threads for processing spilling I/O operations.

**Recommendations:**

- Increase for high-load systems
- Consider the number of CPU cores on the server

**Possible errors:**

- **Can not run operation** — I/O thread pool operation queue overflow. See [Spilling Troubleshooting](../../troubleshooting/spilling.md#can-not-run-operation)

#### QueueSize

**Type:** `uint32`  
**Default:** `1000`  
**Description:** Size of the spilling operations queue. Each task sends only one data block to spilling at a time, so large values are usually not required.

**Possible errors:**

- **Can not run operation** — I/O thread pool operation queue overflow. See [Spilling Troubleshooting](../../troubleshooting/spilling.md#can-not-run-operation)

## Memory Management

### Relationship with memory_controller_config

Spilling activation is closely related to memory controller settings. Detailed `memory_controller_config` configuration is described in a [separate article](../../reference/configuration/index.html#memory-controller).

The key parameter for spilling is **`activities_limit_percent`**, which determines the amount of memory allocated for query processing activities. This parameter affects the available memory for user queries and, accordingly, the frequency of spilling activation.

**Impact on spilling:**

- When increasing `activities_limit_percent`, less memory remains for query execution → spilling activates more frequently
- When decreasing `activities_limit_percent`, more memory is available for queries → spilling activates less frequently

## File System Requirements

### File Descriptors

{% note warning %}

For proper spilling operation in multi-node clusters, it is recommended to increase the limit of simultaneously open file descriptors to 10000. For small clusters (1-3 nodes), this setting is usually not required.

{% endnote %}

To change the file descriptor limit, add the following lines to the `/etc/security/limits.conf` file:

```bash
ydb soft nofile 10000
ydb hard nofile 10000
```

Where `ydb` is the username under which `ydbd` runs.

After changing the file, you need to reboot the system or re-login to apply the new limits.

## Configuration Examples

### High-load System

For maximum performance in high-load systems, it is recommended to increase the spilling size and number of worker threads:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 107374182400   # 100 GiB
      io_thread_pool:
        workers_count: 8
        queue_size: 2000
```

### Limited Resources

For systems with limited resources, it is recommended to use conservative settings:

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      root: ""
      max_total_size: 5368709120     # 5 GiB
      io_thread_pool:
        workers_count: 1
        queue_size: 500
```

## Advanced Configuration

### Enabling and Disabling Spilling

The following parameters control the enabling and disabling of various spilling types. They should typically only be changed when there are specific system requirements.

#### local_file_config.enable

**Location:** `table_service_config.spilling_service_config.local_file_config.enable`
**Type:** `boolean`  
**Default:** `true`  
**Description:** Enables or disables the spilling service. When disabled (`false`), [spilling](../../concepts/spilling.md) does not function, which may lead to errors when processing large data volumes.

**Possible errors:**

- **Spilling Service not started** / **Service not started** — attempt to use spilling when Spilling Service is disabled. See [Spilling Troubleshooting](../../troubleshooting/spilling.md#spilling-service-not-started)

```yaml
table_service_config:
  spilling_service_config:
    local_file_config:
      enable: true
```

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

## Complete Example

```yaml
table_service_config:
  enable_query_service_spilling: true
  enable_spilling_nodes: "All"
  spilling_service_config:
    local_file_config:
      enable: true
      root: ""
      max_total_size: 21474836480    # 20 GiB
      io_thread_pool:
        workers_count: 2
        queue_size: 1000
```

## Troubleshooting

Detailed information about diagnosing and resolving spilling issues is available in the [Spilling Troubleshooting](../../troubleshooting/spilling.md) section.

## See Also

- [Spilling Concept](../../concepts/spilling.md)
- [Memory Controller Configuration](../../reference/configuration/index.html#memory-controller)
- [{{ ydb-short-name }} Monitoring](../../devops/observability/monitoring.md)
- [Performance Diagnostics](../../troubleshooting/performance/index.md)
