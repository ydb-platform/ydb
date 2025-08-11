# Spilling Troubleshooting

## Common Issues

### 1. Permission denied {#permission-denied}

**Description:** Insufficient access permissions to the spilling directory.

**Solution:**

- Ensure the directory has write and read permissions for the user under which ydbd is running
- Check access permissions: `ls -la /path/to/spilling/directory`
- If necessary, change permissions: `chmod 755 /path/to/spilling/directory`

### 2. Spilling Service not started / Service not started {#spilling-service-not-started}

**Description:** Attempt to use spilling when Spilling Service is disabled.

**Solution:**

- Enable spilling: `table_service_config.spilling_service_config.local_file_config.enable: true`

Read more about spilling architecture in the section [Spilling Architecture in {{ ydb-short-name }}](../concepts/spilling.md#spilling-architecture-in-ydb)

### 3. Total size limit exceeded: X/YMb {#total-size-limit-exceeded}

**Description:** Maximum total size of spilling files exceeded (parameter `max_total_size`).

**Solution:**

- Increase `max_total_size` in configuration

### 4. Can not run operation {#can-not-run-operation}

**Description:** I/O thread pool operation queue overflow.

**Solution:**

- Increase `queue_size` in `io_thread_pool`
- Increase `workers_count` for faster operation processing
- Check disk performance

## See Also

- [Spilling Configuration](../reference/configuration/spilling.md)
- [Spilling Concept](../concepts/spilling.md)
- [Memory Controller Configuration](../reference/configuration/index.html#memory-controller)
- [{{ ydb-short-name }} Monitoring](../devops/observability/monitoring.md)
- [Performance Diagnostics](performance/index.md)
