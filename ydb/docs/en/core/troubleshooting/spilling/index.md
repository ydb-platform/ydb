# Spilling Troubleshooting

This section provides troubleshooting information for common spilling issues in {{ ydb-short-name }}. Spilling is a memory management mechanism that temporarily saves intermediate computation data to disk when the system runs out of RAM. These errors can occur during query execution when the system attempts to use spilling functionality and can be observed in logs and query responses.

## Common Issues

- [Permission denied](permission-denied.md) - Insufficient access permissions to the spilling directory
- [Spilling Service not started](service-not-started.md) - Attempt to use spilling when the Spilling Service is disabled
- [Total size limit exceeded](total-size-limit-exceeded.md) - Maximum total size of spilling files exceeded
- [Can not run operation](can-not-run-operation.md) - I/O thread pool operation queue overflow

## See Also

- [Spilling Configuration](../../reference/configuration/table_service_config.md)
- [Spilling Concept](../../concepts/spilling.md)
- [Memory Controller Configuration](../../reference/configuration/memory_controller_config.md)
- [{{ ydb-short-name }} Monitoring](../../devops/observability/monitoring.md)
- [Performance Diagnostics](../performance/index.md)
