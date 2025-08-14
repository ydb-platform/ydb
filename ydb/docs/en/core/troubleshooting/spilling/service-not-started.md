# Spilling Service Not Started

An attempt to use spilling occurs when the Spilling Service is disabled. This happens when the spilling service is not properly configured or has been disabled in the configuration.

## Diagnostics

Check the spilling service configuration:

- Verify that `table_service_config.spilling_service_config.local_file_config.enable` is set to `true`.
- Review the {{ ydb-short-name }} logs for spilling service startup errors.

## Recommendations

To enable spilling:

1. Set `table_service_config.spilling_service_config.local_file_config.enable: true` in your configuration.

{% note info %}

Read more about the spilling architecture in [Spilling Architecture in {{ ydb-short-name }}](../../concepts/spilling.md#spilling-architecture-in-ydb).

{% endnote %}
