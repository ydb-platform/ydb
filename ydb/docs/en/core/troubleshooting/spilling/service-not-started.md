# Spilling Service Not Started

Spilling is disabled, but the system ran out of memory during query execution. When spilling is disabled, {{ ydb-short-name }} cannot offload intermediate data to disk, which leads to query failure when available memory is exhausted.

## Diagnostics

Check the spilling service configuration:

- Verify that [`table_service_config.spilling_service_config.local_file_config.enable`](../../reference/configuration/table_service_config.md#local-file-config-enable) is set to `true`.

## Recommendations

To fix this error, enable spilling:

1. Set [`table_service_config.spilling_service_config.local_file_config.enable`](../../reference/configuration/table_service_config.md#local-file-config-enable): `true` in your configuration.

For more details on enabling spilling, see the [Enable](../../reference/configuration/table_service_config.md#enable) section.

{% note info %}

Read more about the spilling architecture in [Spilling in {{ ydb-short-name }}](../../concepts/query_execution/spilling.md#spilling-in-ydb-short-name).

{% endnote %}
