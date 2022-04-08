{% if tech %}

## Debugging and auxiliary settings {#debug}

{% if feature_webui %}

### `DirectRead`

| Value type | Default |
| --- | --- |
| Flag | false |

An auxiliary setting for previewing tables in the [HTTP API](../../../interfaces/http.md) (both for the web interface and console client).
{% endif %}

### `config.flags("ValidateUdf", "Lazy")`

| Value type | Default |
| --- | --- |
| String: None/Lazy/Greedy | None |

Validating whether UDF results match the declared signature. The Greedy mode enforces materialization of lazy containers, although the Lazy mode doesn't.

### `{{ backend_name_lower }}.DefaultCluster`

| Value type | Default |
| --- | --- |
| A string with the cluster name | hahn |

Selecting a cluster for calculations that don't access tables.

### `config.flags("Diagnostics")`

| Value type | Default |
| --- | --- |
| Flag | false |

Getting diagnostic information from YQL as an additional result of a query.

{% endif %}

