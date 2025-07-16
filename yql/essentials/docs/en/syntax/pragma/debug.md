# Debugging and service settings {#debug}

## `config.flags("ValidateUdf", "Lazy")`

| Value type | Default |
| --- | --- |
| String: None/Lazy/Greedy | None |

Validating whether UDF results match the declared signature. The Greedy mode enforces materialization of lazy containers, although the Lazy mode doesn't.

## `config.flags("Diagnostics")`

| Value type | Default |
| --- | --- |
| Flag | false |

Getting diagnostic information from YQL as an additional result of a query.

