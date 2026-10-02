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

## `DebugPositions`

Disable deduplication of expression evaluation that may lead to errors (`Ensure`/`Unwrap` or UDF call).
This may result in slower query at the cost of increasing the precision of the position that will be tied to a runtime error.

## `UdfBridge`

Enables UDF execution in a separate process.
This may slow down the request, but allows you to isolate crashes/protocol violations in UDF calls down to a single library or narrow them down to a problem within the host process.
