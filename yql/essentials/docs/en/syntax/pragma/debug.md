# Debugging and service settings {#debug}

## `config.flags("ValidateUdf", "Lazy")`

| Value type | Default | Allowed in project settings |
| --- | --- | --- |
| String: None/Lazy/Greedy | None | Yes |

Validating whether UDF results match the declared signature. The Greedy mode enforces materialization of lazy containers, although the Lazy mode doesn't.

## `config.flags("Diagnostics")`

| Value type | Default | Allowed in project settings |
| --- | --- | --- |
| Flag | false | No |

Getting diagnostic information from YQL as an additional result of a query.

## `DebugPositions`

| Allowed in project settings |
| --- |
| No |

Disable deduplication of expression evaluation that may lead to errors (`Ensure`/`Unwrap` or UDF call).
This may result in slower query at the cost of increasing the precision of the position that will be tied to a runtime error.

## `UdfBridge`

| Allowed in project settings |
| --- |
| No |

Enables UDF execution in a separate process.
This may slow down the request, but allows you to isolate crashes/protocol violations in UDF calls down to a single library or narrow them down to a problem within the host process.

## `config.flags("LLVM")`

| Arguments | Allowed in project settings |
| --- | --- |
| Optional LLVM settings string | Yes |

Sets LLVM compilation options for computations. `"OFF"` disables LLVM; omitting the second argument supplies an empty options string.

## `config.flags("LLVM_OFF")`

| Arguments | Allowed in project settings |
| --- | --- |
| No additional arguments | Yes |

Disables LLVM compilation. Equivalent to `PRAGMA config.flags("LLVM", "OFF");`.

## `config.flags("UdfSupportsYield")`

| Arguments | Allowed in project settings |
| --- | --- |
| Optional `"true"` or `"false"` | Yes |

Indicates that scripting UDFs support handling the `yield` signal in stream arguments. Omitting the second argument enables support.

