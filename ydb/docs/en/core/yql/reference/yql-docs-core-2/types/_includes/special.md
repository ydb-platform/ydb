# Special data types

| Type | Description |
| ----- | ----- |
| `Callable` | A callable value that can be executed by passing arguments in parentheses in YQL SQL syntax{% if feature_mapreduce %} or using the `Apply` function with the [s-expressions](/docs/s_expressions) syntax{% endif %}. |
| `Resource` | Resource is an opaque pointer to a resource you can pass between the user defined functions (UDF). The type of the returned and accepted resource is declared inside a function using a string label. When passing a resource, YQL checks for label matching to prevent passing of resources between incompatible functions. If the labels mismatch, a type error occurs. |
| `Tagged` | Tagged is the option to assign an application name to any other type. |
| `Generic` | The data type used for data types. |
| `Unit` | Unit is the data type used for non-enumerable entities (data sources and data sinks, atoms, etc.&nbsp;). |
| `Null` | Void is a singular data type with the only possible null value. It's the type of the `NULL` literal and can be converted to any `Optional` type. |
| `Void` | Void is a singular data type with the only possible ` "null" value`. |
| `EmptyList` | A singular data type with the only possible [] value. It's the type of the `[]` literal and can be converted to any `List` type. |
| `EmptyDict` | A singular data type with the only possible {} value. It's a type of the `{}` literal and can be converted to any `Dict` or `Set` type. |

