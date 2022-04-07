# DECLARE

Declares a typed [named expression](../../expressions.md#named-nodes) whose value will be passed separately from the query text. With parameterization, you can separately develop an analytical solution and then launch it sequentially with different input values.

In the case of transactional load, parameters let you avoid recompilation of queries when repeating calls of the same type. This way you can reduce server utilization and exclude compilation time from the total time of query execution.

Passing of parameters is supported in the SDK, CLI, and graphical interfaces.

**Syntax**

```sql
DECLARE $named-node AS data_type;
```

1. `DECLARE` keyword.
1. `$named-node`: The name by which you can access the passed value. It must start with `$`.
1. `AS` keyword.
1. `data_type` is the data type [represented as a string in the accepted format](../../../types/type_string.md).

Only serializable data types are allowed:

* [Primitive types](../../../types/primitive.md).
* [Optional types](../../../types/optional.md).
* [Containers](../../../types/containers.md), except `Stream<Type>`.
* `Void` and `Null` are the supported [special types](../../../types/special.md).

**Example**

```yql
DECLARE $x AS String;
DECLARE $y AS String?;
DECLARE $z AS List<String>;

SELECT $x, $y, $z;
```

