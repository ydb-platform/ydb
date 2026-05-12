## FROM AS_TABLE {#as-table}

Accessing named expressions as tables using the `AS_TABLE` function.

`AS_TABLE($variable)` allows using the value of $variable as the data source for a query. $variable must be of type `List<Struct<...>>`, `Optional<Struct<...>>`, or a lambda with no arguments and returning a `Stream<Struct<...>>`.

If `$variable` is of type `Optional<List<Struct<...>>>`, the query will fail because `Optional` is treated as a list of length 1 in this context. You can cast `Optional<List<Struct<...>>>` to `List<Struct<...>>`, for example, using [Coalesce](../../builtins/basic.md#coalesce) or [Unwrap](../../builtins/basic.md#unwrap) functions.

#### Example

```yql
$data = AsList(
    AsStruct(1u AS Key, "v1" AS Value),
    AsStruct(2u AS Key, "v2" AS Value),
    AsStruct(3u AS Key, "v3" AS Value));

SELECT Key, Value FROM AS_TABLE($data);
```

