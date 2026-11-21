# FROM AS_TABLE

Accessing named expressions as tables using the `AS_TABLE` function.

`AS_TABLE($variable)` allows using the value of $variable as the data source for a query. $variable must be of type `List<Struct<...>>`, `Optional<Struct<...>>`, or a lambda with no arguments and returning a `Stream<Struct<...>>`.

If `$variable` is of type `Optional<List<Struct<...>>>`, the query will fail: `Optional` is converted to a list of zero or one element in this context, and its element type remains `List<Struct<...>>` rather than the required `Struct<...>`. You can cast `Optional<List<Struct<...>>>` to `List<Struct<...>>`, for example, using [Coalesce](../../builtins/basic.md#coalesce) or [Unwrap](../../builtins/basic.md#unwrap) functions.

## Example


```yql
$data = AsList(
    AsStruct(1u AS Key, "v1" AS Value),
    AsStruct(2u AS Key, "v2" AS Value),
    AsStruct(3u AS Key, "v3" AS Value));

SELECT Key, Value FROM AS_TABLE($data);
```


You should either explicitly specify the modifiable columns in both the source and the target when using expressions with modifying queries such as [UPSERT INTO](../upsert_into.md) or [INSERT INTO](../insert_into.md):


```yql
$data = AsList(
    AsStruct(1u AS Key, "v1" AS Value),
    AsStruct(2u AS Key, "v2" AS Value),
    AsStruct(3u AS Key, "v3" AS Value));

INSERT INTO `my_table` (Key, Value) SELECT Key, Value FROM AS_TABLE($data);
```


Or you should omit them completely:


```yql
$data = AsList(
    AsStruct(1u AS Key, "v1" AS Value),
    AsStruct(2u AS Key, "v2" AS Value),
    AsStruct(3u AS Key, "v3" AS Value));

INSERT INTO `my_table` SELECT * FROM AS_TABLE($data);
```
