# WITH

It's set after the data source in `FROM` and is used for additional hints for tables. You can't use hints for subqueries and [named expressions](../expressions.md#named-nodes).

The following values are supported:

* `INLINE`: Hints that the table contents is small and you need to use its in-memory view to process the query. The actual size of the table is not controlled in this case, and if it's large, the query might fail with an out-of-memory error.
* `UNORDERED`: Suppresses original table sorting.
* `SCHEMA` type: Hints that the specified table schema must be used entirely, ignoring the schema in the metadata.
* `COLUMNS` type: Hints that the specified types should be used for columns whose names match the table's column names in the metadata, as well as which columns are additionally present in the table.

When setting the `SCHEMA` and `COLUMNS` hints, the type must be a [structure](../../types/containers.md).

If you use the `SCHEMA` hint, then with the table functions like [EACH](concat.md#each) you can use an empty list of tables that is treated as an empty table with columns defined in the `SCHEMA`.

## Examples

```yql
SELECT key FROM my_table WITH INLINE;
```

```yql
SELECT key, value FROM my_table WITH SCHEMA Struct<key:String, value:Int32>;
```

```yql
SELECT key, value FROM my_table WITH COLUMNS Struct<value:Int32?>;
```

```yql
SELECT key, value FROM EACH($my_tables) WITH SCHEMA Struct<key:String, value:List<Int32>>;
```
