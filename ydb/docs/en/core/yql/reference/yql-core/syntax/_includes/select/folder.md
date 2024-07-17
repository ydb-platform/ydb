## Listing the contents of a directory in a cluster {#folder}

It's expressed as the `FOLDER` function in [FROM](../../select/from.md).

Arguments:

1. The path to the directory.
2. An optional string with a list of meta attributes separated by a semicolon.

The result is a table with three fixed columns:

1. **Path** (`String`): The full name of the table.
2. **Type** (`String`): The node type (table, map_node, file, document, and others).
3. **Attributes** (`Yson`) is a Yson dictionary with meta attributes ordered in the second argument.

Recommendations for use:

* To get only a list of tables, make sure to add `...WHERE Type == "table"`. Then, by optionally adding more conditions, you can use the aggregate function [AGGREGATE_LIST](../../../builtins/aggregation.md#aggregate-list) from the Path column to get only a list of paths and pass them to [EACH](#each).
* Since the Path column is output in the same format as the result of the function [TablePath()](../../../builtins/basic.md#tablepath), then they can be used to JOIN table attributes to its rows.
* We recommend that you work with the Attributes column using [Yson UDF](../../../udf/list/yson.md).

{% note warning %}

Use FOLDER with attributes containing large values with caution (`schema` could be one of those). A query with FOLDER applied to a folder with a large number of tables and a heavy attribute can create a heavy load on the YT wizard.

{% endnote %}

**Examples:**

```yql
USE hahn;

$table_paths = (
    SELECT AGGREGATE_LIST(Path)
    FROM FOLDER("my_folder", "schema;row_count")
    WHERE
        Type = "table" AND
        Yson::GetLength(Attributes.schema) > 0 AND
        Yson::LookupInt64(Attributes, "row_count") > 0
);

SELECT COUNT(*) FROM EACH($table_paths);
```

