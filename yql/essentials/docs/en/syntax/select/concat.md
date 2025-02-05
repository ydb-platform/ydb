## Accessing multiple tables in a single query {#concat} {#each} {#range} {#like} {#filter} {#regexp}

Standard SQL uses [UNION ALL](union.md#union-all) to execute a query across multiple tables and combine the results of two or more `SELECT` statements. This is not very convenient for the use case of running the same query on multiple tables (for example, if they contain data for different dates). To make this more convenient, in YQL `SELECT` statements, after `FROM`, you can specify not only a table or a subquery, but also call built-in functions letting you combine data from multiple tables.

The following functions are defined for these purposes:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` combines all the tables listed in the arguments.

```EACH($list_of_strings) or EACH($list_of_strings VIEW view_name)``` combines all tables whose names are listed in the list of strings. Optionally, you can pass multiple lists in separate arguments similar to `CONCAT`.

{% note warning %}

All of the above functions don't guarantee the order of the table union.

The list of tables is calculated **before** executing the query. Therefore, the tables created during the query execution won't be included in the function results.

{% endnote %}

By default, the schemas of all the unioned tables are combined using the [UNION ALL](union.md#union-all) rules. If you don't want to combine schemas, then you can use functions with the `_STRICT` suffix, for example `CONCAT_STRICT`, that are totally similar to the original functions, but treat any difference in table schemas as an error.

To specify a cluster of unioned tables, add it before the function name.

All arguments of the functions described above can be declared separately using [named expressions](../expressions.md#named-nodes). In this case, you can also use  simple expressions in them by implicitly calling [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

To get the name of the source table from which you originally obtained each row, use [TablePath()](../../builtins/basic.md#tablepath).

### Examples

```yql
USE some_cluster;
SELECT * FROM CONCAT(
  `table1`,
  `table2`,
  `table3`);
```

```yql
USE some_cluster;
$indices = ListFromRange(1, 4);
$tables = ListMap($indices, ($index) -> {
    RETURN "table" || CAST($index AS String);
});
SELECT * FROM EACH($tables); -- Identical to the previous example
```
