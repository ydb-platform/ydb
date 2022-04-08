## Accessing multiple tables in a single query {#concat} {#each} {#range} {#like} {#filter} {#regexp}

Standard SQL uses [UNION ALL](#unionall) to execute a query across multiple tables and combine the results of two or more `SELECT` statements. This is not very convenient for the use case of running the same query on multiple tables (for example, if they contain data for different dates). To make this more convenient, in YQL `SELECT` statements, after `FROM`, you can specify not only a table or a subquery, but also call built-in functions letting you combine data from multiple tables.

The following functions are defined for these purposes:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` combines all the tables listed in the arguments.

```EACH($list_of_strings) or EACH($list_of_strings VIEW view_name)``` combines all tables whose names are listed in the list of strings. Optionally, you can pass multiple lists in separate arguments similar to `CONCAT`.

```RANGE(`prefix`, `min`, `max`, `suffix`, `view`)``` combines a range of tables. Arguments:

* The prefix is the folder to search tables in. It's specified without a trailing slash. This is the only required argument. If it's the only one specified, all tables in the folder are used.
* min, max: These two arguments specify a range of table names included. The range is inclusive on both ends. If no range is specified, all tables in the prefix folder are used. The names of tables or folders located in the folder specified in prefix are lexicographically matched with the `[min, max]` range rather than concatenated, so make sure to specify the range without leading slashes.
* suffix is the name of the table. It's expected without a leading slash. If no suffix is specified, then the `[min, max]` arguments specify a range of table names. If the suffix is specified, then the `[min, max]` arguments specify a range of folders hosting a table with the name specified in the suffix argument.

```LIKE(`prefix`, `pattern`, `suffix`, `view`)``` and ```REGEXP(`prefix`, `pattern`, `suffix`, `view`)```: The pattern argument is specified in the format similar to the binary operators of the same name: [LIKE](../../expressions.md#like) and [REGEXP](../../expressions.md#regexp).

```FILTER(`prefix`, `callable`, `suffix`, `view`)```: The `callable` argument must be a callable expression with the `(String)->Bool` signature that will be called for each table/subdirectory in the prefix folder. The query will only include those tables for which the callable value returned `true`. It is most convenient to use, as callable values, [lambda functions](../../expressions.md#lambda){% if yql == true %} or user-defined functions (UDF) written in [Python](../../../udf/python.md) or [JavaScript](../../../udf/javascript.md){% endif %}.

{% note warning %}

All of the above functions don't guarantee the order of the table union.

The list of tables is calculated **before** executing the query. Therefore, the tables created during the query execution won't be included in the function results.

{% endnote %}

By default, the schemas of all the unioned tables are combined using the [UNION ALL](#unionall) rules. If you don't want to combine schemas, then you can use functions with the `_STRICT` suffix, for example `CONCAT_STRICT` or `RANGE_STRICT` that are totally similar to the original functions, but treat any difference in table schemas as an error.

To specify a cluster of unioned tables, add it before the function name.

All arguments of the functions described above can be declared separately using [named expressions](../../expressions.md#named-nodes). In this case, you can also use  simple expressions in them by implicitly calling [EvaluateExpr](../../../builtins/basic.md#evaluate_expr_atom).

To get the name of the source table from which you originally obtained each row, use [TablePath()](../../../builtins/basic.md#tablepath).

**Examples:**

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

```yql
USE some_cluster;
SELECT * FROM RANGE(`my_folder`);
```

```yql
SELECT * FROM some_cluster.RANGE( -- You can specify the cluster before the function name
  `my_folder`,
  `from_table`,
  `to_table`);
```

```yql
USE some_cluster;
SELECT * FROM RANGE(
  `my_folder`,
  `from_folder`,
  `to_folder`,
  `my_table`);
```

```yql
USE some_cluster;
SELECT * FROM RANGE(
  `my_folder`,
  `from_table`,
  `to_table`,
  ``,
  `my_view`);
```

```yql
USE some_cluster;
SELECT * FROM LIKE(
  `my_folder`,
  "2017-03-%"
);
```

```yql
USE some_cluster;
SELECT * FROM REGEXP(
  `my_folder`,
  "2017-03-1[2-4]?"
);
```

```yql
$callable = ($table_name) -> {
    return $table_name > "2017-03-13";
};

USE some_cluster;
SELECT * FROM FILTER(
  `my_folder`,
  $callable
);
```

