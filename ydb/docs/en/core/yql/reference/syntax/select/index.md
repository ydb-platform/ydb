## SELECT

Returns the result of evaluating the expressions specified after `SELECT`.

Can be used in combination with other operations to achieve a different effect.

### Examples


```yql
SELECT "Hello, world!";
```


```yql
SELECT 2 + 2;
```


## SELECT execution procedure {#selectexec}

The result of the `SELECT` query is computed as follows:

* the set of input tables is defined; expressions after [FROM](../select/from.md) are evaluated.

{% if feature_match_recogznize==true %}

* [MATCH_RECOGNIZE](match_recognize.md) is applied to input tables

{% endif %}

{% if feature_tablesample==true %}

* [SAMPLE](sample.md) / [TABLESAMPLE](sample.md) is evaluated

{% endif %}

* Either [FLATTEN COLUMNS](flatten.md#flatten-columns) or [](flatten.md) is executed. Aliases specified in `FLATTEN BY` become visible after this point.

{% if feature_join %}

* All [JOIN](join.md) are executed

{% endif %}

* columns specified in [GROUP BY ... AS ...](group-by.md) are added (or replaced) to the resulting data.
* executed [WHERE](where.md): all data that does not satisfy the predicate is filtered out
* [GROUP BY](group-by.md) is performed, aggregate function values are computed
* Filtering is performed using [HAVING](group-by.md#having).

{% if feature_window_functions %}

* Values of [window functions](window.md) are computed

{% endif %}

* Evaluate expressions in `SELECT`.
* Expressions in `SELECT` are assigned names specified by aliases.
* top-level [DISTINCT](distinct.md) is applied to the columns obtained in this way.
* All subqueries in [UNION ALL](union.md#union-all) are computed in the same way, and their union is performed (see [PRAGMA AnsiOrderByLimitInUnionAll](../pragma.md#pragmas))
* Sorting is performed according to [ORDER BY](order_by.md)
* the [OFFSET and LIMIT](limit_offset.md) are applied to the result.

## Column order in YQL {#orderedcolumns}

In standard SQL, the order of columns specified in the projection (in `SELECT`) matters. Besides the fact that the column order must be preserved when displaying query results or when writing to a new table, some SQL constructs use this order. This applies in particular to [UNION ALL](union.md#union-all) and positional [ORDER BY](order_by.md) (ORDER BY ordinal).

By default, column order is ignored in YQL:

* the order of columns in output tables and in query results is not defined
* the schema of the `UNION ALL` result is output by column names, not by positions

When `PRAGMA OrderedColumns;` is enabled, the column order is preserved in the query results and is derived from the column order in the input tables according to the following rules:

* `SELECT` with explicit column enumeration sets the corresponding order
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits the order from its input.

{% if feature_join %}

* the order of columns after [JOIN](join.md): first the columns from the left side, then from the right. If the order of either side present in the output `JOIN` is not defined, the order of the result columns is also not defined.

{% endif %}

* The order of depends on the execution mode of [`UNION ALL`](union.md#union-all)
* the order of columns for [AS_TABLE](from_as_table.md) is not defined.

## Combination of queries {#combining-queries}

Results of multiple SELECT (or subqueries) can be combined using the `UNION` and `UNION ALL` keywords.


```yql
query1 UNION [ALL] query2 (UNION [ALL] query3 ...)
```


Union of more than two queries is interpreted as a left-associative operation, i.e.


```yql
query1 UNION query2 UNION ALL query3
```


is interpreted as


```yql
(query1 UNION query2) UNION ALL query3
```


If `ORDER BY/LIMIT/DISCARD/INTO RESULT` is present in the combined subqueries, the following rules apply:

* `ORDER BY/LIMIT/INTO RESULT` is allowed only after the last subquery
* `DISCARD` is allowed only before the first subquery
* the specified operators act on the result `UNION [ALL]`, not on the subquery.
* to apply an operator to a subquery, the subquery must be enclosed in parentheses.

## Querying multiple tables in a single query

In standard SQL, [UNION ALL](union.md#union-all) is used to query multiple tables; it combines the results of two or more `SELECT`. This is not very convenient for a use case where you need to run the same query against multiple tables (for example, tables containing data for different dates). In YQL, to make it more convenient, in `SELECT` after `FROM` you can specify not only a single table or subquery, but also call built-in functions that allow you to combine data from multiple tables.

The following functions are defined for these purposes:

``` CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...) ``` — combines all tables listed in the arguments.

`EACH($list_of_strings)` or `EACH($list_of_strings VIEW view_name)` — combines all tables whose names are listed in a list of strings. Optionally, you can pass multiple lists in separate arguments, similar to `CONCAT`.

``` RANGE(`prefix`, `min`, `max`, `suffix`, `view`) ```: combines a range of tables. Arguments:

* prefix — directory for searching tables, specified without a trailing slash. The only required argument; if only it is specified, all tables in that directory are used.
* min, max — the next two arguments specify the range of names for including tables. The range is inclusive on both ends. If the range is not specified, all tables in the prefix directory are used. Names of tables or directories located in the directory specified in prefix are compared with the `[min, max]` range lexicographically, not concatenated, so it is important to specify the range without leading slashes.
* suffix — table name. Expected without a leading slash. If suffix is not specified, the `[min, max]` arguments specify the range of table names. If suffix is specified, the `[min, max]` arguments specify the range of folders in which a table with the name specified in the suffix argument exists.

``` LIKE(`prefix`, `pattern`, `suffix`, `view`)` и `REGEXP(`prefix`, `pattern`, `suffix`, `view`) ``` — the pattern argument is specified in a format similar to the binary operators of the same name: [LIKE](../expressions.md#like) and [REGEXP](../expressions.md#regexp).

``` FILTER(`prefix`, `callable`, `suffix`, `view`) ``` — the callable argument must be a callable expression with signature `(String)->Bool`, which will be called for each table/subdirectory in the prefix directory. Only those tables for which the callable returned `true` will be included in the query. As the callable, it is most convenient to use [lambda functions](../expressions.md#lambda){% if yql == true %}, or UDFs in [Python](../../udf/python.md) or [JavaScript](../../udf/javascript.md){% endif %}.

{% note warning %}

The order in which tables are combined by all the above functions is not guaranteed.

The list of tables is computed **before** the query itself is executed. Therefore, tables created during the query will not be included in the function results.

{% endnote %}

By default, schemas of all participating tables are merged according to [UNION ALL](union.md#union-all) rules. If schema merging is not desired, you can use functions with the `_STRICT` suffix, for example `CONCAT_STRICT` or `RANGE_STRICT`, which work exactly the same as the original ones but treat any discrepancy in table schemas as an error.

To specify the cluster of the tables being merged, you need to specify it before the function name.

All arguments of the functions described above can be declared separately using [named expressions](../expressions.md#named-nodes). In this case, simple expressions are also allowed in them via an implicit call to [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

The name of the source table from which each row was originally obtained can be obtained using the [TablePath()](../../builtins/basic.md#tablepath) function.

### Examples


```yql
SELECT * FROM CONCAT(
  `table1`,
  `table2`,
  `table3`);
```


```yql
$indices = ListFromRange(1, 4);
$tables = ListMap($indices, ($index) -> {
    RETURN "table" || CAST($index AS String);
});
SELECT * FROM EACH($tables); -- identical to the previous example
```


```yql
SELECT * FROM RANGE(`my_folder`);
```


```yql
SELECT * FROM some_cluster.RANGE( -- The cluster can be specified before the function name
  `my_folder`,
  `from_table`,
  `to_table`);
```


```yql
SELECT * FROM RANGE(
  `my_folder`,
  `from_folder`,
  `to_folder`,
  `my_table`);
```


```yql
SELECT * FROM RANGE(
  `my_folder`,
  `from_table`,
  `to_table`,
  ``,
  `my_view`);
```


```yql
SELECT * FROM LIKE(
  `my_folder`,
  "2017-03-%"
);
```


```yql
SELECT * FROM REGEXP(
  `my_folder`,
  "2017-03-1[2-4]?"
);
```


```yql
$callable = ($table_name) -> {
    return $table_name > "2017-03-13";
};

SELECT * FROM FILTER(
  `my_folder`,
  $callable
);
```


## Supported constructs in SELECT

* [FROM](from.md)
* [FROM AS_TABLE](from_as_table.md)
* [FROM SELECT](from_select.md)
* [DISTINCT](distinct.md)
* [UNIQUE DISTINCT](unique_distinct_hints.md)
* [UNION](union.md)
* [WITH](with.md)
* [WITHOUT](without.md)
* [WHERE](where.md)
* [ORDER BY](order_by.md)
* [ASSUME ORDER BY](assume_order_by.md)
* [LIMIT OFFSET](limit_offset.md)

{% if feature_tablesample==true %}

* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)

{% endif %}

{% if feature_match_recogznize==true %}

* [MATCH_RECOGNIZE](match_recognize.md)

{% endif %}

{% if feature_join %}

* [JOIN](join.md)

{% endif %}

* [GROUP BY](group-by.md)
* [FLATTEN](flatten.md)

{% if feature_window_functions %}

* [WINDOW](window.md)

{% endif %}

{% if yt %}

* [FOLDER](folder.md)
* [WalkFolders](walk_folders.md)

{% endif %}

{% if feature_mapreduce %}

* [VIEW](view.md)

{% endif %}

{% if feature_temp_table %}

* [TEMPORARY TABLE](temporary_table.md)

{% endif %}

{% if feature_bulk_tables %}

* [CONCAT](concat.md)

{% endif %}

{% if feature_secondary_index %}

* [VIEW secondary_index](secondary_index.md)
* [VIEW vector_index](vector_index.md)
* [VIEW fulltext_index](fulltext_index.md)

{% endif %}
