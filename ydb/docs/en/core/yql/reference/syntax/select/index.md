## SELECT

Returns the result of evaluating the expressions specified after `SELECT`.

May be used in combination with other operations to achieve a different effect.

### Examples


```yql
SELECT "Hello, world!";
```


```yql
SELECT 2 + 2;
```


## SELECT execution procedure {#selectexec}

The result of the `SELECT` query is computed as follows:

* The set of input tables is defined – expressions are evaluated after [FROM](../select/from.md)

{% if feature_match_recogznize==true %}

* [MATCH_RECOGNIZE](match_recognize.md) is applied to input tables

{% endif %}

* is calculated using [SAMPLE](sample.md) / [TABLESAMPLE](sample.md)
* The [FLATTEN COLUMNS](flatten.md#flatten-columns) or [FLATTEN BY](flatten.md) operation is performed. Aliases defined in `FLATTEN BY` become visible after this point.

{% if feature_join %}

* All [JOIN](join.md) are executed

{% endif %}

* Columns specified in [GROUP BY ... AS ...](group-by.md) are added to (or replaced in) the resulting data.
* The [WHERE](where.md) clause is executed: all data that does not satisfy the predicate is filtered out.
* [GROUP BY](group-by.md) is executed, aggregate function values are computed.
* Filtering is performed using [HAVING](group-by.md#having)

{% if feature_window_functions %}

* The values of [window functions](window.md) are computed

{% endif %}

* Expressions are evaluated in `SELECT`.
* Expressions in `SELECT` are assigned names defined by aliases.
* Top-level [DISTINCT](distinct.md) is applied to the columns obtained in this way.
* All subqueries in [UNION ALL](union.md#union-all) are evaluated in the same way, and their union is performed (see [PRAGMA AnsiOrderByLimitInUnionAll](../pragma.md#pragmas)).
* Sorting is performed according to [ORDER BY](order_by.md).
* [OFFSET and LIMIT](limit_offset.md) are applied to the obtained result.

## Column order in YQL {#orderedcolumns}

In standard SQL, the order of columns specified in the projection (in `SELECT`) matters. In addition to the column order needing to be preserved when displaying query results or when writing to a new table, some SQL constructs rely on this order.
This includes [UNION ALL](union.md#union-all) and positional [ORDER BY](order_by.md) (ORDER BY ordinal).

By default, YQL ignores column order:

* the order of columns in output tables and in query results is undefined
* The result data schema `UNION ALL` is output by column names, not by positions

When `PRAGMA OrderedColumns;` is enabled, the column order is preserved in the query results and derived from the column order in the input tables according to the following rules:

* `SELECT` with an explicit column list defines the corresponding order.
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits order from its input.

{% if feature_join %}

* Column order after [JOIN](join.md): first the columns of the left side, then the right side. If the order of any side present in the output `JOIN` is undefined, the order of the result columns is also undefined.

{% endif %}

* The order of `UNION ALL` depends on the execution mode of [UNION ALL](union.md#union-all).
* The column order for [AS_TABLE](from_as_table.md) is undefined.

## Query combination {#combining-queries}

The results of multiple SELECT (or subqueries) can be combined using the keywords `UNION` and `UNION ALL`.


```yql
query1 UNION [ALL] query2 (UNION [ALL] query3 ...)
```


Union of more than two queries is interpreted as a left-associative operation, that is


```yql
query1 UNION query2 UNION ALL query3
```


is interpreted as


```yql
(query1 UNION query2) UNION ALL query3
```


If `ORDER BY/LIMIT/DISCARD/INTO RESULT` is present in the combined subqueries, the following rules apply:

* `ORDER BY/LIMIT/INTO RESULT` is allowed only after the last subquery.
* `DISCARD` is allowed only before the first subquery.
* The specified operators act on the result `UNION [ALL]`, not on the subquery.
* To apply the operator to a subquery, the subquery must be enclosed in parentheses.

## Accessing multiple tables in a single query

In standard SQL, to execute a query over multiple tables you use [UNION ALL](union.md#union-all), which combines the results of two or more `SELECT`. This is not very convenient for a use case where you need to run the same query over multiple tables (for example, tables containing data for different dates). In YQL, for convenience, in `SELECT` after `FROM` you can specify not only a single table or subquery, but also invoke built-in functions that allow you to combine data from multiple tables.

The following functions are defined for these purposes:

``` CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...) ``` — combines all tables listed in the arguments.

`EACH($list_of_strings)` or `EACH($list_of_strings VIEW view_name)` combines all tables whose names are listed in a list of strings. Optionally, you can pass multiple lists in separate arguments, similar to `CONCAT`.

``` RANGE(`prefix`, `min`, `max`, `suffix`, `view`) ``` — combines a range of tables. Arguments:

* prefix — directory for locating tables, specified without a trailing slash. It is the only required argument; if it is the only one provided, all tables in that directory are used.
* min, max — the next two arguments define the inclusive range of names to include tables. The range is inclusive at both ends. If the range is not specified, all tables in the prefix directory are used. Table or directory names located in the directory specified by prefix are compared to the `[min, max]` range lexicographically, not concatenated, so it is important to specify the range without leading slashes.
* suffix — table name. It is expected without a leading slash. If suffix is not provided, the `[min, max]` arguments define the range of table names. If suffix is provided, the `[min, max]` arguments define the range of directories that contain a table with the name given in the suffix argument.

``` LIKE(`prefix`, `pattern`, `suffix`, `view`)` и `REGEXP(`prefix`, `pattern`, `suffix`, `view`) ``` — the pattern argument is specified in a format similar to the similarly named binary operators: [LIKE](../expressions.md#like) and [REGEXP](../expressions.md#regexp).

``` FILTER(`prefix`, `callable`, `suffix`, `view`) ``` — the callable argument must be an invocable expression with signature `(String)->Bool`, which will be called for each table/subdirectory in the prefix directory. Only those tables for which the invoked value returned `true` will participate in the query. The most convenient way to provide the invoked value is to use [lambda functions](../expressions.md#lambda){% if yql == true %}, or a UDF in Python or JavaScript{% endif %}.

{% note warning %}

The order in which tables are combined is not guaranteed by any of the above functions.

The list of tables is computed **before** the query execution. Therefore, tables created during the query will not appear in the function's results.

{% endnote %}

By default, the schemas of all participating tables are combined using [UNION ALL](union.md#union-all) rules. If schema merging is undesirable, you can use functions with the `_STRICT` suffix, such as `CONCAT_STRICT` or `RANGE_STRICT`, which work exactly like the originals but treat any discrepancy in table schemas as an error.

To specify the cluster of the tables being combined, you need to place it before the function name.

All arguments of the functions described above can be declared separately using [named expressions](../expressions.md#named-nodes). In this case, simple expressions are also allowed via an implicit call to [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

The original table name from which each row was initially obtained can be retrieved using the [TablePath()](../../builtins/basic.md#tablepath) function.

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
* WITH
* [WITHOUT](without.md)
* [WHERE](where.md)
* [ORDER BY](order_by.md)
* [ASSUME ORDER BY](assume_order_by.md)
* [LIMIT OFFSET](limit_offset.md)
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)

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
* [VIEW json_index](json_index.md)

{% endif %}
