{% if backend_name != "YDB" %}

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

{% endif %}

{% if backend_name == "YDB" %}

# SELECT

The `SELECT` statement retrieves data from [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) and [column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables) tables, [external tables](../../../../concepts/datamodel/external_table.md) and [data sources](../../../../concepts/datamodel/external_data_source.md), [views](../../../../concepts/datamodel/view.md), and [topics](topics.md). By default, the retrieved data is returned to the user, but `SELECT` can also be part of more complex constructs, such as `UPSERT INTO ... SELECT`, `CREATE TABLE ... AS SELECT ...`, etc.

A `SELECT` query can contain a single selection or multiple selections whose results are combined using `UNION`, `EXCEPT`, and `INTERSECT` operations. Below are the syntax forms for both query types, followed by details of their constituent parts.

## Syntax {#query-syntax}

### Single query {#single-select-overview}

For a query with a single `SELECT`, the general form is as follows:

```yql
[DISCARD]
SELECT [ALL | DISTINCT] <result_list>
[WITHOUT [IF EXISTS] <column_name> [, ...]]
[FROM <sources_and_joins>]
[WHERE <condition>]
[GROUP [COMPACT] BY [ALL] <grouping_element> [, ...]]
[HAVING <condition>]
[WINDOW <window_name> AS (<window_specification>) [, ...]]
[[ASSUME] ORDER BY <sort_key> [ASC | DESC] [, ...]]
[INTO RESULT <result_name>]
[LIMIT <row_count> [OFFSET <offset>]];
```

Only `SELECT` and `<result_list>` are required. Other clauses are added as needed, subject to the following rules:

| Query part | Purpose |
| --- | --- |
| `SELECT <result_list>` | Computes the expressions that form the output columns. The list can contain columns, function calls, expressions, and asterisks. |
| `ALL` / [`DISTINCT`](distinct.md) | `ALL` preserves duplicate rows and is the default. `DISTINCT` keeps only distinct result rows. |
| [`WITHOUT`](without.md) | Excludes the specified columns from the asterisk result. `IF EXISTS` allows the excluded column to be absent. |
| [`FROM`](from.md) | Specifies [row sources](#select-sources-overview), their [transformations](#select-flatten-overview), and [joins](#select-joins-overview). Without `FROM`, you can compute expressions, e.g., `SELECT 2 + 2`, but you cannot reference input columns, filter, or group them. |
| [`WHERE`](where.md) | Keeps input rows for which the condition is true. |
| [`GROUP BY`](group-by.md) | Groups rows by [keys and grouping sets](#select-grouping-overview) to compute aggregate functions. |
| [`HAVING`](group-by.md#having) | Filters aggregation results. |
| [`WINDOW`](window.md) | Declares [named windows](#select-windows-overview) for window function calls via `OVER`. |
| [`ORDER BY`](order_by.md) / [`ASSUME ORDER BY`](assume_order_by.md) | `ORDER BY` sorts the result; `ASSUME ORDER BY` declares an existing sort order without performing it. `ASC` is the default direction, `DESC` specifies reverse order. For `ORDER BY`, `<sort_key>` can be a column or expression. For `ASSUME ORDER BY`, output column names are specified. |
| [`INTO RESULT`](../into_result.md) | Sets the name of the result set. The name can be an identifier, including one in backticks, or a named expression. |
| [`LIMIT` / `OFFSET`](limit_offset.md) | Skips `<offset>` rows and returns at most `<row_count>` subsequent rows. `OFFSET` is specified together with `LIMIT`. `<row_count>` and `<offset>` are expressions. `NULL` in `LIMIT` removes the row count limit. Without `ORDER BY`, the set of selected rows is undefined. |
| [`DISCARD`](../discard.md) | Executes the query but does not return its result to the client. In a single `SELECT`, it cannot be combined with `INTO RESULT`. |

### Compound query {#compound-select-overview}

The `UNION`, `EXCEPT`, and `INTERSECT` operations take the results of individual selections. Each selection has its own sources, filters, grouping, and windows; sorting and limiting the overall result can be specified after the last selection:

```yql
[DISCARD]
SELECT ...
{ UNION | EXCEPT | INTERSECT } [ALL | DISTINCT]
SELECT ...
[...]
[[ASSUME] ORDER BY <sort_key> [ASC | DESC] [, ...]]
[INTO RESULT <result_name>]
[LIMIT <row_count> [OFFSET <offset>]];
```

Here, each `SELECT ...` is a part of the [single query schema](#single-select-overview) from `SELECT` through `WINDOW` inclusive. Common trailing clauses are written at the end of the last `SELECT` without parentheses but apply to the result of the entire combination.

| Operation | Result |
| --- | --- |
| `UNION` | Rows that appear in at least one of the two selections. |
| `EXCEPT` | Rows of the left selection that are absent from the right one. |
| `INTERSECT` | Rows that appear in both selections. |

By default, these operations remove duplicates, which corresponds to `DISTINCT`. The `ALL` qualifier accounts for the number of occurrences: `UNION ALL` keeps all rows from both selections, `EXCEPT ALL` subtracts the number of occurrences on the right from the number on the left, but not below zero, and `INTERSECT ALL` keeps the smaller of the two occurrence counts. `UNION DISTINCT`, `EXCEPT`, and `INTERSECT` are available starting from YQL language version `2025.03`.

`INTERSECT` has higher precedence than `UNION` and `EXCEPT`; operations of equal precedence are grouped left to right. The rules for matching columns in `UNION` are described in the section [Combining subquery results](union.md).

`DISCARD` is placed before the first selection and applies to the entire statement. `INTO RESULT` sets the name of the overall result. For actual sorting of the combination result, use `ORDER BY`: the operations on selections do not guarantee row order by themselves.

For example, the following query returns at most the first ten rows after sorting the overall result:

```yql
SELECT key FROM first_table
UNION ALL
SELECT key FROM second_table
ORDER BY key
LIMIT 10;
```

To apply sorting and limiting to only one selection, enclose that selection in parentheses. In the following query, at most ten rows are taken from the first table, and all rows from the second:

```yql
(SELECT key FROM first_table ORDER BY key LIMIT 10)
UNION ALL
SELECT key FROM second_table;
```

### Result list {#select-list-overview}

Instead of `<result_list>`, expressions and qualified asterisks are specified separated by commas, or a single regular asterisk:

| Element | Purpose |
| --- | --- |
| [`<expression> [[AS] <alias>]`](../expressions.md) | Computes an output column; the alias sets its name. The expression can contain calls to regular, aggregate, and window functions. |
| `<source_alias>.*` | Selects all columns of the specified source; allowed together with other list elements. |
| `*` | Selects all columns. A regular asterisk cannot be mixed with other list elements. |

The `WITHOUT` clause is placed immediately after the result list. A column in it can be specified as `<column_name>` or `<source_alias>.<column_name>`.

Hints about result properties are specified in comments after `SELECT` and are described in the section [UNIQUE and DISTINCT](unique_distinct_hints.md).

### Data sources {#select-sources-overview}

In the [`FROM`](from.md) clause, you can specify a single source or join multiple sources using `JOIN`. The main forms of a single source:

| Form | Purpose |
| --- | --- |
| `[<external_data_source_name>.]<table_name> [VIEW <index_name>]` | Reads a table, optionally via the [specified index](secondary_index.md). The table path can be enclosed in backticks. |
| `[<external_data_source_name>.]<topic_name>` | Reads a [local or external topic](topics.md). To wait for new messages, use `WITH (STREAMING = "TRUE")`; the number of returned messages is limited by `LIMIT`. |
| `(SELECT ...)` | The result of a subquery, including a compound one. Parentheses are required. |
| `$<name>` | A source specified by a named expression: for example, the result of a subquery. |
| [`AS_TABLE(<expression>)`](from_as_table.md) | A list of structures converted into a table source. |
| `(VALUES (<expression> [, ...]) [, ...])` | Rows specified directly in the query. All rows must have the same number of values. |

A source can be assigned an alias: `[AS] <alias>`. For `VALUES`, a list of column names is additionally allowed after the alias: `AS <alias> (<column_name> [, ...])`.

### Source settings (`WITH`) {#select-with-overview}

The [`WITH`](with.md) construct specifies source read settings. The available parameters and their value forms depend on the source type; for example, you can specify the data storage format for external sources, a schema for topics and external sources, and `WATERMARK` for streaming queries. Settings are specified before the alias:

```yql
[<external_data_source_name>.]<table_name> [VIEW <index_name>]
[WITH (<table_hint> [, ...])]
[[AS] <alias>]
```

Each `<table_hint>` contains a parameter name and, if required, a value in the form defined for that setting. For a single setting, parentheses after `WITH` are optional. For example, `FORMAT = <format>` specifies the data format, and `SCHEMA <struct_type>` specifies the schema when reading files from an external source or messages from a [topic](topics.md), including a local one.

### Data transformation (`FLATTEN`) {#select-flatten-overview}

After a source and its alias, you can specify one [`FLATTEN`](flatten.md) transformation:

```yql
FLATTEN COLUMNS
```

or one of the `FLATTEN BY` forms:

```yql
FLATTEN [OPTIONAL | LIST | DICT] BY <column_name> [AS <alias>]
FLATTEN [OPTIONAL | LIST | DICT] BY (<column_name> [AS <alias>] [, ...])
FLATTEN [OPTIONAL | LIST | DICT] BY (<expression> AS <alias> [, ...])
```

`FLATTEN COLUMNS` expands structure fields into separate columns, while `FLATTEN BY` expands container elements into rows. The list in parentheses can contain both columns and expressions. For an arbitrary expression, an alias is required.

### Source joins {#select-joins-overview}

Instead of `<sources_and_joins>` in the main schema, you can specify a source and a sequence of joins. When joining sources, `WITH`, the alias, and `FLATTEN` apply to the source after which they are written. Each [join with a condition](join.md) has the form:

```yql
FROM [ANY] <source>
    [<join_type>] JOIN [ANY] <source>
        { ON <join_condition> | USING (<column_name> [, ...]) }
    [...]
```

The following options are available for `<join_type>`:

| Type | Purpose |
| --- | --- |
| `INNER` | Only matched rows from both sides; used by default. |
| `LEFT [OUTER]` / `RIGHT [OUTER]` | All rows from the specified side and matched rows from the other; if there is no match, the columns of the other side are filled with `NULL`. |
| `FULL [OUTER]` | All rows from both sides with matched rows combined. |
| `LEFT SEMI` / `RIGHT SEMI` | Only columns from the specified side and only rows that have a match with the other side. |
| `LEFT ONLY` / `RIGHT ONLY` | Only columns from the specified side and only rows without a match with the other side. |
| `EXCLUSION` | Rows from both sides that have no match with the other side. |

`ON` specifies equality of key expressions, while `USING` lists same-named key columns. `ANY` before a source keeps one row for each set of join keys from that side; which particular row remains is undefined.

A Cartesian product is written separately, without `ON`, `USING`, or `ANY`:

```yql
FROM <source>
    CROSS JOIN <source>
```

Joins of different types can be interleaved by adding the next join after the previous one. `ANY` without a join is not allowed. A comma join requires `PRAGMA AnsiImplicitCrossJoin;` and corresponds to `CROSS JOIN`.

### Grouping keys and sets {#select-grouping-overview}

The [`GROUP BY`](group-by.md) clause contains one or more grouping elements separated by commas. The following are used as `<grouping_element>`:

| Form | Purpose |
| --- | --- |
| `<expression> [AS <alias>]` | A grouping key: a column or the result of an expression. An alias allows referencing the computed key by name. |
| `()` | An empty key set for a grand total. |
| `(<expression> [, ...])` | A key set, for example inside `GROUPING SETS`. |
| [`ROLLUP(<expression> [, ...])`](group-by.md#rollup) | Totals for successive key sets and a grand total. |
| [`CUBE(<expression> [, ...])`](group-by.md#rollup) | Totals for all key combinations. |
| [`GROUPING SETS(<grouping_set> [, ...])`](group-by.md#rollup) | Explicitly specified key sets for computing totals. |
| [`SessionWindow(<time_expression>, <session_timeout>) AS <alias>`](group-by.md#session-window) | Groups events into sessions. |
| [`HoppingWindow(<time_expression>, <hop>, <interval>)`](group-by.md#group-by-hopping_window) | Groups by overlapping time windows. |
| [`HOP(<time_expression>, <hop>, <interval>, <delay>)`](group-by.md#group-by-hop) | Groups by time windows with a delay parameter. |

`<grouping_set>` specifies a key, a key set in parentheses, an empty set, or a nested grouping construct. `<time_expression>` is an expression for the event time; `<session_timeout>` sets the session boundary, `<hop>` and `<interval>` are the shift period and window duration, and `<delay>` is the delay of its closing in streaming mode.

`COMPACT` selects an aggregation strategy for a small amount of data per key. `ALL` in `GROUP BY` can be omitted; `GROUP BY DISTINCT` is not supported. The `HAVING` clause follows `GROUP BY` and allows filtering groups by aggregate function results.

### Window definitions {#select-windows-overview}

A [window function](window.md) computes a value over rows related to the current row while preserving individual result rows. A window definition can be specified directly after `OVER` or declared in the `WINDOW` clause and used by name:

```yql
<window_function>([<argument> [, ...]]) OVER (<window_specification>)
<window_function>([<argument> [, ...]]) OVER <window_name>
```

The `WINDOW` clause with a `ROWS` frame has the form:

```yql
WINDOW <window_name> AS (
    [PARTITION [COMPACT] BY <expression> [AS <alias>] [, ...]]
    [ORDER BY <sort_key> [ASC | DESC] [, ...]]
    [ROWS { <frame_bound> | BETWEEN <frame_start> AND <frame_end> }]
) [, ...]
```

| Definition part | Purpose |
| --- | --- |
| [`PARTITION BY`](window.md#partition) | Divides rows into independent partitions. Without `PARTITION BY`, all input rows form a single partition. The key can be a column or expression; an alias can be assigned to an expression. |
| [`ORDER BY`](window.md#partition) | Sets the row order within a partition. `ASC` is the default direction, `DESC` specifies reverse order. To order the query result, a separate `ORDER BY` is needed after the `WINDOW` clause. |
| [`ROWS`](window.md#frame) | Sets a frame by row offsets relative to the current row. Aggregate functions, as well as `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`, are computed over the frame. The functions `ROW_NUMBER`, `LAG`, and `LEAD` operate over the entire partition regardless of the frame. |
| Default frame | In the Query Service, the [`AnsiCurrentRow`](../pragma.md#ansicurrentrow) pragma is enabled by default. If no frame is specified, then, when a window `ORDER BY` is present, `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is used: it includes all rows from the start of the partition up to the last row with the same sort key values as the current one. When the pragma is disabled, `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is used. Without a window `ORDER BY`, `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` is used. To get the last value of the entire partition via `LAST_VALUE`, explicitly set the frame end to `UNBOUNDED FOLLOWING`. |
| [`COMPACT`](window.md) | Selects in-memory execution for the `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame. Memory consumption depends on the partition size. |

Instead of `<frame_bound>`, `<frame_start>`, and `<frame_end>`, one of the following forms is specified:

```yql
UNBOUNDED PRECEDING
<frame_offset> PRECEDING
CURRENT ROW
<frame_offset> FOLLOWING
UNBOUNDED FOLLOWING
```

The `PRECEDING` forms count backward, `FOLLOWING` forms count forward; `UNBOUNDED` means the partition boundary. If only one boundary is specified, it sets the start, and the end is considered `CURRENT ROW`. `<frame_offset>` is a non-negative integer. For an unambiguous row order with equal `<sort_key>` values, add a key that distinguishes those rows to the window `ORDER BY`.

### Other notation forms {#select-alternative-overview}

A source can be written before `SELECT` in the form [`FROM ... SELECT`](from_select.md). The result list and other clauses retain their order:

```yql
[DISCARD]
FROM <sources_and_joins>
SELECT [ALL | DISTINCT] <result_list>
[WITHOUT [IF EXISTS] <column_name> [, ...]]
[WHERE <condition>]
[GROUP [COMPACT] BY [ALL] <grouping_element> [, ...]]
[HAVING <condition>]
[WINDOW <window_name> AS (<window_specification>) [, ...]]
[[ASSUME] ORDER BY <sort_key> [ASC | DESC] [, ...]]
[INTO RESULT <result_name>]
[LIMIT <row_count> [OFFSET <offset>]];
```

Only one `FROM` clause is allowed in a single selection.

To limit the result, a comma form is also supported:

```yql
LIMIT <offset>, <row_count>
```

It is equivalent to `LIMIT <row_count> OFFSET <offset>`; the order of the two arguments is reversed.

{% endif %}

## Procedure for executing SELECT {#selectexec}

The result of the `SELECT` query is computed as follows:

* the set of input tables is determined: expressions after [FROM](../select/from.md) are evaluated;
{% if feature_match_recogznize==true %}
* [MATCH_RECOGNIZE](match_recognize.md) is applied to the input tables;
{% endif %}
{% if feature_tablesample==true %}
* [SAMPLE](sample.md) / [TABLESAMPLE](sample.md) is computed;
{% endif %}
* [FLATTEN COLUMNS](flatten.md#flatten-columns) or [FLATTEN BY](flatten.md) is performed; aliases specified in `FLATTEN BY` become visible after this point;
{% if feature_join %}
* all [JOIN](join.md) are executed;
{% endif %}
* columns specified in [GROUP BY ... AS ...](group-by.md) are added (or replaced) to the resulting data;
* the [WHERE](where.md) clause is executed: all data that does not satisfy the predicate is filtered out;
* [GROUP BY](group-by.md) is performed, aggregate function values are computed;
* filtering is performed using [HAVING](group-by.md#having);
{% if feature_window_functions %}
* values of [window functions](window.md) are computed;
{% endif %}
* expressions in `SELECT` are evaluated;
* expressions in `SELECT` are assigned names defined by aliases;
* a top-level [DISTINCT](distinct.md) is applied to the columns obtained in this way;
* all subqueries in [UNION ALL](union.md#union-all) are computed in the same way and combined (see [PRAGMA AnsiOrderByLimitInUnionAll](../pragma.md#pragmas));
* sorting is performed according to [ORDER BY](order_by.md);
* [OFFSET and LIMIT](limit_offset.md) are applied to the result.

## Column order in YQL {#orderedcolumns}

In standard SQL, the order of columns specified in the projection (in `SELECT`) matters. Besides the fact that the column order must be preserved when displaying query results or when writing to a new table, some SQL constructs use this order.
This applies in particular to [UNION ALL](union.md#union-all) and positional [ORDER BY](order_by.md) (ORDER BY ordinal).

By default, the order of columns is ignored in YQL:

* the order of columns in output tables and in query results is undefined;
* the schema of the `UNION ALL` result is determined by column names, not by positions.

When `PRAGMA OrderedColumns;` is enabled, the order of columns is preserved in the query results and is derived from the order of columns in the input tables according to the following rules:

* `SELECT` with explicit column enumeration sets the corresponding order;
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits the order from its input;

{% if feature_join %}
* the order of columns after [JOIN](join.md): first the columns from the left side, then from the right. If the order of either side present in the output `JOIN` is not defined, the order of the result columns is also not defined;
{% endif %}

* the order of `UNION ALL` depends on the execution mode of [UNION ALL](union.md#union-all);
* the column order for [AS_TABLE](from_as_table.md) is not defined.

## Combining queries {#combining-queries}

Results of multiple SELECT (or subqueries) can be combined using the keywords `UNION` and `UNION ALL`.

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

* `ORDER BY/LIMIT/INTO RESULT` is allowed only after the last subquery;
* `DISCARD` is allowed only before the first subquery;
* the specified operators act on the result `UNION [ALL]`, not on the subquery;
* to apply an operator to a subquery, the subquery must be enclosed in parentheses.

{% if backend_name != "YDB" %}

## Accessing multiple tables in a single query

In standard SQL, [UNION ALL](union.md#union-all) is used to query multiple tables, which combines the results of two or more `SELECT`. This is not very convenient for a use case where you need to run the same query across multiple tables (for example, containing data for different dates). In YQL, for convenience, in `SELECT` after `FROM` you can specify not only a single table or subquery, but also call built-in functions that allow combining data from multiple tables.

The following functions are defined for these purposes:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` — combines all tables listed in the arguments.

`EACH($list_of_strings)` or `EACH($list_of_strings VIEW view_name)` — combines all tables whose names are listed in the string list. Optionally, you can pass multiple lists in separate arguments, similar to `CONCAT`.

```RANGE(`prefix`, `min`, `max`, `suffix`, `view`)``` — combines a range of tables. Arguments:

* prefix — directory for searching tables, specified without a trailing slash. The only required argument; if only it is specified, all tables in this directory are used.
* min, max — the next two arguments specify a range of names for including tables. The range is inclusive on both ends. If the range is not specified, all tables in the prefix directory are used. Names of tables or directories located in the directory specified in prefix are compared with the range `[min, max]` lexicographically, not concatenated, so it is important to specify the range without leading slashes.
* suffix — table name. Expected without a leading slash. If suffix is not specified, the arguments `[min, max]` specify a range of table names. If suffix is specified, the arguments `[min, max]` specify a range of folders in which a table with the name specified in the suffix argument exists.

```LIKE(`prefix`, `pattern`, `suffix`, `view`)` and `REGEXP(`prefix`, `pattern`, `suffix`, `view`)``` — the pattern argument is specified in a format similar to the binary operators of the same name: [LIKE](../expressions.md#check-match) and [REGEXP](../expressions.md#check-match).

```FILTER(`prefix`, `callable`, `suffix`, `view`)``` — the callable argument must be a callable expression with signature `(String)->Bool`, which will be called for each table/subdirectory in the prefix directory. Only those tables for which the callable value returned `true` will participate in the query. As a callable value, it is most convenient to use [lambda functions](../expressions.md#lambda){% if yql == true %}, or UDFs in [Python](../../udf/python.md) or [JavaScript](../../udf/javascript.md){% endif %}.

{% note warning %}

The order in which tables are merged by all the above functions is not guaranteed.

The list of tables is computed **before** the query itself is executed. Therefore, tables created during the query will not be included in the function results.

{% endnote %}

By default, schemas of all participating tables are merged according to the rules of [UNION ALL](union.md#union-all). If schema merging is not desired, you can use functions with the suffix `_STRICT`, for example `CONCAT_STRICT` or `RANGE_STRICT`, which work exactly like the original ones but treat any discrepancy in table schemas as an error.

To specify the cluster of the merged tables, you need to specify it before the function name.

All arguments of the functions described above can be declared separately using [named expressions](../expressions.md#named-nodes). In this case, simple expressions are also allowed in them by implicitly calling [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

The name of the source table from which each row was originally obtained can be obtained using the {% if feature_bulk_tables %}[TablePath()](../../builtins/basic.md#tablepath){% else %}TablePath(){% endif %} function.

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

{% endif %}

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
* [Hybrid search](hybrid_search.md)
* [VIEW json_index](json_index.md)

{% endif %}
