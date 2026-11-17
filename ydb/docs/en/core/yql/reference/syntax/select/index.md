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

The `SELECT` construct retrieves data from [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) and [column-oriented](../../../../concepts/datamodel/table.md#column-oriented-tables) tables, [external tables](../../../../concepts/datamodel/external_table.md) and [data sources](../../../../concepts/datamodel/external_data_source.md), [views](../../../../concepts/datamodel/view.md), and [topics](topics.md). By default, the selected data is returned to the user, but `SELECT` can also be part of more complex constructs, such as `UPSERT INTO ... SELECT`, `CREATE TABLE ... AS SELECT ...`, and so on.

A `SELECT` query can contain a single select or multiple selects whose results are combined by the `UNION`, `EXCEPT`, and `INTERSECT` operations. Below are the schemas for both forms, followed by a description of their constituent parts.


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

Only `SELECT` and `<result_list>` are mandatory. The remaining sections are added as needed according to the following rules:

| Query part | Purpose |
| --- | --- |
| `SELECT <result_list>` | Evaluates the expressions that form the output columns. The list can contain columns, function calls, expressions, and asterisks. |
| `ALL` / [`DISTINCT`](distinct.md) | `ALL` keeps duplicate rows and is the default. `DISTINCT` keeps distinct result rows. |
| [`WITHOUT`](without.md) | Excludes the specified columns from the asterisk result. `IF EXISTS` allows the excluded column to be absent. |
| [`FROM`](from.md) | Specifies the [row sources](#select-sources-overview), their [transformations](#select-flatten-overview), and [joins](#select-joins-overview). Without `FROM`, you can evaluate expressions, e.g. `SELECT 2 + 2`, but you cannot access input columns, filter them, or group them. |
| [`WHERE`](where.md) | Keeps input rows for which the condition is true. |
| [`GROUP BY`](group-by.md) | Combines rows into groups by [grouping keys and sets](#select-grouping-overview) to compute aggregate functions. |
| [`HAVING`](group-by.md#having) | Filters the aggregation results. |
| [`WINDOW`](window.md) | Declares [named windows](#select-windows-overview) for window function calls via `OVER`. |
| [`ORDER BY`](order_by.md) / [`ASSUME ORDER BY`](assume_order_by.md) | `ORDER BY` sorts the result; `ASSUME ORDER BY` declares an existing sort without performing it. The `ASC` direction is used by default, `DESC` sets the reverse order. For `ORDER BY`, `<sort_key>` can be a column or an expression. In `ASSUME ORDER BY`, output column names are specified. |
| [`INTO RESULT`](../into_result.md) | Sets the name of the result set. The name can be an identifier, including one in backticks, or a named expression. |
| [`LIMIT` / `OFFSET`](limit_offset.md) | Skips `<offset>` rows and returns at most `<row_count>` subsequent rows. `OFFSET` is specified together with `LIMIT`. `<row_count>` and `<offset>` are expressions. `NULL` in `LIMIT` removes the row count limit. Without `ORDER BY`, the set of selected rows is undefined. |
| [`DISCARD`](../discard.md) | Executes the query but does not return its result to the client. In a single `SELECT`, it cannot be combined with `INTO RESULT`. |

### Compound query {#compound-select-overview}

The `UNION`, `EXCEPT`, and `INTERSECT` operations take the results of individual selects. Each select has its own sources, filters, grouping, and windows; sorting and limiting of the overall result can be specified after the last select:

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

Here each `SELECT ...` is a part of the [single query schema](#single-select-overview) from `SELECT` through `WINDOW` inclusive. The common trailing sections are written at the end of the last `SELECT` without parentheses, but apply to the result of the entire combination.

| Operation | Result |
| --- | --- |
| `UNION` | Rows that appear in at least one of the two selects. |
| `EXCEPT` | Rows of the left select that are absent from the right one. |
| `INTERSECT` | Rows that appear in both selects. |

By default, these operations remove duplicates, which corresponds to `DISTINCT`. The `ALL` qualifier takes the number of occurrences into account: `UNION ALL` keeps all rows of both selects, `EXCEPT ALL` subtracts the number of occurrences on the right from the number of occurrences on the left, but not below zero, and `INTERSECT ALL` keeps the smaller of the two occurrence counts. `UNION DISTINCT`, `EXCEPT`, and `INTERSECT` are available starting from YQL language version `2025.03`.

`INTERSECT` has higher precedence than `UNION` and `EXCEPT`; operations of the same precedence are grouped left to right. The rules for matching columns in `UNION` are described in the [Combining subquery results](union.md) section.

`DISCARD` is placed before the first select and applies to the entire statement. `INTO RESULT` sets the name of the overall result. To actually sort the result of the combination, use `ORDER BY`: operations over selects by themselves do not guarantee row order.

For example, the following query returns at most the first ten rows after sorting the overall result:

```yql
SELECT key FROM first_table
UNION ALL
SELECT key FROM second_table
ORDER BY key
LIMIT 10;
```

To make sorting and limiting apply to only one select, that select is enclosed in parentheses. In the following query, at most ten rows are taken from the first table, and all rows from the second:

```yql
(SELECT key FROM first_table ORDER BY key LIMIT 10)
UNION ALL
SELECT key FROM second_table;
```

### Result list {#select-list-overview}

Instead of `<result_list>`, you specify expressions and qualified asterisks separated by commas, or a single plain asterisk:

| Element | Purpose |
| --- | --- |
| [`<expression> [[AS] <alias>]`](../expressions.md) | Evaluates an output column; the alias sets its name. The expression can contain calls to regular, aggregate, and window functions. |
| `<source_alias>.*` | Selects all columns of the specified source; allowed together with other list elements. |
| `*` | Selects all columns. A plain asterisk cannot be mixed with other list elements. |

The `WITHOUT` section is placed immediately after the result list. A column in it can be specified as `<column_name>` or `<source_alias>.<column_name>`.

Hints about result properties are specified in comments after `SELECT` and are described in the [UNIQUE and DISTINCT](unique_distinct_hints.md) section.

### Data sources {#select-sources-overview}

In the [`FROM`](from.md) section, you can specify a single source or join multiple sources using `JOIN`. The main forms of a single source:

| Form | Purpose |
| --- | --- |
| `[<external_data_source_name>.]<table_name> [VIEW <index_name>]` | Reads a table, optionally through the [specified index](secondary_index.md). The table path can be enclosed in backticks. |
| `[<external_data_source_name>.]<topic_name>` | Reads a [local or external topic](topics.md). To wait for new messages, use `WITH (STREAMING = "TRUE")`; the number of returned messages is limited by `LIMIT`. |
| `(SELECT ...)` | The result of a subquery, including a compound one. Parentheses are mandatory. |
| `$<name>` | A source defined by a named expression, for example, the result of a subquery. |
| [`AS_TABLE(<expression>)`](from_as_table.md) | A list of structures converted into a table source. |
| `(VALUES (<expression> [, ...]) [, ...])` | Rows specified directly in the query. All rows must have the same number of values. |

A source can be assigned an alias: `[AS] <alias>`. For `VALUES`, a list of column names is additionally allowed after the alias: `AS <alias> (<column_name> [, ...])`.

### Source settings (`WITH`) {#select-with-overview}

The [`WITH` construct](with.md) sets the settings for reading from a source. The available parameters and the forms of their values depend on the source type; for example, you can specify the format of stored data for external sources, a schema for topics and external sources, and `WATERMARK` for streaming queries. The settings are specified before the alias:

```yql
[<external_data_source_name>.]<table_name> [VIEW <index_name>]
[WITH (<table_hint> [, ...])]
[[AS] <alias>]
```

Each `<table_hint>` contains a parameter name and, if required, a value in the form defined for that setting. For a single setting, the parentheses after `WITH` are optional. For example, `FORMAT = <format>` sets the data format, and `SCHEMA <struct_type>` sets the schema when reading files from an external source or messages of a [topic](topics.md), including a local one.

### Data transformation (`FLATTEN`) {#select-flatten-overview}

After the source and its alias, you can specify one [`FLATTEN` transformation](flatten.md):

```yql
FLATTEN COLUMNS
```

or one of the `FLATTEN BY` forms:

```yql
FLATTEN [OPTIONAL | LIST | DICT] BY <column_name> [AS <alias>]
FLATTEN [OPTIONAL | LIST | DICT] BY (<column_name> [AS <alias>] [, ...])
FLATTEN [OPTIONAL | LIST | DICT] BY (<expression> AS <alias> [, ...])
```

`FLATTEN COLUMNS` expands structure fields into separate columns, and `FLATTEN BY` expands container elements into rows. The list in parentheses can contain both columns and expressions. For an arbitrary expression, the alias is mandatory.

### Joining sources {#select-joins-overview}

Instead of `<sources_and_joins>` in the main schema, you can specify a source and a sequence of joins. When joining sources, `WITH`, the alias, and `FLATTEN` refer to the source after which they are written. Each [join with a condition](join.md) has the form:

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
| `LEFT [OUTER]` / `RIGHT [OUTER]` | All rows of the specified side and matched rows of the other; when there is no match, the columns of the other side are filled with `NULL`. |
| `FULL [OUTER]` | All rows of both sides with matched rows combined. |
| `LEFT SEMI` / `RIGHT SEMI` | Only columns of the specified side and only rows that have a match with the other side. |
| `LEFT ONLY` / `RIGHT ONLY` | Only columns of the specified side and only rows without a match with the other side. |
| `EXCLUSION` | Rows of both sides that have no match with the other side. |

`ON` specifies equality of key expressions, and `USING` lists same-named key columns. `ANY` before a source keeps one row for each set of join keys on that side; which row is kept is undefined.

The Cartesian product is written separately, without `ON`, `USING`, and `ANY`:

```yql
FROM <source>
    CROSS JOIN <source>
```

Joins of different types can be alternated by adding the next join after the previous one. `ANY` without a join is not allowed. A comma join requires `PRAGMA AnsiImplicitCrossJoin;` and corresponds to `CROSS JOIN`.

### Grouping keys and sets {#select-grouping-overview}

The [`GROUP BY` section](group-by.md) contains one or more grouping elements separated by commas. The following are used as `<grouping_element>`:

| Form | Purpose |
| --- | --- |
| `<expression> [AS <alias>]` | A grouping key: a column or the result of an expression. The alias allows referring to the computed key by name. |
| `()` | An empty set of keys for a grand total. |
| `(<expression> [, ...])` | A set of keys, for example inside `GROUPING SETS`. |
| [`ROLLUP(<expression> [, ...])`](group-by.md#rollup) | Totals over successive sets of keys and a grand total. |
| [`CUBE(<expression> [, ...])`](group-by.md#rollup) | Totals over all combinations of keys. |
| [`GROUPING SETS(<grouping_set> [, ...])`](group-by.md#rollup) | Explicitly specified sets of keys for computing totals. |
| [`SessionWindow(<time_expression>, <session_timeout>) AS <alias>`](group-by.md#session-window) | Grouping events by sessions. |
| [`HoppingWindow(<time_expression>, <hop>, <interval>)`](group-by.md#group-by-hopping_window) | Grouping by overlapping time windows. |
| [`HOP(<time_expression>, <hop>, <interval>, <delay>)`](group-by.md#group-by-hop) | Grouping by time windows with a delay parameter. |

`<grouping_set>` specifies a key, a set of keys in parentheses, an empty set, or a nested grouping construct. `<time_expression>` is an expression for the event time; `<session_timeout>` sets the session boundary, `<hop>` and `<interval>` are the shift period and window duration, and `<delay>` is the delay of its closing in streaming mode.

`COMPACT` selects the aggregation strategy for a small amount of data per key. `ALL` in `GROUP BY` can be omitted; `GROUP BY DISTINCT` is not supported. The `HAVING` section follows `GROUP BY` and allows filtering groups by the results of aggregate functions.

### Window definitions {#select-windows-overview}

A [window function](window.md) computes a value over rows related to the current row while preserving individual result rows. A window definition can be specified directly after `OVER` or declared in the `WINDOW` section and used by name:

```yql
<window_function>([<argument> [, ...]]) OVER (<window_specification>)
<window_function>([<argument> [, ...]]) OVER <window_name>
```

The `WINDOW` section with a `ROWS` frame has the form:

```yql
WINDOW <window_name> AS (
    [PARTITION [COMPACT] BY <expression> [AS <alias>] [, ...]]
    [ORDER BY <sort_key> [ASC | DESC] [, ...]]
    [ROWS { <frame_bound> | BETWEEN <frame_start> AND <frame_end> }]
) [, ...]
```

| Definition part | Purpose |
| --- | --- |
| [`PARTITION BY`](window.md#partition) | Divides rows into independent partitions. Without `PARTITION BY`, all input rows form a single partition. The key can be a column or an expression; an expression can be assigned an alias. |
| [`ORDER BY`](window.md#partition) | Sets the order of rows within a partition. The `ASC` direction is used by default, `DESC` sets the reverse order. To order the query result, a separate `ORDER BY` after the `WINDOW` section is required. |
| [`ROWS`](window.md#frame) | Sets a frame by row offsets relative to the current row. Aggregate functions, as well as `FIRST_VALUE`, `LAST_VALUE`, and `NTH_VALUE`, are computed over the frame. The `ROW_NUMBER`, `LAG`, and `LEAD` functions work over the entire partition regardless of the frame. |
| Default frame | In the Query Service, the [`AnsiCurrentRow`](../pragma.md#pragmas) pragma is enabled by default. If no frame is specified, then, when a window `ORDER BY` is present, `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is used: it includes all rows from the start of the partition up to the last row with the same sort key values as the current one. When the pragma is disabled, `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` is used. Without a window `ORDER BY`, `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` is used. To get the last value of the entire partition via `LAST_VALUE`, explicitly set the frame end to `UNBOUNDED FOLLOWING`. |
| [`COMPACT`](window.md) | Selects in-memory execution for the `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame. Memory consumption depends on the partition size. |

Instead of `<frame_bound>`, `<frame_start>`, and `<frame_end>`, one of the following forms is specified:

```yql
UNBOUNDED PRECEDING
<frame_offset> PRECEDING
CURRENT ROW
<frame_offset> FOLLOWING
UNBOUNDED FOLLOWING
```

The `PRECEDING` forms count backwards, `FOLLOWING` count forwards; `UNBOUNDED` means the partition boundary. If only one boundary is specified, it sets the start, and the end is considered equal to `CURRENT ROW`. `<frame_offset>` is a non-negative integer. For an unambiguous row order when `<sort_key>` values are equal, add a key that distinguishes these rows to the window `ORDER BY`.

### Other forms of notation {#select-alternative-overview}

A source can be written before `SELECT` in the [`FROM ... SELECT`](from_select.md) form. The result list and the remaining sections keep their order:

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

Only one `FROM` section is allowed in a single select.

To limit the result, the comma form is also supported:

```yql
LIMIT <offset>, <row_count>
```

It is equivalent to `LIMIT <row_count> OFFSET <offset>`; the order of the two arguments is swapped.

{% endif %}

## Procedure for executing SELECT {#selectexec}

The result of the `SELECT` query is computed as follows:

* the set of input tables is determined: expressions after [FROM](../select/from.md) are evaluated
{% if feature_match_recogznize==true %}
* [MATCH_RECOGNIZE](match_recognize.md) is applied to the input tables
{% endif %}
{% if feature_tablesample==true %}
* [SAMPLE](sample.md) / [TABLESAMPLE](sample.md) is computed
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

* the order of columns in output tables and in query results is undefined
* the data schema of the `UNION ALL` result is output by column names, not by positions

When `PRAGMA OrderedColumns;` is enabled, the order of columns is preserved in the query results and is derived from the order of columns in the input tables according to the following rules:

* `SELECT` with explicit column enumeration sets the corresponding order;
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits the order from its input;

{% if feature_join %}
* the order of columns after [JOIN](join.md): first the columns from the left side, then from the right. If the order of either side present in the output `JOIN` is not defined, the order of the result columns is also not defined;
{% endif %}

* the order of `UNION ALL` depends on the execution mode of [UNION ALL](union.md#union-all);
* column order for [AS_TABLE](from_as_table.md) is not defined;


## Combination of queries {#combining-queries}

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
