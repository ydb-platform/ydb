<!-- markdownlint-disable blanks-around-fences -->

# SELECT

Returns the result of evaluating the expressions specified after `SELECT`.

It can be used in combination with other operations to obtain other effect.

## Examples

```yql
SELECT "Hello, world!";
```

```yql
SELECT 2 + 2;
```



## SELECT execution procedure {#selectexec}

The `SELECT` query result is calculated as follows:

* Determine the set of input tables by evaluating the [FROM](from.md) clauses.
* Apply [MATCH_RECOGNIZE](match_recognize.md) to input tables.
* Evaluate [SAMPLE](sample.md)/[TABLESAMPLE](sample.md).
* Execute [FLATTEN COLUMNS](flatten.md#flatten-columns) or [FLATTEN BY](flatten.md); aliases set in `FLATTEN BY` become visible after this point.

{% if feature_join %}

* Execute every [JOIN](join.md).

{% endif %}

* Add to (or replace in) the data the columns listed in [GROUP BY ... AS ...](group-by.md).
* Execute [WHERE](where.md) &mdash; Discard all the data mismatching the predicate.
* Execute [GROUP BY](group-by.md), evaluate aggregate functions.
* Apply the filter [HAVING](group-by.md#having).

{% if feature_window_functions %}

* Evaluate [window functions](window.md);

{% endif %}

* Evaluate expressions in `SELECT`.
* Assign names set by aliases to expressions in `SELECT`.
* Apply top-level [DISTINCT](distinct.md) to the resulting columns.
* Execute similarly every subquery inside [UNION ALL](union.md#union_all), combine them (see [PRAGMA AnsiOrderByLimitInUnionAll](../pragma.md#pragmas)).
* Perform sorting with [ORDER BY](order_by.md).
* Apply [OFFSET and LIMIT](limit_offset.md) to the result.



## Column order in YQL {#orderedcolumns}

The standard SQL is sensitive to the order of columns in projections (that is, in `SELECT`). While the order of columns must be preserved in the query results or when writing data to a new table, some SQL constructs use this order.
This applies, for example, to [UNION ALL](union.md#union_all) and positional [ORDER BY](order_by.md) (ORDER BY ordinal).

The column order is ignored in YQL by default:

* The order of columns in the output tables and query results is undefined
* The data scheme of the `UNION ALL` result is output by column names rather than positions

If you enable `PRAGMA OrderedColumns;`, the order of columns is preserved in the query results and is derived from the order of columns in the input tables using the following rules:

* `SELECT`: an explicit column enumeration dictates the result order.
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits the order from its input.

{% if feature_join %}

* The order of columns after [JOIN](join.md): First output the left-hand columns, then the right-hand ones. If the column order in any of the sides in the `JOIN` output is undefined, the column order in the result is also undefined.

{% endif %}

* The order in `UNION ALL` depends on the [UNION ALL](union.md#union_all) execution mode.
* The column order for [AS_TABLE](from_as_table.md) is undefined.

{% note warning %}

In the YT table schema, key columns always precede non-key columns. The order of key columns is determined by the order of the composite key.
When `PRAGMA OrderedColumns;` is enabled, non-key columns preserve their output order.

{% endnote %}



### Combining queries {#combining-queries}

Results of several SELECT statements (or subqueries) can be combined using `UNION` and `UNION ALL` keywords.

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

If the underlying queries have one of the `ORDER BY/LIMIT/DISCARD/INTO RESULT` operators, the following rules apply:

* `ORDER BY/LIMIT/INTO RESULT` is only allowed after the last query
* `DISCARD` is only allowed before the first query
* the operators apply to the `UNION [ALL]` as a whole, instead of referring to one of the queries
* to apply the operator to one of the queries, enclose the query in parantheses


## Clauses supported in SELECT

* [FROM](from.md)
* [FROM AS_TABLE](from_as_table.md)
* [FROM SELECT](from_select.md)
* [JOIN](join.md)
* [GROUP BY](group-by.md)
* [FLATTEN](flatten.md)
* [WINDOW](window.md)
* [DISTINCT](distinct.md)
* [UNIQUE DISTINCT](unique_distinct_hints.md)
* [UNION](union.md)
* [WITH](with.md)
* [WITHOUT](without.md)
* [WHERE](where.md)
* [ORDER BY](order_by.md)
* [ASSUME ORDER BY](assume_order_by.md)
* [LIMIT OFFSET](limit_offset.md)
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)
* [MATCH_RECOGNIZE](match_recognize.md)

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

{% endif %}

* [VIEW vector_index](vector_index.md)
