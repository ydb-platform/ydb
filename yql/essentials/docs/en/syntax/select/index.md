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
* Apply [SAMPLE](sample.md)/[TABLESAMPLE](sample.md) to input tables.
* Execute [FLATTEN COLUMNS](../flatten.md#flatten-columns) or [FLATTEN BY](../flatten.md); aliases set in `FLATTEN BY` become visible after this point.
* Execute every [JOIN](../join.md).
* Add to (or replace in) the data the columns listed in [GROUP BY ... AS ...](../group_by.md) (this clause executes after `WHERE` since version [2025.02](../../changelog/2025.02.md#group-by-expr-alias-where)).
* Execute [WHERE](where.md) - discard all the data mismatching the predicate.
* Execute [GROUP BY](../group_by.md), evaluate aggregate functions.
* Apply the filter [HAVING](../group_by.md#having).
* Evaluate [window functions](../window.md);
* Evaluate expressions in `SELECT`.
* Assign names set by aliases to expressions in `SELECT`.
* Apply top-level [DISTINCT](distinct.md) to the resulting columns.
* Execute similarly every subquery inside [UNION ALL](union.md#union-all).
* Perform sorting with [ORDER BY](order_by.md).
* Apply [OFFSET and LIMIT](limit_offset.md) to the result.


## Column order in YQL {#orderedcolumns}

The standard SQL is sensitive to the order of columns in projections (that is, in `SELECT`). While the order of columns must be preserved in the query results or when writing data to a new table, some SQL constructs use this order.
This applies, for example, to [UNION ALL](union.md#union-all) and positional [ORDER BY](order_by.md) (ORDER BY ordinal).

The column order is ignored in YQL by default:

* The order of columns in the output tables and query results is undefined
* The data scheme of the `UNION ALL` result is output by column names rather than positions

If you enable `PRAGMA OrderedColumns;`, the order of columns is preserved in the query results and is derived from the order of columns in the input tables using the following rules:

* `SELECT`: an explicit column enumeration dictates the result order.
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits the order from its input.
* The order of columns after [JOIN](../join.md): First output the left-hand columns, then the right-hand ones. If the column order in any of the sides in the `JOIN` output is undefined, the column order in the result is also undefined.
* The order in `UNION ALL` depends on the [UNION ALL](union.md#union-all) execution mode.
* The column order for [AS_TABLE](from_as_table.md) is undefined.


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

Intersection of several `SELECT` statements (or subqueries) can be computed using `INTERSECT` and `INTERSECT ALL` keywords.

```yql
query1 INTERSECT query2
```

Intersection of more than two queries is interpreted as a left-associative operation, that is

```yql
query1 INTERSECT query2 INTERSECT query3
```

is interpreted as

```yql
(query1 INTERSECT query2) INTERSECT query3
```

Difference between several `SELECT` statements (or subqueries) can be computed using `EXCEPT` and `EXCEPT ALL` keywords.

```yql
query1 EXCEPT query2
```

Difference between more than two queries is interpreted as a left-associative operation, that is

```yql
query1 EXCEPT query2 EXCEPT query3
```

is interpreted as

```yql
(query1 EXCEPT query2) EXCEPT query3
```

Different operators (`UNION`, `INTERSECT`, and `EXCEPT`) can be used together in a single query. In such cases, `UNION` and `EXCEPT` have equal, but lower precedence than `INTERSECT`.

For example, the following queries:

```yql
query1 UNION query2 INTERSECT query3

query1 UNION query2 EXCEPT query3

query1 EXCEPT query2 UNION query3
```

are interpreted as:

```yql
query1 UNION (query2 INTERSECT query3)

(query1 UNION query2) EXCEPT query3

(query1 EXCEPT query2) UNION query3
```

respectively.

If the underlying queries have one of the `ORDER BY/LIMIT/DISCARD/INTO RESULT` operators, the following rules apply:

* `ORDER BY/LIMIT/INTO RESULT` is only allowed after the last query
* `DISCARD` is only allowed before the first query
* the operators apply to the `UNION [ALL]` as a whole, instead of referring to one of the queries
* to apply the operator to one of the queries, enclose the query in parantheses

## Clauses supported in SELECT

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
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)
* [CONCAT](concat.md)
