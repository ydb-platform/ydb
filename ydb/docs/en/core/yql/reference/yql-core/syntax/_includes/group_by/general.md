## GROUP BY

Group the `SELECT` results by the values of the specified columns or expressions. `GROUP BY` is often combined with [aggregate functions](../../../builtins/aggregation.md) (`COUNT`, `MAX`, `MIN`, `SUM`, `AVG`) to perform calculations in each group.

**Syntax**

```sql
SELECT                             -- In SELECT, you can use:
    column1,                       -- key columns specified in GROUP BY
    key_n,                         -- named expressions specified in GROUP BY
    column1 + key_n,               -- arbitrary non-aggregate functions on them
    Aggr_Func1( column2 ),         -- aggregate functions containing any columns as arguments,
    Aggr_Func2( key_n + column2 ), -- including named expressions specified in GROUP BY
    ...
FROM table
GROUP BY
    column1, column2, ...,
    <expr> AS key_n           -- When grouping by expression, you can set a name for it using AS,
                              -- and use it in SELECT
```

The query in the format `SELECT * FROM table GROUP BY k1, k2, ...` returns all columns listed in GROUP BY, i.e., is equivalent to `SELECT DISTINCT k1, k2, ... FROM table`.

An asterisk can also be used as an argument for the `COUNT` aggregate function. `COUNT(*)` means "the count of rows in the group".

{% note info %}

Aggregate functions ignore `NULL` in their arguments, except for `COUNT`.

{% endnote %}

YQL also provides aggregation factories implemented by the functions [`AGGREGATION_FACTORY`](../../../builtins/basic.md#aggregationfactory) and [`AGGREGATE_BY`](../../../builtins/aggregation.md#aggregateby).

**Examples**

```sql
SELECT key, COUNT(*) FROM my_table
GROUP BY key;
```

```sql
SELECT double_key, COUNT(*) FROM my_table
GROUP BY key + key AS double_key;
```

```sql
SELECT
   double_key,                           -- OK: A key column
   COUNT(*) AS group_size,               -- OK: COUNT(*)
   SUM(key + subkey) AS sum1,            -- OK: An aggregate function
   CAST(SUM(1 + 2) AS String) AS sum2,   -- OK: An aggregate function with a constant argument
   SUM(SUM(1) + key) AS sum3,            -- ERROR: Nested aggregations are not allowed
   key AS k1,                            -- ERROR: Using a non-key column named key without aggregation
   key * 2 AS dk1,                       -- ERROR in YQL: using a non-key column named key without aggregation
FROM my_table
GROUP BY
  key * 2 AS double_key,
  subkey as sk,
```

{% note warning %}

Specifying a name for a column or expression in `GROUP BY .. AS foo` it is an extension on top of YQL. Such a name becomes visible in `WHERE` despite the fact that filtering by `WHERE` is executed [before](../../select/where.md) the grouping. For example, if the `T` table includes two columns, `foo` and `bar`, then the query `SELECT foo FROM T WHERE foo > 0 GROUP BY bar AS foo` would actually filter data by the `bar` column from the source table.

{% endnote %}

