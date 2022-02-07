## COUNT {#count}

Counting the number of rows in the table (if `*` or constant is specified as the argument) or non-empty values in a table column (if the column name is specified as an argument).

Like other aggregate functions, it can be combined with [GROUP BY](../../../syntax/group_by.md) to get statistics on the parts of the table that correspond to the values in the columns being grouped. {% if select_statement != "SELECT STREAM" %}Use the modifier [DISTINCT](../../../syntax/group_by.md#distinct) to count distinct values.{% endif %}

**Examples**

```yql
SELECT COUNT(*) FROM my_table;
```

```yql
SELECT key, COUNT(value) FROM my_table GROUP BY key;
```

{% if select_statement != "SELECT STREAM" %}

```yql
SELECT COUNT(DISTINCT value) FROM my_table;
```

{% endif %}

## MIN and MAX {#min-max}

Minimum or maximum value.

As an argument, you may use an arbitrary computable expression with a numeric result.

**Examples**

```yql
SELECT MIN(value), MAX(value) FROM my_table;
```

## SUM {#sum}

Sum of the numbers.

As an argument, you may use an arbitrary computable expression with a numeric result.

Integers are automatically expanded to 64 bits to reduce the risk of overflow.

```yql
SELECT SUM(value) FROM my_table;
```

## AVG {#avg}

Arithmetic average.

As an argument, you may use an arbitrary computable expression with a numeric result.

Integer values and time intervals are automatically converted to Double.

**Examples**

```yql
SELECT AVG(value) FROM my_table;
```

## COUNT_IF {#count-if}

Number of rows for which the expression specified as the argument is true (the expression's calculation result is true).

The value `NULL` is equated to `false` (if the argument type is `Bool?`).

The function *does not* do the implicit type casting to Boolean for strings and numbers.

**Examples**

```yql
SELECT
  COUNT_IF(value % 2 == 1) AS odd_count
```

{% if select_statement != "SELECT STREAM" %}
{% note info %}

To count distinct values in rows meeting the condition, unlike other aggregate functions, you can't use the modifier [DISTINCT](../../../syntax/group_by.md#distinct) because arguments contain no values. To get this result, use in the subquery the built-in function [IF](../../../builtins/basic.md#if) with two arguments (to get `NULL` in else), and apply an outer [COUNT(DISTINCT ...)](#count) to its result.

{% endnote %}
{% endif %}

## SUM_IF and AVG_IF {#sum-if}

Sum or arithmetic average, but only for the rows that satisfy the condition passed by the second argument.

Therefore, `SUM_IF(value, condition)` is a slightly shorter notation for `SUM(IF(condition, value))`, same for `AVG`. The argument's data type expansion is similar to the same-name functions without a suffix.

**Examples**

```yql
SELECT
    SUM_IF(value, value % 2 == 1) AS odd_sum,
    AVG_IF(value, value % 2 == 1) AS odd_avg,
FROM my_table;
```

When you use [aggregation factories](../../basic.md#aggregationfactory), a `Tuple` containing a value and a predicate is passed as the first [AGGREGATE_BY](#aggregateby) argument.

**Examples**

```yql
$sum_if_factory = AggregationFactory("SUM_IF");
$avg_if_factory = AggregationFactory("AVG_IF");

SELECT
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $sum_if_factory) AS odd_sum,
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $avg_if_factory) AS odd_avg
FROM my_table;
```

## SOME {#some}

Get the value for an expression specified as an argument, for one of the table rows. Gives no guarantee of which row is used. It's similar to the [any()]{% if lang == "en" %}(https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/any/){% else %}(https://clickhouse.tech/docs/ru/sql-reference/aggregate-functions/reference/any/){% endif %} function in ClickHouse.

Because of no guarantee, `SOME` is computationally cheaper than [MIN](#min)/[MAX](#max) often used in similar situations.

**Examples**

```yql
SELECT
  SOME(value)
FROM my_table;
```

{% note alert %}

When the aggregate function `SOME` is called multiple times, it's **not** guaranteed that all the resulting values are taken from the same row of the source table. To get this guarantee, pack the values into any container and pass it to `SOME`. For example, in the case of a structure, you can apply [AsStruct](../../../builtins/basic.md#asstruct)

{% endnote %}

