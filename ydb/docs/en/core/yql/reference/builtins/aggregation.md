# Aggregate functions

## COUNT {#count}

Counting the number of rows in the table (if `*` or constant is specified as the argument) or non-empty values in a table column (if the column name is specified as an argument).

Like other aggregate functions, it can be combined with [GROUP BY](../syntax/select/group-by.md) to get statistics on the parts of the table that correspond to the values in the columns being grouped. {% if select_statement != "SELECT STREAM" %}Use the modifier [DISTINCT](../syntax/select/group-by.md#distinct) to count distinct values.{% endif %}

### Examples

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

### Examples

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

### Examples

```yql
SELECT AVG(value) FROM my_table;
```

## COUNT_IF {#count-if}

Number of rows for which the expression specified as the argument is true (the expression's calculation result is true).

The value `NULL` is equated to `false` (if the argument type is `Bool?`).

The function *does not* do the implicit type casting to Boolean for strings and numbers.

### Examples

```yql
SELECT
  COUNT_IF(value % 2 == 1) AS odd_count
FROM my_table;
```

{% if select_statement != "SELECT STREAM" %}

{% note info %}

To count distinct values in rows meeting the condition, unlike other aggregate functions, you can't use the modifier [DISTINCT](../syntax/select/group-by.md#distinct) because arguments contain no values. To get such a result, use a query like this:

```yql
SELECT
    COUNT(DISTINCT IF(value % 2 == 1, value))
FROM my_table;
```

{% endnote %}

{% endif %}

## SUM_IF and AVG_IF {#sum-if}

Sum or arithmetic average, but only for the rows that satisfy the condition passed by the second argument.

Therefore, `SUM_IF(value, condition)` is a slightly shorter notation for `SUM(IF(condition, value))`, same for `AVG`. The argument's data type expansion is similar to the same-name functions without a suffix.

### Examples

```yql
SELECT
    SUM_IF(value, value % 2 == 1) AS odd_sum,
    AVG_IF(value, value % 2 == 1) AS odd_avg,
FROM my_table;
```

When you use [aggregation factories](basic.md#aggregationfactory), a `Tuple` containing a value and a predicate is passed as the first [AGGREGATE_BY](#aggregate-by) argument.

```yql
$sum_if_factory = AggregationFactory("SUM_IF");
$avg_if_factory = AggregationFactory("AVG_IF");

SELECT
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $sum_if_factory) AS odd_sum,
    AGGREGATE_BY(AsTuple(value, value % 2 == 1), $avg_if_factory) AS odd_avg
FROM my_table;
```

## SOME {#some}

Get the value for an expression specified as an argument, for one of the table rows. Gives no guarantee of which row is used. It's similar to the [any()](https://clickhouse.com/docs/en/sql-reference/aggregate-functions/reference/any/) function in ClickHouse.

Because of no guarantee, `SOME` is computationally cheaper than [MIN / MAX](#min-max) often used in similar situations.

### Examples

```yql
SELECT
  SOME(value)
FROM my_table;
```

{% note alert %}

When the aggregate function `SOME` is called multiple times, it's **not** guaranteed that all the resulting values are taken from the same row of the source table. To get this guarantee, pack the values into any container and pass it to `SOME`. For example, in the case of a structure, you can apply [AsStruct](basic.md#asstruct)

{% endnote %}



## CountDistinctEstimate, HyperLogLog, and HLL {#countdistinctestimate}

Approximating the number of unique values using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm. Logically, it does the same thing as [COUNT(DISTINCT ...)](#count), but runs much faster at the cost of some error.

Arguments:

1. Estimated value
2. Accuracy (4 to 18 inclusive, 14 by default).

By selecting accuracy, you can trade added resource and RAM consumption for decreased error.

All the three functions are aliases at the moment, but `CountDistinctEstimate` may start using a different algorithm in the future.

### Examples

```yql
SELECT
  CountDistinctEstimate(my_column)
FROM my_table;
```

```yql
SELECT
  HyperLogLog(my_column, 4)
FROM my_table;
```



## AGGREGATE_LIST {#agg-list}

Get all column values as a list. When combined with `DISTINCT,` it returns only distinct values. The optional second parameter sets the maximum number of values to be returned. A zero limit value means unlimited.

If you know already that you have few distinct values, use the `AGGREGATE_LIST_DISTINCT` aggregate function to build the same result in memory (that might not be enough for a large number of distinct values).

The order of elements in the result list depends on the implementation and can't be set externally. To return an ordered list, sort the result, for example, with [ListSort](list.md#listsort).

To return a list of multiple values from one line, **DO NOT** use the `AGGREGATE_LIST` function several times, but add all the needed values to a container, for example, via [AsList](basic.md#aslist) or [AsTuple](basic.md#astuple), then pass this container to a single `AGGREGATE_LIST` call.

For example, you can combine it with `DISTINCT` and the function [String::JoinFromList](../udf/list/string.md) (it's an equivalent of `','.join(list)` in Python) to output to a string all the values found in the column after [GROUP BY](../syntax/select/group-by.md).

### Examples

```yql
SELECT
   AGGREGATE_LIST( region ),
   AGGREGATE_LIST( region, 5 ),
   AGGREGATE_LIST( DISTINCT region ),
   AGGREGATE_LIST_DISTINCT( region ),
   AGGREGATE_LIST_DISTINCT( region, 5 )
FROM users
```

```yql
-- An equivalent of GROUP_CONCAT in MySQL
SELECT
    String::JoinFromList(CAST(AGGREGATE_LIST(region, 2) AS List<String>), ",")
FROM users
```

These functions also have a short notation: `AGG_LIST` and `AGG_LIST_DISTINCT`.

{% note alert %}

Execution is **NOT** lazy, so when you use it, be sure that the list has a reasonable size (about a thousand items or less). To stay on the safe side, better use a second optional numeric argument that limits the number of items in the list.

{% endnote %}



## MAX_BY and MIN_BY {#max-min-by}

Return the value of the first argument for the table row where the second argument is minimum/maximum.

You can optionally specify the third argument N that affects behavior if the table has multiple rows with the same minimum or maximum value:

* If N is omitted, the value of one of the rows is returned, and the other rows are discarded.
* If N is specified, the list is returned with all values, but their number can't exceed N. All values after the number are discarded.

When choosing N, we recommend that you don't exceed several hundreds or thousands to avoid issues with the limited memory available on {{ backend_name }} clusters.

If your task needs absolutely all values, and their number is measured in dozens of thousands or more, then instead of those aggregate functions better use `JOIN` on the source table with a subquery doing `GROUP BY + MIN/MAX` on the desired columns of this table.

{% note warning "Attention" %}

If the second argument is always `NULL`, the aggregation result is `NULL`.

{% endnote %}

When you use [aggregation factories](basic.md#aggregationfactory), a `Tuple` containing a value and a key is passed as the first [AGGREGATE_BY](#aggregate-by) argument.

### Examples

```yql
SELECT
  MIN_BY(value, LENGTH(value)),
  MAX_BY(value, key, 100)
FROM my_table;
```

```yql
$min_by_factory = AggregationFactory("MIN_BY");
$max_by_factory = AggregationFactory("MAX_BY", 100);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $min_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $max_by_factory)
FROM my_table;
```



## TOP and BOTTOM {#top-bottom}

Return a list of the maximum/minimum values of an expression. The first argument is an expression, the second argument limits the number of items.

### Examples

```yql
SELECT
    TOP(key, 3),
    BOTTOM(value, 3)
FROM my_table;
```

```yql
$top_factory = AggregationFactory("TOP", 3);
$bottom_factory = AggregationFactory("BOTTOM", 3);

SELECT
    AGGREGATE_BY(key, $top_factory),
    AGGREGATE_BY(value, $bottom_factory)
FROM my_table;
```

## TOP_BY and BOTTOM_BY {#top-bottom-by}

Return a list of values of the first argument for the rows containing the maximum/minimum values of the second argument. The third argument limits the number of items in the list.

When you use [aggregation factories](basic.md#aggregationfactory), a `Tuple` containing a value and a key is passed as the first [AGGREGATE_BY](#aggregate-by) argument. In this case, the limit for the number of items is passed by the second argument at factory creation.

### Examples

```yql
SELECT
    TOP_BY(value, LENGTH(value), 3),
    BOTTOM_BY(value, key, 3)
FROM my_table;
```

```yql
$top_by_factory = AggregationFactory("TOP_BY", 3);
$bottom_by_factory = AggregationFactory("BOTTOM_BY", 3);

SELECT
    AGGREGATE_BY(AsTuple(value, LENGTH(value)), $top_by_factory),
    AGGREGATE_BY(AsTuple(value, key), $bottom_by_factory)
FROM my_table;
```



## TOPFREQ and MODE {#topfreq-mode}

Getting an **approximate** list of the most common values in a column with an estimation of their count. Returns a list of structures with two fields:

* `Value`: the frequently occurring value that was found.
* `Frequency`: An estimated value occurrence in the table.

Required argument: the value itself.

Optional arguments:

1. For `TOPFREQ`, the desired number of items in the result. `MODE` is an alias to `TOPFREQ` with this argument set to 1. For `TOPFREQ`, this argument is also 1 by default.
2. The number of items in the buffer used: lets you trade memory consumption for accuracy. Default: 100.

### Examples

```yql
SELECT
    MODE(my_column),
    TOPFREQ(my_column, 5, 1000)
FROM my_table;
```



## STDDEV and VARIANCE {#stddev-variance}

Standard deviation and variance in a column. Those functions use a [single-pass parallel algorithm](https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm), whose result may differ from the more common methods requiring two passes through the data.

By default, the sample variance and standard deviation are calculated. Several write methods are available:

* with the `POPULATION` suffix/prefix, for example: `VARIANCE_POPULATION`, `POPULATION_VARIANCE` calculates the variance or standard deviation for the population.
* With the `SAMPLE` suffix/prefix or without a suffix, for example, `VARIANCE_SAMPLE`, `SAMPLE_VARIANCE`, `SAMPLE` calculate sample variance and standard deviation.

Several abbreviated aliases are also defined, for example, `VARPOP` or `STDDEVSAMP`.

If all the values passed are `NULL`, it returns `NULL`.

### Examples

```yql
SELECT
  STDDEV(numeric_column),
  VARIANCE(numeric_column)
FROM my_table;
```



## CORRELATION and COVARIANCE {#correlation-covariance}

Correlation and covariance between two columns.

Abbreviated versions are also available: `CORR` or `COVAR`. For covariance, there are also versions with the `SAMPLE`/`POPULATION` suffix that are similar to [VARIANCE](#stddev-variance) above.

Unlike most other aggregate functions, they don't skip `NULL`, but accept it as 0.

When you use [aggregation factories](basic.md#aggregationfactory), a `Tuple` containing two values is passed as the first [AGGREGATE_BY](#aggregate-by) argument.

### Examples

```yql
SELECT
  CORRELATION(numeric_column, another_numeric_column),
  COVARIANCE(numeric_column, another_numeric_column)
FROM my_table;
```

```yql
$corr_factory = AggregationFactory("CORRELATION");

SELECT
    AGGREGATE_BY(AsTuple(numeric_column, another_numeric_column), $corr_factory)
FROM my_table;
```



## PERCENTILE and MEDIAN {#percentile-median}

Calculating percentiles using the amortized version of the [TDigest](https://github.com/tdunning/t-digest) algorithm. `MEDIAN`: An alias for `PERCENTILE(N, 0.5)`.

{% note info "Restriction" %}

The first argument (N) must be a table column name. If you need to bypass this restriction, use a subquery. The restriction is introduced to simplify calculations, since the implementation merges the calls with the same first argument (N) into a single pass.

{% endnote %}

```yql
SELECT
    MEDIAN(numeric_column),
    PERCENTILE(numeric_column, 0.99)
FROM my_table;
```



## HISTOGRAM {#histogram}

Plotting an approximate histogram based on a numeric expression with automatic selection of buckets.

[Auxiliary functions](../udf/list/histogram.md)

### Basic settings

You can limit the number of buckets using an optional argument. The default value is 100. Keep in mind that added accuracy costs you more computing resources and may negatively affect the query execution time. In extreme cases, it may affect your query success.

### Support for weights

You can specify a "weight" for each value used in the histogram. To do this, pass to the aggregate function the second argument with an expression for calculating the weight. The weight of `1.0` is always used by default. If you use non-standard weights, you may also use the third argument to limit the number of buckets.

If you pass two arguments, the meaning of the second argument is determined by its type (if it's an integer literal, it limits the number of buckets, otherwise it's used as a weight).

{% if tech %}

### Algorithms

* [Source whitepaper](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf);

Various modifications of the algorithm are available:

```yql
AdaptiveDistanceHistogram
AdaptiveWeightHistogram
AdaptiveWardHistogram
BlockWeightHistogram
BlockWardHistogram
```

By default, `HISTOGRAM` is a synonym for `AdaptiveWardHistogram`. Both functions are equivalent and interchangeable in all contexts.

The Distance, Weight, and Ward algorithms differ in the formulas that combine two points into one:

```c++
    TWeightedValue CalcDistanceQuality(const TWeightedValue& left, const TWeightedValue& right) {
        return TWeightedValue(right.first - left.first, left.first);
    }

    TWeightedValue CalcWeightQuality(const TWeightedValue& left, const TWeightedValue& right) {
        return TWeightedValue(right.second + left.second, left.first);
    }

    TWeightedValue CalcWardQuality(const TWeightedValue& left, const TWeightedValue& right) {
        const double N1 = left.second;
        const double N2 = right.second;
        const double mu1 = left.first;
        const double mu2 = right.first;
        return TWeightedValue(N1 * N2 / (N1 + N2) * (mu1 - mu2) * (mu1 - mu2), left.first);
    }
```

Difference between Adaptive and Block:

> Contrary to adaptive histogram, block histogram doesn't rebuild bins after each point is added. Instead, it accumulates points and if the amount of points overflows specified limits, it shrinks all the points at once to produce a histogram. Indeed, there exist two limits and two shrinkage operations:
>
> 1. FastGreedyShrink is fast but coarse. It is used to shrink from upper limit to intermediate limit (override FastGreedyShrink to set specific behaviour).
> 2. SlowShrink is slow, but produces finer histogram. It shrinks from the intermediate limit to the actual number of bins in a manner similar to that in adaptive histogram (set CalcQuality in 34constuctor)
> While FastGreedyShrink is used most of the time, SlowShrink is mostly used for histogram finalization

{% endif %}

### If you need an accurate histogram

1. You can use the aggregate functions described below with fixed bucket grids: [LinearHistogram](#linearhistogram) or [LogarithmicHistogram](#linearhistogram).
2. You can calculate the bucket number for each row and apply to it [GROUP BY](../syntax/select/group-by.md).

When you use [aggregation factories](basic.md#aggregationfactory), a `Tuple` containing a value and a weight is passed as the first [AGGREGATE_BY](#aggregate-by) argument.

### Examples

```yql
SELECT
    HISTOGRAM(numeric_column)
FROM my_table;
```

```yql
SELECT
    Histogram::Print(
        HISTOGRAM(numeric_column, 10),
        50
    )
FROM my_table;
```

```yql
$hist_factory = AggregationFactory("HISTOGRAM");

SELECT
    AGGREGATE_BY(AsTuple(numeric_column, 1.0), $hist_factory)
FROM my_table;
```

## LinearHistogram, LogarithmicHistogram, and LogHistogram {#linearhistogram}

Plotting a histogram based on an explicitly specified fixed bucket scale.

Arguments:

1. Expression used to plot the histogram. All the following arguments are optional.
2. Spacing between the `LinearHistogram` buckets or the logarithm base for `LogarithmicHistogram`/`LogHistogram` (those are aliases). In both cases, the default value is 10.
3. Minimum value. By default, it's minus infinity.
4. Maximum value. By default, it's plus infinity.

The format of the result is totally similar to [adaptive histograms](#histogram), so you can use the same [set of auxiliary functions](../udf/list/histogram.md).

If the spread of input values is uncontrollably large, we recommend that you specify the minimum and maximum values to prevent potential failures due to high memory consumption.

### Examples

```yql
SELECT
    LogarithmicHistogram(numeric_column, 2)
FROM my_table;
```



## BOOL_AND, BOOL_OR and BOOL_XOR {#bool-and-or-xor}

### Signature

```yql
BOOL_AND(Bool?)->Bool?
BOOL_OR(Bool?)->Bool?
BOOL_XOR(Bool?)->Bool?
```

Apply the relevant logical operation  (`AND`/`OR`/`XOR`) to all values in a Boolean column or expression.

Unlike most other aggregate functions, these functions **don't skip** `NULL` during aggregation and use the following rules:

- `true AND null == null`
- `false OR null == null`

For `BOOL_AND`:

- If at least one `NULL` value is present, the result is `NULL` regardless of `true` values in the expression.
- If at least one `false` value is present, the result changes to `false` regardless of `NULL` values in the expression.

For `BOOL_OR`:

- If at least one `NULL` value is present, the result changes to `NULL` regardless of `false` values in the expression.
- If at least one `true` value is present, the result changes to `true` regardless of `NULL` values in the expression.

For `BOOL_XOR`:

- The result is `NULL` if any `NULL` is found.

Examples of such behavior can be found below.

To skip `NULL` values during aggregation, use the `MIN`/`MAX` or `BIT_AND`/`BIT_OR`/`BIT_XOR` functions.

### Examples

```yql
$data = [
    <|nonNull: true, nonFalse: true, nonTrue: NULL, anyVal: true|>,
    <|nonNull: false, nonFalse: NULL, nonTrue: NULL, anyVal: NULL|>,
    <|nonNull: false, nonFalse: NULL, nonTrue: false, anyVal: false|>,
];

SELECT
    BOOL_AND(nonNull) as nonNullAnd,      -- false
    BOOL_AND(nonFalse) as nonFalseAnd,    -- NULL
    BOOL_AND(nonTrue) as nonTrueAnd,      -- false
    BOOL_AND(anyVal) as anyAnd,           -- false
    BOOL_OR(nonNull) as nonNullOr,        -- true
    BOOL_OR(nonFalse) as nonFalseOr,      -- true
    BOOL_OR(nonTrue) as nonTrueOr,        -- NULL
    BOOL_OR(anyVal) as anyOr,             -- true
    BOOL_XOR(nonNull) as nonNullXor,      -- true
    BOOL_XOR(nonFalse) as nonFalseXor,    -- NULL
    BOOL_XOR(nonTrue) as nonTrueXor,      -- NULL
    BOOL_XOR(anyVal) as anyXor,           -- NULL
FROM AS_TABLE($data);
```

## BIT_AND, BIT_OR and BIT_XOR {#bit-and-or-xor}

Apply the relevant bitwise operation to all values of a numeric column or expression.

### Examples

```yql
SELECT
    BIT_XOR(unsigned_numeric_value)
FROM my_table;
```



{% if feature_window_functions %}

  ## SessionStart {#session-start}

No arguments. It's allowed only if there is [SessionWindow](../syntax/select/group-by.md#session-window) in [GROUP BY](../syntax/select/group-by.md) / [PARTITION BY](../syntax/select/window.md#partition).
Returns the value of the `SessionWindow` key column. If `SessionWindow` has two arguments, it returns the minimum value of the first argument within the group/section.
In the case of the expanded version `SessionWindow`, it returns the value of the second element from the tuple returned by `<calculate_lambda>`, for which the first tuple element is `True`.



{% endif %}

## AGGREGATE_BY and MULTI_AGGREGATE_BY {#aggregate-by}

Applying an [aggregation factory](basic.md#aggregationfactory) to all values of a column or expression. The `MULTI_AGGREGATE_BY` function requires that the value of a column or expression has a structure, tuple, or list, and applies the factory to each individual element, placing the result in a container of the same format. If different values of a column or expression contain lists of different length, the resulting list will have the smallest of the source lengths.

1. Column, `DISTINCT` column or expression.
2. Factory.

### Examples

```yql
$count_factory = AggregationFactory("COUNT");

SELECT
    AGGREGATE_BY(DISTINCT column, $count_factory) as uniq_count
FROM my_table;

SELECT
    MULTI_AGGREGATE_BY(nums, AggregationFactory("count")) as count,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("min")) as min,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("max")) as max,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("avg")) as avg,
    MULTI_AGGREGATE_BY(nums, AggregationFactory("percentile", 0.9)) as p90
FROM my_table;
```


