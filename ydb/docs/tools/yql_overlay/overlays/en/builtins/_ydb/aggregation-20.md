1. You can use the aggregate functions described below with fixed bucket grids: [LinearHistogram](#linearhistogram) or [LogarithmicHistogram](#linearhistogram).
2. You can calculate the bucket number for each row and apply to it [GROUP BY](../../syntax/select/group-by.md).

When you use [aggregation factories](../basic.md#aggregationfactory), a `Tuple` containing a value and a weight is passed as the first [AGGREGATE_BY](#aggregate-by) argument.

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
