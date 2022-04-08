## CountDistinctEstimate, HyperLogLog, and HLL {#countdistinctestimate}

Approximating the number of unique values using the [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algorithm. Logically, it does the same thing as [COUNT(DISTINCT ...)](#count), but runs much faster at the cost of some error.

Arguments:

1. Estimated value
2. Accuracy (4 to 18 inclusive, 14 by default).

By selecting accuracy, you can trade added resource and RAM consumption for decreased error.

All the three functions are aliases at the moment, but `CountDistinctEstimate` may start using a different algorithm in the future.

**Examples**

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

