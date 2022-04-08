## CORRELATION and COVARIANCE {#correlation-covariance}

Correlation and covariance between two columns.

Abbreviated versions are also available: `CORR` or `COVAR`. For covariance, there are also versions with the `SAMPLE`/`POPULATION` suffix that are similar to [VARIANCE](#variance) above.

Unlike most other aggregate functions, they don't skip `NULL`, but accept it as 0.

When you use [aggregation factories](../../basic.md#aggregationfactory), a `Tuple` containing two values is passed as the first [AGGREGATE_BY](#aggregateby) argument.

**Examples**

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

