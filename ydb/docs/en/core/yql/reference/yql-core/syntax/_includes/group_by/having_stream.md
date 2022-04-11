## HAVING {#having}

Filtering a `SELECT STREAM` based on the calculation results of [aggregate functions](../../../builtins/aggregation.md). The syntax is similar to [WHERE](../../select_stream.md#where).

**Examples:**

```yql
SELECT STREAM
    key
FROM my_table
GROUP BY key, HOP(ts, "PT1M", "PT1M", "PT1M")
HAVING COUNT(value) > 100;
```

