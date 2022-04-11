## HAVING {#having}

Фильтрация выборки `SELECT STREAM` по результатам вычисления [агрегатных функций](../../../builtins/aggregation.md). Синтаксис аналогичен [WHERE](../../select_stream.md#where).

**Примеры:**
``` yql
SELECT STREAM
    key
FROM my_table
GROUP BY key, HOP(ts, "PT1M", "PT1M", "PT1M")
HAVING COUNT(value) > 100;
```