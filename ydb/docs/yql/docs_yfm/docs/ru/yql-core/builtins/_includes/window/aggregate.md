## Агрегатные функции {#aggregate-functions}

Все [агрегатные функции](../../aggregation.md) также могут использоваться в роли оконных.
В этом случае на каждой строке оказывается результат агрегации, полученный на множестве строк из [рамки окна](../../../syntax/window.md#frame).

**Примеры:**
``` yql
SELECT
    SUM(int_column) OVER w1 AS running_total,
    SUM(int_column) OVER w2 AS total,
FROM my_table
WINDOW
    w1 AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w2 AS ();
```
