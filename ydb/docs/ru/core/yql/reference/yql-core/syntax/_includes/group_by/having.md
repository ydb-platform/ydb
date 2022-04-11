## HAVING {#having}

Фильтрация выборки `SELECT` по результатам вычисления [агрегатных функций](../../../builtins/aggregation.md). Синтаксис аналогичен конструкции [`WHERE`](../../select.md#where).

**Пример**

``` yql
SELECT
    key
FROM my_table
GROUP BY key
HAVING COUNT(value) > 100;
```
