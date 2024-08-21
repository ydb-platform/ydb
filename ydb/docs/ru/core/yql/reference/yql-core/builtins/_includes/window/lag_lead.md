## LAG / LEAD {#lag-lead}

Доступ к значению из строки [раздела](../../../syntax/window.md#partition), отстающей (`LAG`) или опережающей (`LEAD`) текущую на фиксированное число. В первом аргументе указывается выражение, к которому необходим доступ, а во втором — отступ в строках. Отступ можно не указывать, по умолчанию используется соседняя строка — предыдущая или следующая, соответственно, то есть подразумевается 1. В строках, для которых нет соседей с заданным расстоянием (например `LAG(expr, 3)` в первой и второй строках раздела), возвращается `NULL`.

**Сигнатура**
```
LEAD(T[,Int32])->T?
LAG(T[,Int32])->T?
```

**Примеры**
``` yql
SELECT
   int_value - LAG(int_value) OVER w AS int_value_diff
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT item, odd, LAG(item, 1) OVER w as lag1 FROM (
    SELECT item, item % 2 as odd FROM (
        SELECT AsList(1, 2, 3, 4, 5, 6, 7) as item
    )
    FLATTEN BY item
)
WINDOW w As (
    PARTITION BY odd
    ORDER BY item
);

/* Output:
item	odd	lag1
-------------
2	0	NULL
4	0	2
6	0	4
1	1	NULL
3	1	1
5	1	3
7	1	5
*/

```
