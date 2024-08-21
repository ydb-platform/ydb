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
SELECT l, x, f, LAG(l, 1) OVER w as ll1 FROM (
SELECT l, l*l as f, 1 as x FROM(
  SELECT AsList(1,2,3,4,5) as l
)
FLATTEN BY l
)
WINDOW w As (
    PARTITION BY x
    ORDER BY f
);

/* Output:
l	x	f	ll1
-------------
1	1	1	NULL
2	1	4	1
3	1	9	2
4	1	16	3
5	1	25	4
*/

```
