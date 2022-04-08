## WITHOUT {#without}

Исключение столбцов из результата `SELECT *`.

**Примеры**

``` yql
SELECT * WITHOUT foo, bar FROM my_table;
```

``` yql
PRAGMA simplecolumns;
SELECT * WITHOUT t.foo FROM my_table AS t
CROSS JOIN (SELECT 1 AS foo) AS v;
```
