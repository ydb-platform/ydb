
## LIMIT и OFFSET {#limit-offset}

`LIMIT` ограничивает вывод указанным количеством строк. По умолчанию вывод не ограничен.

`OFFSET` указывает отступ от начала (в строках). По умолчанию — ноль.

**Примеры**

``` yql
SELECT key FROM my_table
LIMIT 7;
```

``` yql
SELECT key FROM my_table
LIMIT 7 OFFSET 3;
```

``` yql
SELECT key FROM my_table
LIMIT 3, 7; -- эквивалентно предыдущему примеру
```