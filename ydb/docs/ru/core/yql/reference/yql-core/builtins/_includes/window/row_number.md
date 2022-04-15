## ROW_NUMBER {#row_number}

Номер строки в рамках [раздела](../../../syntax/window.md#partition). Без аргументов.

**Сигнатура**
```
ROW_NUMBER()->Uint64
```


**Примеры**
``` yql
SELECT
    ROW_NUMBER() OVER w AS row_num
FROM my_table
WINDOW w AS (ORDER BY key);
```
