## CUME_DIST

Возвращает относительную позицию (> 0 и <= 1) строки в рамках [раздела](../../../syntax/window.md#partition). Без аргументов.

**Сигнатура**
```
CUME_DIST()->Double
```


**Примеры**
``` yql
SELECT
    CUME_DIST() OVER w AS dist
FROM my_table
WINDOW w AS (ORDER BY key);
```

