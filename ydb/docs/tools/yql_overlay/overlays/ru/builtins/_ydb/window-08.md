Возвращает относительную позицию (> 0 и <= 1) строки в рамках [раздела](../../syntax/select/window.md#partition). Без аргументов.

### Сигнатура

```yql
CUME_DIST()->Double
```

### Примеры

```yql
SELECT
    CUME_DIST() OVER w AS dist
FROM my_table
WINDOW w AS (ORDER BY key);
```



## SessionState() {#session-state}

Нестандартная оконная функция `SessionState()` (без аргументов) позволяет получить состояние расчета сессий из [SessionWindow](../../syntax/select/group-by.md#session-window) для текущей строки.
