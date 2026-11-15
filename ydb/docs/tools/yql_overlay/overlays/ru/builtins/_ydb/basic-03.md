### Сигнатура

```yql
COALESCE(T?, ..., T)->T
COALESCE(T?, ..., T?)->T?
```

Позволяет передавать потенциально пустые значения в функции, которые не умеют обрабатывать их самостоятельно.

Доступен краткий формат записи в виде оператора `??`. Можно использовать алиас `NVL`.

### Примеры

```yql
SELECT COALESCE(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

```yql
SELECT
  maybe_empty_column ?? "it's empty!"
FROM my_table;
```

```yql
SELECT NVL(
  maybe_empty_column,
  "it's empty!"
) FROM my_table;
```

Все три примера выше эквивалентны.
