## NANVL {#nanvl}

Заменяет значения `NaN` (not a number) в выражениях типа `Float`, `Double` или [Optional](../../../types/optional.md).

**Сигнатура**
```
NANVL(Float, Float)->Float
NANVL(Double, Double)->Double
```

Аргументы:

1. Выражение, в котором нужно произвести замену.
2. Значение, на которое нужно заменить `NaN`.

Если один из агрументов `Double`, то в выдаче `Double`, иначе `Float`. Если один из агрументов `Optional`, то и в выдаче `Optional`.

**Примеры**
``` yql
SELECT
  NANVL(double_column, 0.0)
FROM my_table;
```
