### Сигнатура

```yql
SUM(Unsigned?)->Uint64?
SUM(Signed?)->Int64?
SUM(Interval?)->Interval?
SUM(Decimal(N, M)?)->Decimal(35, M)?
```

Сумма чисел.

В качестве аргумента допустимо произвольное вычислимое выражение с числовым результатом или типом `Interval`.

Целые числа автоматически расширяются до 64 бит, чтобы уменьшить риск переполнения.

```yql
SELECT SUM(value) FROM my_table;
```
