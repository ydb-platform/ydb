### Сигнатура

```yql
CallableType(Uint32, Type, [Type, ...])->тип вызываемого значения
```

Строит тип вызываемого значения по следующим аргументам:

1. Число опциональных аргументов (если все обязательные — 0).
2. Тип результата.
3. Все последующие аргументы CallableType трактуются как типы аргументов вызываемого значения со сдвигом на два обязательных (например, третий аргумент CallableType описывает тип первого аргумента вызываемого значения).

### Примеры

```yql
SELECT FormatType(CallableType(
  1, -- optional args count
  DataType("Double"), -- result type
  DataType("String"), -- arg #1 type
  OptionalType(DataType("Int64")) -- arg #2 type
)); -- Callable<(String,[Int64?])->Double>
```

## GenericType, UnitType и VoidType {#generictype}

### Сигнатура

```yql
GenericType()->тип
UnitType()->тип
VoidType()->тип
```

Возвращают одноименные [специальные типы данных](../../types/special.md). Аргументов нет, так как они не параметризуются.

### Примеры

```yql
SELECT FormatType(VoidType()); -- Void
```
