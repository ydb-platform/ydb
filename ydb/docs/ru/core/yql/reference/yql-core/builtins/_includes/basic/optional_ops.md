## Just, Unwrap, Nothing {#optional-ops}

`Just()` - Изменить тип данных значения на [optional](../../../types/optional.md) от текущего типа данных (то есть `T` превращается в `T?`).

**Сигнатура**
```
Just(T)->T?
```

**Примеры**
``` yql
SELECT
  Just("my_string"); --  String?
```

`Unwrap()` - Преобразование значения [optional](../../../types/optional.md) типа данных в соответствующий не-optional тип с ошибкой времени выполнений, если в данных оказался `NULL`. Таким образом, `T?` превращается в `T`.

Если значение не является [optional](../../../types/optional.md), то функция возвращает свой первый аргумент без изменений.

**Сигнатура**
```
Unwrap(T?)->T
Unwrap(T?, Utf8)->T
Unwrap(T?, String)->T
```

Аргументы:

1. Значение для преобразования;
2. Опциональная строка с комментарием для текста ошибки.

Обратная операция — [Just](#just).

**Примеры**
``` yql
$value = Just("value");

SELECT Unwrap($value, "Unexpected NULL for $value");
```

`Nothing()` - Создать пустое значение указанного [Optional](../../../types/optional.md) типа данных.

**Сигнатура**
```
Nothing(Type<T?>)->T?
```

**Примеры**
``` yql
SELECT
  Nothing(String?); -- пустое значение (NULL) с типом String?
```

[Подробнее о ParseType и других функциях для работы с типами данных](../../types.md).
