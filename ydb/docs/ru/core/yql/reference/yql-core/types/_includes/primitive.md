# Примитивные типы данных

Термины «простые», «примитивные» и «элементарные» типы данных используются как синонимы.

## Числовые типы {#numeric}

{% include [datatypes](datatypes_primitive_number.md) %}

## Строковые типы {#string}

{% include [datatypes](datatypes_primitive_string.md) %}

## Дата и время {#datetime}

{% include [datatypes](datatypes_primitive_datetime.md) %}

{% include [x](tz_date_types.md) %}

# Приведение простых типов данных {#cast}

## Явное приведение {#explicit-cast}

Явное приведение при помощи [CAST](../../syntax/expressions.md#cast):

### Приведение к численным типам


Тип | Bool | Int8 | Int16 | Int32 | Int64 | Uint8 | Uint16 | Uint32 | Uint64 | Float | Double | Decimal
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---
**Bool** | — | Да<sup>1</sup> | Да<sup>1</sup> | Да<sup>1</sup> | Да<sup>1</sup> | Да<sup>1</sup> | Да<sup>1</sup> | Да<sup>1</sup> | Да<sup>1</sup> | Да<sup>1</sup> | Да<sup>1</sup> | Нет
**Int8** | Да<sup>2</sup> | — | Да | Да | Да | Да<sup>3</sup> | Да<sup>3</sup> | Да<sup>3</sup> | Да<sup>3</sup> | Да | Да | Да
**Int16** | Да<sup>2</sup> | Да<sup>4</sup> | — | Да | Да | Да<sup>3,4</sup> | Да<sup>3</sup> | Да<sup>3</sup> | Да<sup>3</sup> | Да | Да | Да
**Int32** | Да<sup>2</sup> | Да<sup>4</sup> | Да<sup>4</sup> | — | Да | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3</sup> | Да<sup>3</sup> | Да | Да | Да
**Int64** | Да<sup>2</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | — | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3</sup> | Да | Да | Да
**Uint8** | Да<sup>2</sup> | Да<sup>4</sup> | Да | Да | Да | — | Да | Да | Да | Да | Да | Да
**Uint16** | Да<sup>2</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да | Да | Да<sup>4</sup> | — | Да | Да | Да | Да | Да
**Uint32** | Да<sup>2</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да | Да<sup>4</sup> | Да<sup>4</sup> | — | Да | Да | Да | Да
**Uint64** | Да<sup>2</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | — | Да | Да | Да
**Float** | Да<sup>2</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3,4</sup> | — | Да | Нет
**Double** | Да<sup>2</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да | — | Нет
**Decimal** | Нет | Да | Да | Да | Да | Да | Да | Да | Да | Да | Да | —
**String** | Да | Да | Да | Да | Да | Да | Да | Да | Да | Да | Да | Да
**Utf8** | Да | Да | Да | Да | Да | Да | Да | Да | Да | Да | Да | Да
**Json** | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет
**Yson** | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Да<sup>5</sup> | Нет
**Uuid** | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет | Нет
**Date** | Нет | Да<sup>4</sup> | Да<sup>4</sup> | Да | Да | Да<sup>4</sup> | Да | Да | Да | Да | Да | Да | Нет
**Datetime** | Нет | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да | Да<sup>4</sup> | Да<sup>4</sup> | Да | Да | Да | Да | Нет
**Timestamp** | Нет | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да | Да | Да | Нет
**Interval** | Нет | Да<sup>4</sup> | Да<sup>4</sup> | Да<sup>4</sup> | Да | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3,4</sup> | Да<sup>3</sup> | Да | Да | Нет

<sup>1</sup> `True` преобразуется в `1`, `False` преобразуется в `0`.
<sup>2</sup> Любое значение кроме `0` преобразуется в `True`, `0` преобразуется в `False`.
<sup>3</sup> Возможно только в случае неотрицательного значения.
<sup>4</sup> Возможно только в случае попадания в диапазон допустимых значений.
<sup>5</sup> При помощи встроенной функции [Yson::ConvertTo](../../udf/list/yson.md#ysonconvertto).

### Приведение к типам данных даты и времени

Тип | Date | Datetime | Timestamp | Interval
--- | --- | --- | --- | ---
**Bool** | Нет | Нет | Нет | Нет
**Int8** | Да | Да | Да | Да
**Int16** | Да | Да | Да | Да
**Int32** | Да | Да | Да | Да
**Int64** | Да | Да | Да | Да
**Uint8** | Да | Да | Да | Да
**Uint16** | Да | Да | Да | Да
**Uint32** | Да | Да | Да | Да
**Uint64** | Да | Да | Да | Да
**Float** | Нет | Нет | Нет | Нет
**Double** | Нет | Нет | Нет | Нет
**Decimal** | Нет | Нет | Нет | Нет
**String** | Да | Да | Да | Да
**Utf8** | Да | Да | Да | Да
**Json** | Нет | Нет | Нет | Нет
**Yson** | Нет | Нет | Нет | Нет
**Uuid** | Нет | Нет | Нет | Нет
**Date** | — | Да | Да | Нет
**Datetime** | Да | — | Да | Нет
**Timestamp** | Да | Да | — | Нет
**Interval** | Нет | Нет | Нет | — | —

### Приведение к другим типам данных

Тип | String | Utf8 | Json | Yson | Uuid
--- | --- | --- | --- | --- | ---
**Bool** | Да | Нет | Нет | Нет | Нет |
**Int8** | Да | Нет | Нет | Нет | Нет
**Int16** | Да | Нет | Нет | Нет | Нет
**Int32** | Да | Нет | Нет | Нет | Нет
**Int64** | Да | Нет | Нет | Нет | Нет
**Uint8** | Да | Нет | Нет | Нет | Нет
**Uint16** | Да | Нет | Нет | Нет | Нет
**Uint32** | Да | Нет | Нет | Нет | Нет
**Uint64** | Да | Нет | Нет | Нет | Нет
**Float** | Да | Нет | Нет | Нет | Нет
**Double** | Да | Нет | Нет | Нет | Нет
**Decimal** | Да | Нет | Нет | Нет | Нет
**String** | — | Да | Да | Да | Да
**Utf8** | Да | — | Нет | Нет | Нет
**Json** | Да | Да | — | Нет | Нет
**Yson** | Да<sup>1</sup> | Нет | Нет | Нет | Нет
**Uuid** | Да | Да | Нет | Нет | —
**Date** | Да | Да | Нет | Нет | Нет
**Datetime** | Да | Да | Нет | Нет | Нет
**Timestamp** | Да | Да | Нет | Нет | Нет
**Interval** | Да | Да | Нет | Нет | Нет

<sup>1</sup> При помощи встроенной функции [Yson::ConvertTo](../../udf/list/yson.md#ysonconvertto).

**Примеры**

{% include [x](../../_includes/cast_examples.md) %}

## Неявное приведение {#implicit-cast}

Неявное приведение типов, которое возникает в базовых операциях (`+`, `-`, `*`, `/`, `%`) между разными типами данных. В ячейках таблицы указан тип результата операции, если она возможна:

### Численные типы

При несовпадении численных типов сначала выполняется BitCast обоих аргументов к типу результата, а потом уже операция.

Тип | Int8 | Int16 | Int32 | Int64 | Uint8 | Uint16 | Uint32 | Uint64 | Float | Double
--- | --- | --- | --- | --- | --- | --- | --- | --- | --- | ---
**Int8** | — | `Int16` | `Int32` | `Int64` | `Int8` | `Uint16` | `Uint32` | `Uint64` | `Float` | `Double`
**Int16** | `Int16` | — | `Int32` | `Int64` | `Int16` | `Int16` | `Uint32` | `Uint64` | `Float` | `Double`
**Int32** | `Int32` | `Int32` | — | `Int64` | `Int32` | `Int32` | `Int32` | `Uint64` | `Float` | `Double`
**Int64** | `Int64` | `Int64` | `Int64` | — | `Int64` | `Int64` | `Int64` | `Int64` | `Float` | `Double`
**Uint8** | `Int8` | `Int16` | `Int32` | `Int64` | — | `Uint16` | `Uint32` | `Uint64` | `Float` | `Double`
**Uint16** | `Uint16` | `Int16` | `Int32` | `Int64` | `Uint16` | — | `Uint32` | `Uint64` | `Float` | `Double`
**Uint32** | `Uint32` | `Uint32` | `Int32` | `Int64` | `Uint32` | `Uint32` | — | `Uint64` | `Float` | `Double`
**Uint64** | `Uint64` | `Uint64` | `Uint64` | `Int64` | `Uint64` | `Uint64` | `Uint64` | — | `Float` | `Double`
**Float** | `Float` | `Float` | `Float` | `Float` | `Float` | `Float` | `Float` | `Float` | — | `Double`
**Double** | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | `Double` | —


### Типы даты и времени

Тип | Date | Datetime | Timestamp | Interval | TzDate | TzDatetime | TzTimestamp
--- | --- | --- | --- | --- | --- | --- | ---
**Date** | — | — | — | `Date` | — | — | —
**Datetime** | — | — | — | `Datetime` | — | — | —
**Timestamp** | — | — | — | `Timestamp` | — | — | —
**Interval** | `Date` | `Datetime` | `Timestamp` | — | `TzDate` | `TzDatetime` | `TzTimestamp`
**TzDate** | — | — | — | `TzDate` | — | — | —
**TzDatetime** | — | — | — | `TzDatetime` | — | — | —
**TzTimestamp**  | — | — | — | `TzTimestamp` | — | — | —
