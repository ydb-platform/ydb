# SimplePg

Модуль SimplePg предоставляет доступ к некоторым функциям из кодовой базы PostgreSQL, при этом входные и выходные аргументы адаптированны к [примитивным типам](../../types/primitive.md).
По умолчанию, для вызова этих функций понадобится указание имени модуля в виде `SimplePg::foo`, но если добавить PRAGMA `SimplePg`, то их можно будет использовать в глобальной области видимости как `foo` (при этом гарантируется, что они перекроют встроенную функцию YQL, если таковая есть).

Для многих функций из этого модуля есть аналог среди других функций YQL с лучшей производительностью.

## now

#### Сигнатура

```yql
SimplePg::now() -> Timestamp?
```

Доступна начиная с версии [2025.04](../../changelog/2025.04.md#simple-pg-module).
Аргументов нет.
Возвращает текущее время.
Оригинальная [документация](https://www.postgresql.org/docs/16/functions-datetime.html).
Аналог - [CurrentUtcTimestamp](../../builtins/basic.md#current-utc).

#### Примеры

```yql
PRAGMA SimplePg;
SELECT now();
```

## to_date

#### Сигнатура

```yql
SimplePg::to_date(Utf8?,Utf8?) -> Date32?
```

Доступна начиная с версии [2025.04](../../changelog/2025.04.md#simple-pg-module).

Аргументы:
* Строка с значением даты;
* Строка формата.

Парсит дату согласно строке формата.
Оригинальная [документация](https://www.postgresql.org/docs/16/functions-formatting.html).
Аналог - [DateTime::Parse64](datetime.md#parse).

#### Примеры

```yql
PRAGMA SimplePg;
SELECT to_date('05 Dec 2000', 'DD Mon YYYY'); -- 2000-12-05
```

## to_char

#### Сигнатура

```yql
SimplePg::to_char(AnyPrimitiveType?,Utf8?) -> Utf8?
```

Доступна начиная с версии [2025.04](../../changelog/2025.04.md#simple-pg-module).

Аргументы:
* Значение примитивного типа;
* Строка формата.

Форматирует значение согласно строке формата.
Оригинальная [документация](https://www.postgresql.org/docs/16/functions-formatting.html).
Аналог - [DateTime::Format](datetime.md#format).

#### Примеры

```yql
PRAGMA SimplePg;
SELECT to_char(125.8, '999D9'); -- " 125.8"
SELECT to_char(Timestamp('2002-04-20T17:31:12.66Z'), 'HH12:MI:SS'); -- 05:31:12
```

## date_part

#### Сигнатура

```yql
SimplePg::date_part(Utf8?,Timestamp64?) -> Double?
SimplePg::date_part(Utf8?,Interval64?) -> Double?
```

Доступна начиная с версии [2025.04](../../changelog/2025.04.md#simple-pg-module).

Аргументы:
* Компонента, возможные [значения](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT);
* Таймстемп.

Извлекает из таймстемпа или интервала заданную компоненту.
Оригинальная [документация](https://www.postgresql.org/docs/16/functions-datetime.html).
Аналог - [DateTime::Get](datetime.md#get).

#### Примеры

```yql
PRAGMA SimplePg;
SELECT date_part('hour', Timestamp('2001-02-16T20:38:40Z')); -- 20
SELECT date_part('minute', Interval('PT01H02M03S')); -- 2
```

## date_trunc

#### Сигнатура

```yql
SimplePg::date_trunc(Utf8?,Timestamp64?) -> Timestamp64?
```

Доступна начиная с версии [2025.04](../../changelog/2025.04.md#simple-pg-module).

Аргументы:
* Масштаб, возможные [значения](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC);
* Таймстемп.

Округляет таймстемп на начало заданного масштаба.
Оригинальная [документация](https://www.postgresql.org/docs/16/functions-datetime.html).
Аналог - [DateTime::StartOf](datetime.md#startof) и т.п..

#### Примеры

```yql
PRAGMA SimplePg;
SELECT date_trunc('hour', Timestamp('2001-02-16T20:38:40Z')); -- 2001-02-16 20:00:00
SELECT date_trunc('year', Timestamp('2001-02-16T20:38:40Z')); -- 2001-01-01 00:00:00
```

## floor

#### Сигнатура

```yql
SimplePg::floor(Double?) -> Double?
```

Доступна начиная с версии [2025.04](../../changelog/2025.04.md#simple-pg-module).

Аргументы:
* Значение с плавающей точкой;

Округляет значение до целого числа в меньшую сторону.
Оригинальная [документация](https://www.postgresql.org/docs/16/functions-math.html).
Аналог - [Math::Floor](math.md).

#### Примеры

```yql
PRAGMA SimplePg;
SELECT floor(42.8); -- 42
SELECT floor(-42.8); -- -43
```

## ceil

#### Сигнатура

```yql
SimplePg::ceil(Double?) -> Double?
```

Доступна начиная с версии [2025.04](../../changelog/2025.04.md#simple-pg-module).

Аргументы:
* Значение с плавающей точкой;

Округляет значение до целого числа в большую сторону.
Оригинальная [документация](https://www.postgresql.org/docs/16/functions-math.html).
Аналог - [Math::Ceil](math.md).

#### Примеры

```yql
PRAGMA SimplePg;
SELECT ceil(42.2); -- 43
SELECT ceil(-42.8); -- -42
```

## round

#### Сигнатура

```yql
SimplePg::round(Double?,[Int32?]) -> Double?
```

Доступна начиная с версии [2025.04](../../changelog/2025.04.md#simple-pg-module).

Аргументы:
* Значение с плавающей точкой;
* Опциональное число знаков после запятой (по умолчанию - 0).

Округляет значение до заданного количества десятичных знаков. Используется округление от нуля.
Оригинальная [документация](https://www.postgresql.org/docs/16/functions-math.html).
Аналог - [Math::Round](math.md).

#### Примеры

```yql
PRAGMA SimplePg;
SELECT round(42.4382, 2); -- 42.44
SELECT round(1234.56, -1); -- 1230
```
