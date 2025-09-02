
# Базовые встроенные функции

Ниже описаны функции общего назначения, а для специализированных функций есть отдельные статьи: [агрегатные](aggregation.md), [оконные](window.md), а также для работы со [списками](list.md), [словарями](dict.md), [структурами](struct.md), [типами данных](types.md) и [генерацией кода](codegen.md).


## COALESCE {#coalesce}

Перебирает аргументы слева направо и возвращает первый найденный непустой аргумент. Чтобы результат получился гарантированно непустым (не [optional типа](../types/optional.md)), самый правый аргумент должен быть такого типа (зачастую используют литерал). При одном аргументе возвращает его без изменений.

#### Сигнатура

```yql
COALESCE(T?, ..., T)->T
COALESCE(T?, ..., T?)->T?
```

Позволяет передавать потенциально пустые значения в функции, которые не умеют обрабатывать их самостоятельно.

Доступен краткий формат записи в виде оператора `??`. Можно использовать алиас `NVL`.

#### Примеры

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


## LENGTH {#length}

Возвращает длину строки в байтах. Также эта функция доступна под именем `LEN`.

#### Сигнатура

```yql
LENGTH(T)->Uint32
LENGTH(T?)->Uint32?
```

#### Примеры

```yql
SELECT LENGTH("foo");
```

```yql
SELECT LEN("bar");
```

{% note info %}

Для вычисления длины строки в unicode символах можно воспользоваться функцией [Unicode::GetLength](../udf/list/unicode.md).<br/><br/>Для получения числа элементов в списке нужно использовать функцию [ListLength](list.md#listlength).

{% endnote %}


## SUBSTRING {#substring}

Возвращает подстроку.

#### Сигнатура

```yql
Substring(String[, Uint32? [, Uint32?]])->String
Substring(String?[, Uint32? [, Uint32?]])->String?
```

Обязательные аргументы:

* Исходная строка;
* Позиция — отступ от начала строки в байтах (целое число) или `NULL`, означающий «от начала».

Опциональные аргументы:

* Длина подстроки — количество байт, начиная с указанной позиции (целое число, или `NULL` по умолчанию, означающий «до конца исходной строки»).

Индексация с нуля. Если указанные позиция и длина выходят за пределы строки, возвращает пустую строку.
Если входная строка является опциональной, то таким же является и результат.

#### Примеры

```yql
SELECT SUBSTRING("abcdefg", 3, 1); -- d
```

```yql
SELECT SUBSTRING("abcdefg", 3); -- defg
```

```yql
SELECT SUBSTRING("abcdefg", NULL, 3); -- abc
```


## FIND {#find}

Поиск позиции подстроки в строке.

#### Сигнатура

```yql
Find(String, String[, Uint32?])->Uint32?
Find(String?, String[, Uint32?])->Uint32?
Find(Utf8, Utf8[, Uint32?])->Uint32?
Find(Utf8?, Utf8[, Uint32?])->Uint32?
```

Обязательные аргументы:

* Исходная строка;
* Искомая подстрока.

Опциональные аргументы:

* Позиция — в байтах, с которой начинать поиск (целое число, или `NULL` по умолчанию, означающий «от начала исходной строки»).

Возвращает первую найденную позицию подстроки или `NULL`, означающий что искомая подстрока с указанной позиции не найдена.

#### Примеры

```yql
SELECT FIND("abcdefg_abcdefg", "abc"); -- 0
```

```yql
SELECT FIND("abcdefg_abcdefg", "abc", 1); -- 8
```

```yql
SELECT FIND("abcdefg_abcdefg", "abc", 9); -- null
```

## RFIND {#rfind}

Обратный поиск позиции подстроки в строке, от конца к началу.

#### Сигнатура

```yql
RFind(String, String[, Uint32?])->Uint32?
RFind(String?, String[, Uint32?])->Uint32?
RFind(Utf8, Utf8[, Uint32?])->Uint32?
RFind(Utf8?, Utf8[, Uint32?])->Uint32?
```

Обязательные аргументы:

* Исходная строка;
* Искомая подстрока.

Опциональные аргументы:

* Позиция — в байтах, с которой начинать поиск (целое число, или `NULL` по умолчанию, означающий «от конца исходной строки»).

Возвращает первую найденную позицию подстроки или `NULL`, означающий, что искомая подстрока с указанной позиции не найдена.

#### Примеры

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd"); -- 9
```

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd", 8); -- 1
```

```yql
SELECT RFIND("abcdefg_abcdefg", "bcd", 0); -- null
```


## StartsWith, EndsWith {#starts_ends_with}

Проверка наличия префикса или суффикса в строке.

#### Сигнатуры

```yql
StartsWith(T str, U prefix)->Bool[?]

EndsWith(T str, U suffix)->Bool[?]
```

Обязательные аргументы:

* Исходная строка;
* Искомая подстрока.

Аргументы должны иметь тип `String`/`Utf8` (или опциональный `String`/`Utf8`) либо строковый PostgreSQL тип (`PgText`/`PgBytea`/`PgVarchar`).
Результатом функции является опциональный Bool, за исключением случая, когда оба аргумента неопциональные – в этом случае возвращается Bool.

#### Примеры

```yql
SELECT StartsWith("abc_efg", "abc") AND EndsWith("abc_efg", "efg"); -- true
```

```yql
SELECT StartsWith("abc_efg", "efg") OR EndsWith("abc_efg", "abc"); -- false
```

```yql
SELECT StartsWith("abcd", NULL); -- null
```

```yql
SELECT EndsWith(NULL, Utf8("")); -- null
```

```yql
SELECT StartsWith("abc_efg"u, "abc"p) AND EndsWith("abc_efg", "efg"pv); -- true
```


## IF {#if}

Проверяет условие `IF(condition_expression, then_expression, else_expression)`.

Является упрощенной альтернативой для [CASE WHEN ... THEN ... ELSE ... END](../syntax/expressions.md#case).

#### Сигнатура

```yql
IF(Bool, T, T)->T
IF(Bool, T)->T?
```

Аргумент `else_expression` можно не указывать. В этом случае, если условие ложно (`condition_expression` вернул `false`), будет возвращено пустое значение с типом, соответствующим `then_expression` и допускающим значение `NULL`. Таким образом, у результата получится [optional тип данных](../types/optional.md).

#### Примеры

```yql
SELECT
  IF(foo > 0, bar, baz) AS bar_or_baz,
  IF(foo > 0, foo) AS only_positive_foo
FROM my_table;
```


## NANVL {#nanvl}

Заменяет значения `NaN` (not a number) в выражениях типа `Float`, `Double` или [Optional](../types/optional.md).

#### Сигнатура

```yql
NANVL(Float, Float)->Float
NANVL(Double, Double)->Double
```

Аргументы:

1. Выражение, в котором нужно произвести замену.
2. Значение, на которое нужно заменить `NaN`.

Если один из агрументов `Double`, то в выдаче `Double`, иначе `Float`. Если один из агрументов `Optional`, то и в выдаче `Optional`.

#### Примеры

```yql
SELECT
  NANVL(double_column, 0.0)
FROM my_table;
```


## Random... {#random}

Генерирует псевдослучайное число:

* `Random()` — число с плавающей точкой (Double) от 0 до 1;
* `RandomNumber()` — целое число из всего диапазона Uint64;
* `RandomUuid()` — [Uuid version 4](https://tools.ietf.org/html/rfc4122#section-4.4).

#### Сигнатуры

```yql
Random(T1[, T2, ...])->Double
RandomNumber(T1[, T2, ...])->Uint64
RandomUuid(T1[, T2, ...])->Uuid
```

При генерации случайных чисел аргументы не используются и нужны исключительно для управления моментом вызова. В каждый момент вызова возвращается новое случайное число. Поэтому:

* Повторный вызов Random в рамках **одного запроса** при идентичном наборе аргументов возвращает тот же самый набор случайных чисел. Важно понимать, что речь именно про сами аргументы (текст между круглыми скобками), а не их значения.

* Вызовы Random с одним и тем же набором аргументов в **разных запросах** вернут разные наборы случайных чисел.

{% note warning %}

Если Random используется в [именованных выражениях](../syntax/expressions.md#named-nodes), то его однократное вычисление не гарантируется. В зависимости от оптимизаторов и среды исполнения он может посчитаться как один раз, так и многократно. Для гарантированного однократного подсчета необходимо в этом случае материализовать именованное выражение в таблицу.

{% endnote %}

Сценарии использования:

* `SELECT RANDOM(1);` — получить одно случайное значение на весь запрос и несколько раз его использовать (чтобы получить несколько, можно передать разные константы любого типа);
* `SELECT RANDOM(1) FROM table;` — одно и то же случайное число на каждую строку таблицы;
* `SELECT RANDOM(1), RANDOM(2) FROM table;` — по два случайных числа на каждую строку таблицы, все числа в каждой из колонок одинаковые;
* `SELECT RANDOM(some_column) FROM table;` — разные случайные числа на каждую строку таблицы;
* `SELECT RANDOM(some_column), RANDOM(some_column) FROM table;` — разные случайные числа на каждую строку таблицы, но в рамках одной строки — два одинаковых числа;
* `SELECT RANDOM(some_column), RANDOM(some_column + 1) FROM table;` или `SELECT RANDOM(some_column), RANDOM(other_column) FROM table;` — две колонки, и все с разными числами.

#### Примеры

```yql
SELECT
    Random(key) -- [0, 1)
FROM my_table;
```

```yql
SELECT
    RandomNumber(key) -- [0, Max<Uint64>)
FROM my_table;
```

```yql
SELECT
    RandomUuid(key) -- Uuid version 4
FROM my_table;
```

```yql
SELECT
    RANDOM(column) AS rand1,
    RANDOM(column) AS rand2, -- same as rand1
    RANDOM(column, 1) AS randAnd1, -- different from rand1/2
    RANDOM(column, 2) AS randAnd2 -- different from randAnd1
FROM my_table;
```


## Udf {#udf}

Строит `Callable` по заданному названию функции и опциональным `external user types`, `RunConfig` и `TypeConfig`.

* `Udf(Foo::Bar)` — Функция `Foo::Bar` без дополнительных параметров.
* `Udf(Foo::Bar)(1, 2, 'abc')` — Вызов udf `Foo::Bar`.
* `Udf(Foo::Bar, Int32, @@{"device":"AHCI"}@@ as TypeConfig")(1, 2, 'abc')` — Вызов udf `Foo::Bar` с дополнительным типом `Int32` и указанным `TypeConfig`.
* `Udf(Foo::Bar, "1e9+7" as RunConfig")(1, 'extended' As Precision)` — Вызов udf `Foo::Bar` с указанным `RunConfig` и именоваными параметрами.
* `Udf(Foo::Bar, $parent as Depends)` — Вызов udf `Foo::Bar` с указанием зависимости вычисления от заданного узла - с версии [2025.03](../changelog/2025.03.md).

#### Сигнатуры

```yql
Udf(Callable[, T1, T2, ..., T_N][, V1 as TypeConfig][,V2 as RunConfig]])->Callable
```

Где `T1`, `T2`, и т. д. -- дополнительные (`external`) пользовательские типы.

#### Примеры

```yql
$IsoParser = Udf(DateTime2::ParseIso8601);
SELECT $IsoParser("2022-01-01");
```

```yql
SELECT Udf(Unicode::IsUtf)("2022-01-01")
```

```yql
$config = @@{
    "name":"MessageFoo",
    "meta": "..."
}@@;
SELECT Udf(Protobuf::TryParse, $config As TypeConfig)("")
```


## CurrentUtc... {#current-utc}

`CurrentUtcDate()`, `CurrentUtcDatetime()` и `CurrentUtcTimestamp()` - получение текущей даты и/или времени в UTC. Тип данных результата указан в конце названия функции.

#### Сигнатуры

```yql
CurrentUtcDate(...)->Date
CurrentUtcDatetime(...)->Datetime
CurrentUtcTimestamp(...)->Timestamp
```

Аргументы опциональны и работают по тому же принципу, что и у [RANDOM](#random).

#### Примеры

```yql
SELECT CurrentUtcDate();
```

```yql
SELECT CurrentUtcTimestamp(TableRow()) FROM my_table;
```


## CurrentTz... {#current-tz}

`CurrentTzDate()`, `CurrentTzDatetime()` и `CurrentTzTimestamp()` - получение текущей даты и/или времени в указанной в первом аргументе [IANA временной зоне](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones). Тип данных результата указан в конце названия функции.

#### Сигнатуры

```yql
CurrentTzDate(String, ...)->TzDate
CurrentTzDatetime(String, ...)->TzDatetime
CurrentTzTimestamp(String, ...)->TzTimestamp
```

Последующие аргументы опциональны и работают по тому же принципу, что и у [RANDOM](#random).

#### Примеры

```yql
SELECT CurrentTzDate("Europe/Moscow");
```

```yql
SELECT CurrentTzTimestamp("Europe/Moscow", TableRow()) FROM my_table;
```

## AddTimezone

Добавление информации о временной зоне к дате/времени, заданных в UTC. При выводе в результате `SELECT` или после `CAST` в `String` будут применены правила временной зоны по вычислению смещения времени.

#### Сигнатура

```yql
AddTimezone(Date, String)->TzDate
AddTimezone(Date?, String)->TzDate?
AddTimezone(Datetime, String)->TzDatetime
AddTimezone(Datetime?, String)->TzDatetime?
AddTimezone(Timestamp, String)->TzTimestamp
AddTimezone(Timestamp?, String)->TzTimestamp?
```

Аргументы:

1. Дата - тип `Date`/`Datetime`/`Timestamp`;
2. [IANA имя временной зоны](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones).

Тип результата - `TzDate`/`TzDatetime`/`TzTimestamp`, в зависимости от типа данных входа.

#### Примеры

```yql
SELECT AddTimezone(Datetime("2018-02-01T12:00:00Z"), "Europe/Moscow");
```

## RemoveTimezone

Удаление информации о временной зоне и перевод в дату/время, заданные в UTC.

#### Сигнатура

```yql
RemoveTimezone(TzDate)->Date
RemoveTimezone(TzDate?)->Date?
RemoveTimezone(TzDatetime)->Datetime
RemoveTimezone(TzDatetime?)->Datetime?
RemoveTimezone(TzTimestamp)->Timestamp
RemoveTimezone(TzTimestamp?)->Timestamp?
```

Аргументы:

1. Дата - тип `TzDate`/`TzDatetime`/`TzTimestamp`.

Тип результата - `Date`/`Datetime`/`Timestamp`, в зависимости от типа данных входа.

#### Примеры

```yql
SELECT RemoveTimezone(TzDatetime("2018-02-01T12:00:00,Europe/Moscow"));
```


## Version {#version}

`Version()` возвращает строку, описывающую текущую версию узла, обрабатывающего запрос. В некоторых случаях, например, во время постепенного обновлений кластера, она может возвращать разные строки в зависимости от того, какой узел обрабатывает запрос. Функция не принимает никаких аргументов.

#### Примеры

```yql
SELECT Version();
```

## CurrentLanguageVersion {#current-language-version}

`CurrentLanguageVersion()` возвращает строку, описывающую текущую версию языка, выбранного для текущего запроса, если она определена, либо пустую строку.

#### Примеры

```yql
SELECT CurrentLanguageVersion();
```


## MAX_OF, MIN_OF, GREATEST и LEAST {#max-min}

Возвращает минимальный или максимальный среди N аргументов. Эти функции позволяют не использовать стандартную для SQL конструкцию `CASE WHEN a < b THEN a ELSE b END`, которая была бы особенно громоздкой для N больше двух.

#### Сигнатуры

```yql
MIN_OF(T[,T,...})->T
MAX_OF(T[,T,...})->T
```

Типы аргументов должны быть приводимы друг к другу и могут допускать значение `NULL`.

`GREATEST` является синонимом к `MAX_OF`, а `LEAST` — к `MIN_OF`.

#### Примеры

```yql
SELECT MIN_OF(1, 2, 3);
```


## AsTuple, AsStruct, AsList, AsDict, AsSet, AsListStrict, AsDictStrict и AsSetStrict {#as-container}

Создает контейнеры соответствующих типов. Также доступна [операторная запись](#containerliteral) литералов контейнеров.

Особенности:

* Элементы контейнеров передаются через аргументы, таким образом число элементов результирующего контейнера равно числу переданных аргументов, кроме случая, когда повторяются ключи словаря.
* В `AsTuple` и `AsStruct` могут быть вызваны без аргументов, а также аргументы могут иметь разные типы.
* Имена полей в `AsStruct` задаются через `AsStruct(field_value AS field_name)`.
* Для создания списка требуется хотя бы один аргумент, если нужно вывести типы элементов. Для создания пустого списка с заданным типом элементов используется функция [ListCreate](list.md#listcreate). Можно создать пустой список как вызов `AsList()` без аргументов, в этом случае это выражение будет иметь тип `EmptyList`.
* Для создания словаря требуется хотя бы один аргумент, если нужно вывести типы элементов. Для создания пустого словаря с заданным типом элементов используется функция [DictCreate](dict.md#dictcreate). Можно создать пустой словарь как вызов `AsDict()` без аргументов, в этом случае это выражение будет иметь тип `EmptyDict`.
* Для создания множества требуется хотя бы один аргумент, если нужно вывести типы элементов. Для создания пустого множества с заданным типом элементов используется функция [SetCreate](dict.md#setcreate). Можно создать пустое множество как вызов `AsSet()` без аргументов, в этом случае это выражение будет иметь тип `EmptyDict`.
* `AsList` выводит общий тип элементов списка. При несовместимых типах генерируется ошибка типизации.
* `AsDict` выводит раздельно общие типы ключей и значений. При несовместимых типах генерируется ошибка типизации.
* `AsSet` выводит общие типы ключей. При несовместимых типах генерируется ошибка типизации.
* `AsListStrict`, `AsDictStrict`, `AsSetStrict` требуют одинакового типа для аргументов.
* В `AsDict` и `AsDictStrict` в качестве аргументов ожидаются `Tuple` из двух элементов: ключ и значение, соответственно. Если ключи повторяются, в словаре останется только значение для первого ключа.
* В `AsSet` и `AsSetStrict` в качестве аргументов ожидаются ключи.

#### Примеры

```yql
SELECT
  AsTuple(1, 2, "3") AS `tuple`,
  AsStruct(
    1 AS a,
    2 AS b,
    "3" AS c
  ) AS `struct`,
  AsList(1, 2, 3) AS `list`,
  AsDict(
    AsTuple("a", 1),
    AsTuple("b", 2),
    AsTuple("c", 3)
  ) AS `dict`,
  AsSet(1, 2, 3) AS `set`
```


## Литералы контейнеров {#containerliteral}

Для некоторых контейнеров возможна операторная форма записи их литеральных значений:

* Кортеж — `(value1, value2...)`;
* Структура — `<|name1: value1, name2: value2...|>`;
* Список — `[value1, value2,...]`;
* Словарь — `{key1: value1, key2: value2...}`;
* Множество — `{key1, key2...}`.

Во всех случаях допускается незначащая хвостовая запятая. Для кортежа с одним элементом эта запятая является обязательной - `(value1,)`.
Для имен полей в литерале структуры допускается использовать выражение, которое можно посчитать в evaluation time, например, строковые литералы, а также идентификаторы (в том числе в backticks).

Для списка внутри используется функция [AsList](#as-container), словаря - [AsDict](#as-container), множества - [AsSet](#as-container), кортежа - [AsTuple](#as-container), структуры - [AsStruct](#as-container).

#### Примеры

```yql
$name = "computed " || "member name";
SELECT
  (1, 2, "3") AS `tuple`,
  <|
    `complex member name`: 2.3,
    b: 2,
    $name: "3",
    "inline " || "computed member name": false
  |> AS `struct`,
  [1, 2, 3] AS `list`,
  {
    "a": 1,
    "b": 2,
    "c": 3,
  } AS `dict`,
  {1, 2, 3} AS `set`
```


## Variant {#variant}

`Variant()` создает значение варианта над кортежем или структурой.

#### Сигнатура

```yql
Variant(T, String, Type<Variant<...>>)->Variant<...>
```

Аргументы:

* Значение
* Строка с именем поля или индексом кортежа
* Тип варианта

#### Пример

```yql
$var_type = Variant<foo: Int32, bar: Bool>;

SELECT
   Variant(6, "foo", $var_type) as Variant1Value,
   Variant(false, "bar", $var_type) as Variant2Value;
```

## AsVariant {#asvariant}

`AsVariant()` создает значение [варианта над структурой](../types/containers.md) с одним полем. Это значение может быть неявно преобразовано к любому варианту над структурой, в которой совпадает для этого имени поля тип данных и могут быть дополнительные поля с другими именами.

#### Сигнатура

```yql
AsVariant(T, String)->Variant
```

Аргументы:

* Значение
* Строка с именем поля

#### Пример

```yql
SELECT
   AsVariant(6, "foo") as VariantValue
```

## Visit, VisitOrDefault {#visit}

Обрабатывает возможные значения варианта, представленного структурой или кортежем, с использованием предоставленных функций-обработчиков для каждого из его полей/элементов.

#### Сигнатура

```yql
Visit(Variant<key1: K1, key2: K2, ...>, K1->R AS key1, K2->R AS key2, ...)->R
Visit(Variant<K1, K2, ...>, K1->R, K2->R, ...)->R

VisitOrDefault(Variant<K1, K2, ...>{Flags:AutoMap}, R, [K1->R, [K2->R, ...]])->R
VisitOrDefault(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap}, R, [K1->R AS key1, [K2->R AS key2, ...]])->R
```

### Аргументы

* Для варианта над структурой функция принимает сам вариант в качестве позиционного аргумента и по одному именованному аргументу-обработчику для каждого поля этой структуры.
* Для варианта над кортежем функция принимает сам вариант и по одному обработчику на каждый элемент кортежа в качестве позиционных аргументов.
* Модификация `VisitOrDefault` принимает дополнительный позиционный аргумент (на втором месте), представляющий значение по умолчанию, и позволяет не указывать некоторые обработчики.

#### Пример

```yql
$vartype = Variant<num: Int32, flag: Bool, str: String>;
$handle_num = ($x) -> { return 2 * $x; };
$handle_flag = ($x) -> { return If($x, 200, 10); };
$handle_str = ($x) -> { return Unwrap(CAST(LENGTH($x) AS Int32)); };

$visitor = ($var) -> { return Visit($var, $handle_num AS num, $handle_flag AS flag, $handle_str AS str); };
SELECT
    $visitor(Variant(5, "num", $vartype)),                -- 10
    $visitor(Just(Variant(True, "flag", $vartype))),      -- Just(200)
    $visitor(Just(Variant("somestr", "str", $vartype))),  -- Just(7)
    $visitor(Nothing(OptionalType($vartype))),            -- Nothing(Optional<Int32>)
    $visitor(NULL)                                        -- NULL
;
```

## VariantItem {#variantitem}

Возвращает значение гомогенного варианта (т.е. содержащего поля/элементы одного типа).

#### Сигнатура

```yql
VariantItem(Variant<key1: K, key2: K, ...>{Flags:AutoMap})->K
VariantItem(Variant<K, K, ...>{Flags:AutoMap})->K
```

#### Пример

```yql
$vartype1 = Variant<num1: Int32, num2: Int32, num3: Int32>;
SELECT
    VariantItem(Variant(7, "num2", $vartype1)),          -- 7
    VariantItem(Just(Variant(5, "num1", $vartype1))),    -- Just(5)
    VariantItem(Nothing(OptionalType($vartype1))),       -- Nothing(Optional<Int32>)
    VariantItem(NULL)                                    -- NULL
;
```

## Way {#way}

Возвращает активное поле (активный индекс) варианта поверх структуры (кортежа).

#### Сигнатура

```yql
Way(Variant<key1: K1, key2: K2, ...>{Flags:AutoMap})->Utf8
Way(Variant<K1, K2, ...>{Flags:AutoMap})->Uint32
```

#### Пример

```yql
$vr = Variant(1, "0", Variant<Int32, String>);
$vrs = Variant(1, "a", Variant<a:Int32, b:String>);


SELECT Way($vr);  -- 0
SELECT Way($vrs); -- "a"

```

## DynamicVariant {#dynamic_variant}

Создает экзмепляр гомогенного варианта (т.е. содержащего поля/элементы одного типа), причем индекс или поле варианта можно задавать динамически. При несуществующем индексе или имени поля будет возвращен `NULL`.
Обратная функция - [VariantItem](#variantitem).

#### Сигнатура

```yql
DynamicVariant(item:T,index:Uint32?,Variant<T, T, ...>)->Optional<Variant<T, T, ...>>
DynamicVariant(item:T,index:Utf8?,Variant<key1: T, key2: T, ...>)->Optional<Variant<key1: T, key2: T, ...>>
```

#### Пример

```yql
$dt = Int32;
$tvt = Variant<$dt,$dt>;
SELECT ListMap([(10,0u),(20,2u),(30,NULL)],($x)->(DynamicVariant($x.0,$x.1,$tvt))); -- [0: 10,NULL,NULL]

$dt = Int32;
$svt = Variant<x:$dt,y:$dt>;
SELECT ListMap([(10,'x'u),(20,'z'u),(30,NULL)],($x)->(DynamicVariant($x.0,$x.1,$svt))); -- [x: 10,NULL,NULL]

```

## Enum {#enum}

`Enum()` создает значение перечисления.

#### Сигнатура

```yql
Enum(String, Type<Enum<...>>)->Enum<...>
```

Аргументы:

* Строка с именем поля
* Тип перечисления

#### Пример

```yql
$enum_type = Enum<Foo, Bar>;
SELECT
   Enum("Foo", $enum_type) as Enum1Value,
   Enum("Bar", $enum_type) as Enum2Value;
```

## AsEnum {#asenum}

`AsEnum()` создает значение [перечисления](../types/containers.md) с одним элементом. Это значение может быть неявно преобразовано к любому перечислению, содержащему такое имя.

#### Сигнатура

```yql
AsEnum(String)->Enum<'tag'>
```

Аргументы:

* Строка с именем элемента перечисления

#### Пример

```yql
SELECT
   AsEnum("Foo");
```


## AsTagged, Untag {#as-tagged}

Оборачивает значение в [Tagged тип данных](../types/special.md) с указанной меткой с сохранением физического типа данных. `Untag` — обратная операция.

#### Сигнатура

```yql
AsTagged(T, tagName:String)->Tagged<T,tagName>
AsTagged(T?, tagName:String)->Tagged<T,tagName>?

Untag(Tagged<T, tagName>)->T
Untag(Tagged<T, tagName>?)->T?
```

Обязательные аргументы:

1. Значение произвольного типа;
2. Имя метки.

Возвращает копию значения из первого аргумента с указанной меткой в типе данных.

Примеры сценариев использования:

* Возвращение на клиент для отображения в веб-интерфейсе медиа-файлов из base64-encoded строк.
* Защита на границах вызова UDF от передачи некорректных значений;
* Дополнительные уточнения на уровне типов возвращаемых колонок.

## TablePath {#tablepath}

Доступ к текущему имени таблицы, что бывает востребовано при использовании [CONCAT](../syntax/select/concat.md#concat) и других подобных механизмов.

#### Сигнатура

```yql
TablePath()->String
```

Аргументов нет. Возвращает строку с полным путём, либо пустую строку и warning при использовании в неподдерживаемом контексте (например, при работе с подзапросом или диапазоном из 1000+ таблиц).

{% note info %}

Функции [TablePath](#tablepath), [TableName](#tablename) и [TableRecordIndex](#tablerecordindex) не работают для временных и анонимных таблиц (возвращают пустую строку или 0 для [TableRecordIndex](#tablerecordindex)).
Данные функции вычисляются в момент [выполнения](../syntax/select/index.md#selectexec) проекции в `SELECT`, и к этому моменту текущая таблица уже может быть временной.
Чтобы избежать такой ситуации, следует поместить вычисление этих функций в подзапрос, как это сделано во втором примере ниже.

{% endnote %}

#### Примеры

```yql
SELECT TablePath() FROM CONCAT(table_a, table_b);
```

```yql
SELECT key, tpath_ AS path FROM (SELECT a.*, TablePath() AS tpath_ FROM RANGE(`my_folder`) AS a)
WHERE key IN $subquery;
```

## TableName {#tablename}

Получить имя таблицы из пути к таблице. Путь можно получить через функцию [TablePath](#tablepath).

#### Сигнатура

```yql
TableName()->String
TableName(String)->String
TableName(String, String)->String
```

Необязательные аргументы:

* путь к таблице, по умолчанию используется `TablePath()` (также см. его ограничения);
* указание системы ("yt"), по правилам которой выделяется имя таблицы. Указание системы нужно только в том случае, если с помощью [USE](../syntax/use.md) не указан текущий кластер.

#### Примеры

```yql
USE hahn;
SELECT TableName() FROM CONCAT(table_a, table_b);
```

## TableRecordIndex {#tablerecordindex}

Доступ к текущему порядковому номеру строки в исходной физической таблице, **начиная с 1** (зависит от реализации хранения).

#### Сигнатура

```yql
TableRecordIndex()->Uint64
```

Аргументов нет. При использовании в сочетании с [CONCAT](../syntax/select/concat.md#concat) и другими подобными механизмами нумерация начинается заново для каждой таблицы на входе. В случае использования в некорректном контексте возвращает 0.

#### Пример

```yql
SELECT TableRecordIndex() FROM my_table;
```


## TableRow, JoinTableRow {#tablerow}

Получение всей строки таблицы целиком в виде структуры. Аргументов нет. `JoinTableRow` в случае `JOIN`-ов всегда возвращает структуру с префиксами таблиц.

#### Сигнатура

```yql
TableRow()->Struct
```

#### Пример

```yql
SELECT TableRow() FROM my_table;
```

## FileContent и FilePath {#file-content-path}

#### Сигнатуры

```yql
FilePath(String)->String
FileContent(String)->String
```

Аргумент `FileContent` и `FilePath` — строка с алиасом.

#### Примеры

```yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```
## FolderPath {#folderpath}

Получение пути до корня директории с несколькими «приложенными» файлами с указанным общим префиксом.

#### Сигнатура

```yql
FolderPath(String)->String
```

Аргумент — строка с префиксом среди алиасов.

Также см. [PRAGMA File](../syntax/pragma/file.md#file) и [PRAGMA Folder](../syntax/pragma/file.md#folder).

#### Примеры

```yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- в директории по возвращённому пути будут
                          -- находиться файлы 1.txt и 2.txt, скачанные по указанным выше ссылкам
```

## ParseFile

Получить из приложенного текстового файла список значений. Может использоваться в сочетании с [IN](../syntax/expressions.md#in) и прикладыванием файла по URL.

Поддерживается только один формат файла — по одному значению на строку.

#### Сигнатура

```yql
ParseFile(String, String)->List<T>
```

Два обязательных аргумента:

1. Тип ячейки списка: поддерживаются только строки и числовые типы;
2. Имя приложенного файла.

{% note info %}

Возвращаемое значение - ленивый список. Для многократного использования его нужно обернуть в функцию [ListCollect](list.md#listcollect)

{% endnote %}

#### Примеры

```yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```

```yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt");
```

## Ensure... {#ensure}

Проверка пользовательских условий:

* `Ensure()` — проверка верности предиката во время выполнения запроса.
* `EnsureType()` — проверка точного соответствия типа выражения указанному.
* `EnsureConvertibleTo()` — мягкая проверка соответствия типа выражения, работающая по тем же правилам, что и неявное приведение типов.

Если проверка не прошла успешно, то весь запрос завершается с ошибкой.

#### Сигнатуры

```yql
Ensure(T, Bool, String)->T
EnsureType(T, Type<T>, String)->T
EnsureConvertibleTo(T, Type<T>, String)->T
```

Аргументы:

1. Выражение, которое станет результатом вызова функции в случае успеха проверки. Оно же подвергается проверке на тип данных в соответствующих функциях.
2. В Ensure — булевый предикат, который проверяется на `true`. В остальных функциях — тип данных, который может быть получен через [предназначенные для этого функции](types.md), либо строковый литерал с [текстовым описанием типа](../types/type_string.md).
3. Опциональная строка с комментарием к ошибке, которая попадет в общее сообщение об ошибке при завершении запроса. Для проверок типов не может использовать сами данные, так как они выполняются на этапе валидации запроса, а для Ensure — может быть произвольным выражением.

Для проверки условий по финальному результату вычисления Ensure удобно использовать в сочетании с [DISCARD SELECT](../syntax/discard.md).


#### Примеры

```yql
SELECT Ensure(
    value,
    value < 100,
    "value out or range"
) AS value FROM my_table;
```

```yql
SELECT EnsureType(
    value,
    TypeOf(other_value),
    "expected value and other_value to be of same type"
) AS value FROM my_table;
```

```yql
SELECT EnsureConvertibleTo(
    value,
    Double?,
    "expected value to be numeric"
) AS value FROM my_table;
```


## AssumeStrict {#assumestrict}

#### Сигнатура

```yql
AssumeStrict(T)->T
```

Функция `AssumeStrict` возвращает свой аргумент. Использование этой функции – способ сказать оптимизатору YQL, что выражение в аргументе является *строгим*, т.е. свободным от ошибок времени выполнения.
Большинство встроенных функций и операторов YQL являются строгими, но есть исключения – например [Unwrap](#optional-ops) и [Ensure](#ensure).
Кроме того, нестрогим выражением считается вызов UDF.

Если есть уверенность, что при вычислении выражения ошибок времени выполнения на самом деле не возникает, то имеет смысл использовать `AssumeStrict`.

#### Пример

```yql
SELECT * FROM T1 AS a JOIN T2 AS b USING(key)
WHERE AssumeStrict(Unwrap(CAST(a.key AS Int32))) == 1;
```

В данном примере мы считаем что все значения текстовой колонки `a.key` в таблице `T1` являются валидными числами, поэтому Unwrap не приводит к ошибке.
При налиичии `AssumeStrict` оптимизатор сможет выполнить сначала фильтрацию, а потом JOIN.
Без `AssumeStrict` такая оптимизация не выполняется – оптимизатор обязан учитывать ситуацию, при которой в колонке `a.key` есть нечисловые значения, которые отфильтровываются `JOIN`ом.


## Likely {#likely}

#### Сигнатура

```yql
Likely(Bool)->Bool
Likely(Bool?)->Bool?
```

Функция `Likely` возвращает свой аргумент. Функция является подсказкой оптимизатору и говорит о том, что в большинстве случаев ее аргумент будет иметь значение `True`.
Например, наличие такой функции в `WHERE` означает что фильтр является слабоселективным.

#### Пример

```yql
SELECT * FROM T1 AS a JOIN T2 AS b USING(key)
WHERE Likely(a.amount > 0)  -- почти всегда верно
```

При наличии `Likely` оптимизатор не будет стараться выполнить фильтрацию перед `JOIN`.

## EvaluateExpr, EvaluateAtom {#evaluate_expr_atom}

Возможность выполнить выражение до начала основного расчета и подставить его результат в запрос как литерал (константу). Во многих контекстах, где в стандартном SQL ожидалась бы только константа (например, в именах таблиц, количестве строк в [LIMIT](../syntax/select/limit_offset.md) и т.п.) этот функционал активируется неявным образом автоматически.

EvaluateExpr может использоваться в тех местах, где грамматикой уже ожидается выражение. Например, с его помощью можно:

* округлить текущее время до дней, недель или месяцев и подставить в запрос, что затем позволит корректно работать кешированию запросов, хотя обычно использование [функций для получения текущего времени](#current-utc) его полностью отключает;
* сделать тяжелое вычисление с небольшим результатом один раз на запрос вместо одного раза на каждую джобу.

EvaluateAtom позволяет динамически создать [атом](../types/special.md), но т.к. ими в основном оперирует более низкий уровень [s-expressions](/docs/s_expressions/functions), то использовать эту функцию напрямую как правило не рекомендуется.

Единственный аргумент у обоих функций — само выражение для вычисления и подстановки.

Ограничения:

* выражение не должно приводить к запуску MapReduce операций;
* данный функционал полностью заблокирован в YQL over YDB.

#### Примеры

```yql
$now = CurrentUtcDate();
SELECT EvaluateExpr(
    DateTime::MakeDate(DateTime::StartOfWeek($now)
    )
);
```

## Литералы простых типов {#data-type-literals}

Для простых типов могут быть созданы литералы на основании строковых литералов.

#### Синтаксис

`<Простой тип>( <строка>[, <дополнительные атрибуты>] )`

В отличие от `CAST("myString" AS MyType)`:

* Проверка на приводимость литерала к требуемому типу происходит на этапе валидации;
* Результат не является optional.

Для типов данных `Date`, `Datetime`, `Timestamp` и `Interval` поддерживаются литералы только в формате, соответствующем [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601). У `Interval` есть следующие отличия от стандарта:

* поддерживается отрицательный знак для сдвигов в прошлое;
* микросекунды могут быть записаны как дробная часть секунд;
* единицы измерения больше недель не доступны;
* не поддерживаются варианты с началом/концом интервала, а также повторами.

Для типов данных `TzDate`, `TzDatetime`, `TzTimestamp` литералы также задаются в формате, соответствующем [ISO 8601](https://ru.wikipedia.org/wiki/ISO_8601), но вместо опционального суффикса Z через запятую указывается [IANA имя временной зоны](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones), например, GMT или Europe/Moscow.

{% include [decimal args](../_includes/decimal_args.md) %}

#### Примеры

```yql
SELECT
  Bool("true"),
  Uint8("0"),
  Int32("-1"),
  Uint32("2"),
  Int64("-3"),
  Uint64("4"),
  Float("-5"),
  Double("6"),
  Decimal("1.23", 5, 2), -- до 5 десятичных знаков, из которых 2 после запятой
  String("foo"),
  Utf8("привет"),
  Yson("<a=1>[3;%false]"),
  Json(@@{"a":1,"b":null}@@),
  Date("2017-11-27"),
  Datetime("2017-11-27T13:24:00Z"),
  Timestamp("2017-11-27T13:24:00.123456Z"),
  Interval("P1DT2H3M4.567890S"),
  TzDate("2017-11-27,Europe/Moscow"),
  TzDatetime("2017-11-27T13:24:00,America/Los_Angeles"),
  TzTimestamp("2017-11-27T13:24:00.123456,GMT"),
  Uuid("f9d5cc3f-f1dc-4d9c-b97e-766e57ca4ccb");
```

## ToBytes и FromBytes {#to-from-bytes}

Конвертация [простых типов данных](../types/primitive.md) в строку со своим бинарным представлением и обратно. Числа представляются в [little endian](https://en.wikipedia.org/wiki/Endianness#Little-endian).

#### Сигнатуры

```yql
ToBytes(T)->String
ToBytes(T?)->String?

FromBytes(String, Type<T>)->T?
FromBytes(String?, Type<T>)->T?
```

#### Примеры

```yql
SELECT
    ToBytes(123), -- "\u0001\u0000\u0000\u0000"
    FromBytes(
        "\xd2\x02\x96\x49\x00\x00\x00\x00",
        Uint64
    ); -- 1234567890ul
```


## ByteAt {#byteat}

Получение значение байта в строке по индексу от её начала. В случае некорректного индекса возвращается `NULL`.

#### Сигнатура

```yql
ByteAt(String, Uint32)->Uint8
ByteAt(String?, Uint32)->Uint8?

ByteAt(Utf8, Uint32)->Uint8
ByteAt(Utf8?, Uint32)->Uint8?
```

Аргументы:

1. Строка: `String` или `Utf8`;
2. Индекс: `Uint32`.

#### Примеры

```yql
SELECT
    ByteAt("foo", 0), -- 102
    ByteAt("foo", 1), -- 111
    ByteAt("foo", 9); -- NULL
```


## ...Bit {#bitops}

`TestBit()`, `ClearBit()`, `SetBit()` и `FlipBit()` - проверить, сбросить, установить или инвертировать бит в беззнаковом числе по указанному порядковому номеру бита.

#### Сигнатуры

```yql
TestBit(T, Uint8)->Bool
TestBit(T?, Uint8)->Bool?
TestBit(String, Uint8)->Bool?
TestBit(String?, Uint8)->Bool?

ClearBit(T, Uint8)->T
ClearBit(T?, Uint8)->T?

SetBit(T, Uint8)->T
SetBit(T?, Uint8)->T?

FlipBit(T, Uint8)->T
FlipBit(T?, Uint8)->T?
```

Аргументы:

1. Беззнаковое число, над которым выполнять требуемую операцию. `TestBit` также реализован и для строк (см. ниже описание).
2. Номер бита.

`TestBit` возвращает `true/false`. Остальные функции возвращают копию своего первого аргумента с проведенным соответствующим преобразованием.

`TestBit` для строковых аргументов работает следующим образом:

1. По второму аргументу (номеру бита) выбирается соответсвующий байт *с начала строки*.
2. Затем в выбранном байте выбирается соответствующий младший бит.

#### Примеры

```yql
SELECT
    TestBit(1u, 0), -- true
    TestBit('ax', 12) -- true (второй байт, четвертый бит)
    SetBit(8u, 0); -- 9
```


## Abs {#abs}

Абсолютное значение числа.

#### Сигнатура

```yql
Abs(T)->T
Abs(T?)->T?
```

#### Примеры

```yql
SELECT Abs(-123); -- 123
```


## Just {#optional-ops}

`Just()` - Изменить тип данных значения на [optional](../types/optional.md) от текущего типа данных (то есть `T` превращается в `T?`).

#### Сигнатура

```yql
Just(T)->T?
```

#### Примеры

```yql
SELECT
  Just("my_string"); --  String?
```

## Unwrap {#unwrap}

`Unwrap()` - Преобразование значения [optional](../types/optional.md) типа данных в соответствующий не-optional тип с ошибкой времени выполнений, если в данных оказался `NULL`. Таким образом, `T?` превращается в `T`.

Если значение не является [optional](../types/optional.md), то функция возвращает свой первый аргумент без изменений.

#### Сигнатура

```yql
Unwrap(T?)->T
Unwrap(T?, Utf8)->T
Unwrap(T?, String)->T
```

Аргументы:

1. Значение для преобразования;
2. Опциональная строка с комментарием для текста ошибки.

Обратная операция — [Just](#optional-ops).

#### Примеры

```yql
$value = Just("value");

SELECT Unwrap($value, "Unexpected NULL for $value");
```

## Nothing {#nothing}

`Nothing()` - Создать пустое значение указанного [Optional](../types/optional.md) типа данных.

#### Сигнатура

```yql
Nothing(Type<T?>)->T?
```

#### Примеры

```yql
SELECT
  Nothing(String?); -- пустое значение (NULL) с типом String?
```

[Подробнее о ParseType и других функциях для работы с типами данных](types.md).


## Callable {#callable}

Создать вызываемое значение с заданной сигнатурой из лямбда-функции. Обычно используется для того, чтобы размещать вызываемые значения в контейнерах.

#### Сигнатура

```yql
Callable(Type<Callable<(...)->T>>, lambda)->Callable<(...)->T>
```

Аргументы:

1. Тип;
2. Лямбда-функция.

#### Примеры

```yql
$lambda = ($x) -> {
    RETURN CAST($x as String)
};

$callables = AsTuple(
    Callable(Callable<(Int32)->String>, $lambda),
    Callable(Callable<(Bool)->String>, $lambda),
);

SELECT $callables.0(10), $callables.1(true);
```


## Pickle, Unpickle {#pickle}

`Pickle()` и `StablePickle()` сериализуют произвольный объект в последовательность байт, если это возможно. Типовыми несериализуемыми объектами являются Callable и Resource. Формат сериализации не версионируется, допускается использовать в пределах одного запроса. Для типа Dict функция StablePickle предварительно сортирует ключи, а для Pickle порядок элементов словаря в сериализованном представлении не определен.

`Unpickle()` — обратная операция (десериализация), где первым аргументом передается тип данных результата, а вторым — строка с результатом `Pickle()` или `StablePickle()`.

#### Сигнатуры

```yql
Pickle(T)->String
StablePickle(T)->String
Unpickle(Type<T>, String)->T
```

#### Примеры

```yql
SELECT *
FROM my_table
WHERE Digest::MurMurHash32(
        Pickle(TableRow())
    ) % 10 == 0; -- в реальности лучше использовать TABLESAMPLE

$buf = Pickle(123);
SELECT Unpickle(Int32, $buf);
```


## StaticMap

Преобразует структуру или кортеж, применяя лямбду к каждому элементу.

#### Сигнатура

```yql
StaticMap(Struct<...>, lambda)->Struct<...>
StaticMap(Tuple<...>, lambda)->Tuple<...>
```

Аргументы:

* Структура или кортеж;
* Лямбда для обработки элементов.

Результат: структура или кортеж с аналогичным первому аргументу количеством и именованием элементов, а типы данных элементов определяются результатами лямбды.

#### Примеры

```yql
SELECT *
FROM (
    SELECT
        StaticMap(TableRow(), ($item) -> {
            return CAST($item AS String);
        })
    FROM my_table
) FLATTEN COLUMNS; -- преобразование всех колонок в строки
```



## StaticZip

Поэлементно "склеивает" структуры или кортежи. Все аргументы (один и более) должны быть либо структурами с одинаковым набором полей, либо кортежами одинаковой длины.
Результататом будет соответственно структура или кортеж.
Каждый элемент результата – кортеж с соответствующими элементами из аргументов.

#### Сигнатура

```yql
StaticZip(Struct, Struct)->Struct
StaticZip(Tuple, Tuple)->Tuple
```

#### Примеры

```yql
$one = <|k1:1, k2:2.0|>;
$two = <|k1:3.0, k2:4|>;

-- поэлементное сложение двух структур
SELECT StaticMap(StaticZip($one, $two), ($tuple)->($tuple.0 + $tuple.1)) AS sum;
```


## StaticFold, StaticFold1 {#staticfold}

```yql
StaticFold(obj:Struct/Tuple, initVal, updateLambda)
StaticFold1(obj:Struct/Tuple, initLambda, updateLambda)
```

Статическая левоассоциативная свертка структуры или кортежа.
Для кортежей свертка производится в порядке от меньшего индекса к большему, для структур порядок не гарантируется.

- `obj` - объект, элементы которого нужно свернуть
- `initVal` - *(для StaticFold)* исходное состояние свертки
- `initLambda` - *(для StaticFold1)* функция для получения исходного состояния по первому элементу
- `updateLambda` - функция обновления состояния (принимает в аргументах следующий элемент объекта и предыдущее состояние)

`StaticFold(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $init, $f)` преобразуется в свертку:

```yql
$f($el_n, ...$f($el_2, $f($init, el_1))...)
```

`StaticFold1(<|key_1:$el_1, key_2:$el_2, ..., key_n:$el_n|>, $f0, $f)`:

```yql
$f($el_n, ...$f($el_2, $f($f0($init), el_1))...)
```

`StaticFold1(<||>, $f0, $f)` вернет `NULL`.

Аналогично работает и с кортежами.


## AggregationFactory {#aggregationfactory}

Создать фабрику для [агрегационных функций](aggregation.md) для того чтобы разделить процесс описания того, как агрегировать данные, и то, к каким данным это применять.

Аргументы:

1. Строка в кавычках, являющаяся именем агрегационной функции, например ["MIN"](aggregation.md#min).
2. Опциональные параметры агрегационной функции, которые не зависят от данных. Например, значение percentile в [PERCENTILE](aggregation.md#percentile).

Полученную фабрику можно использовать как второй параметр функции [AGGREGATE_BY](aggregation.md#aggregateby).
Если агрегационная функция работает на двух колонках вместо одной, как например, [MIN_BY](aggregation.md#minby), то в [AGGREGATE_BY](aggregation.md#aggregateby) первым аргументом передается `Tuple` из двух значений. Подробнее это указано при описании такой агрегационной функции.

#### Примеры

```yql
$factory = AggregationFactory("MIN");
SELECT
    AGGREGATE_BY(value, $factory) AS min_value -- применить MIN агрегацию к колонке value
FROM my_table;
```

## AggregateTransformInput {#aggregatetransform}

`AggregateTransformInput()` преобразует фабрику для [агрегационных функций](aggregation.md), например, полученную через функцию [AggregationFactory](#aggregationfactory) в другую фабрику, в которой перед началом выполнения агрегации производится указанное преобразование входных элементов.

Аргументы:

1. Фабрика для агрегационных функций;
2. Лямбда функция с одним аргументом, преобразующая входной элемент.

#### Примеры

```yql
$f = AggregationFactory("sum");
$g = AggregateTransformInput($f, ($x) -> (cast($x as Int32)));
$h = AggregateTransformInput($f, ($x) -> ($x * 2));
SELECT ListAggregate([1,2,3], $f); -- 6
SELECT ListAggregate(["1","2","3"], $g); -- 6
SELECT ListAggregate([1,2,3], $h); -- 12
```

## AggregateTransformOutput {#aggregatetransformoutput}

`AggregateTransformOutput()` преобразует фабрику для [агрегационных функций](aggregation.md), например, полученную через функцию [AggregationFactory](#aggregationfactory) в другую фабрику, в которой после окончания выполнения агрегации производится указанное преобразование результата.

Аргументы:

1. Фабрика для агрегационных функций;
2. Лямбда функция с одним аргументом, преобразующая результат.

#### Примеры

```yql
$f = AggregationFactory("sum");
$g = AggregateTransformOutput($f, ($x) -> ($x * 2));
SELECT ListAggregate([1,2,3], $f); -- 6
SELECT ListAggregate([1,2,3], $g); -- 12
```

## AggregateFlatten {#aggregateflatten}

Адаптирует фабрику для [агрегационных функций](aggregation.md), например, полученную через функцию [AggregationFactory](#aggregationfactory) так, чтобы выполнять агрегацию над входными элементами - списками. Эта операция похожа на [FLATTEN LIST BY](../syntax/flatten.md) - производится агрегация каждого элемента списка.

Аргументы:

1. Фабрика для агрегационных функций.

#### Примеры

```yql
$i = AggregationFactory("AGGREGATE_LIST_DISTINCT");
$j = AggregateFlatten($i);
SELECT AggregateBy(x, $j) from (
   SELECT [1,2] as x
   union all
   SELECT [2,3] as x
); -- [1, 2, 3]

```

## YQL::, s-expressions {#s-expressions}

Полный список внутренних функций YQL находится в [документации к s-expressions](/docs/s_expressions/functions), альтернативному низкоуровневому синтаксису YQL. Любую из перечисленных там функций можно вызвать и из SQL синтаксиса, добавив к её имени префикс `YQL::`, но это не рекомендуется делать, т.к. данный механизм предназначен в первую очередь для временного обхода возможных проблем, а также для нужд внутреннего тестирования.

Если функция доступна в SQL синтаксисе без префикса `YQL::`, то её поведение имеет право отличаться от одноименной функции из документации по s-expressions, если таковая существует.

