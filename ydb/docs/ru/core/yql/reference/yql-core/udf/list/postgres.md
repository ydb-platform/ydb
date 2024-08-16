# PostgreSQL UDF

[YQL](../../index.md) предоставляет возможность доступа к [функциям](https://www.postgresql.org/docs/16/functions.html) и [типам данных](https://www.postgresql.org/docs/16/datatype.html) PostgreSQL.


Система типов в PostgreSQL существенно проще, чем в YQL:
1. Все типы в PostgreSQL являются `nullable` (аналогом типа `int4` в postgres является yql тип `Int32?`).
2. Единственно возможный сложный (контейнерный) тип в PostgreSQL – это массив (`array`) с некоторой размерностью. Вложенные массивы невозможны на уровне типов.

На данный момент поддерживаются все простые типы данных из PostgreSQL, включая массивы.

Имена PostgreSQL типов в YQL получаются добавлением префикса `Pg` к исходному имени типа.
Например `PgVarchar`, `PgInt4`, `PgText`. Имена pg типов (как и вообще всех типов) в YQL являются case-insensitive.

Если исходный тип является типом массива (в PostgreSQL такие типы начинаются с подчеркивания: `_int4` - массив 32-битных целых), то имя типа в YQL тоже начинается с подчеркивания – `_PgInt4`.

## Литералы {#literals}

Строковые и числовые литералы Pg типов можно создавать с помощью специальных суффиксов (аналогично простым [строковым](../yql/reference/yql-core/syntax/lexer#string-literals) и [числовым](../yql/reference/yql-core/syntax/lexer#literal-numbers) литералам).

{% include [pgliterals](./_includes/pgliterals.md) %}

## Операторы {#operators}

Операторы PostgreSQL (унарные и бинарные) доступны через встроенную функцию `PgOp(<оператор>, <операнды>)`:

``` sql
SELECT
    PgOp("*", 123456789987654321pn, 99999999999999999999pn), --  12345678998765432099876543210012345679
    PgOp('|/', 10000.0p), -- 100.0p (квадратный корень)
    PgOp("-", 1p), -- -1p
    -1p,           -- унарный минус для литералов работает и без PgOp
;
```

## Оператор приведения типа {#cast_operator}

Для приведения значения одного Pg типа к другому используется встроенная функция `PgCast(<исходное значение>, <желаемый тип>)`:

``` sql
SELECT
    PgCast(123p, PgText), -- преобразуем число в строку
;
```

При преобразовании из строковых Pg типов в некоторые целевые типы можно указать дополнительные модификаторы. Возможные модификаторы для типа `pginterval` перечислены в [документации](https://www.postgresql.org/docs/16/datatype-datetime.html).

``` sql
SELECT
    PgCast('90'p, pginterval, "day"), -- 90 days
    PgCast('13.45'p, pgnumeric, 10, 1); -- 13.5
;
```

## Преобразование значений Pg типов в значения YQL типов и обратно {#frompgtopg}

Для некоторых Pg типов возможна конвертация в YQL типы и обратно. Конвертация осуществляется с помощью встроенных функций
`FromPg(<значение Pg типа>)` и `ToPg(<значение YQL типа>)`:

``` sql
SELECT
    FromPg("тест"pt), -- Just(Utf8("тест")) - pg типы всегда nullable
    ToPg(123.45), -- 123.45pf8
;
```

### Список псевдонимов типов PostgreSQL при их использовании в YQL {#pgyqltypes}

Ниже приведены типы данных YQL, соответствующие им логические типы PostgreSQL и названия типов PostgreSQL при их использовании в YQL:

| YQL | PostgreSQL | Название PostgreSQL-типа в YQL|
|---|---|---|
| `Bool` | `bool` |`pgbool` |
| `Int8` | `int2` |`pgint2` |
| `Uint8` | `int2` |`pgint2` |
| `Int16` | `int2` |`pgint2` |
| `Uint16` | `int4` |`pgint4` |
| `Int32` | `int4` |`pgint4` |
| `Uint32` | `int8` |`pgint8` |
| `Int64` | `int8` |`pgint8` |
| `Uint64` | `numeric` |`pgnumeric` |
| `Float` | `float4` |`pgfloat4` |
| `Double` | `float8` |`pgfloat8` |
| `String` | `bytea` |`pgbytea` |
| `Utf8` | `text` |`pgtext` |
| `Yson` | `bytea` |`pgbytea` |
| `Json` | `json` |`pgjson` |
| `Uuid` | `uuid` |`pguuid` |
| `JsonDocument` | `jsonb` |`pgjsonb` |
| `Date` | `date` |`pgdate` |
| `Datetime` | `timestamp` |`pgtimestamp` |
| `Timestamp` | `timestamp` |`pgtimestamp` |
| `Interval` | `interval` | `pginterval` |
| `TzDate` | `text` |`pgtext` |
| `TzDatetime` | `text` |`pgtext` |
| `TzTimestamp` | `text` |`pgtext` |
| `Date32` | `date` | `pgdate`|
| `Datetime64` | `timestamp` |`pgtimestamp` |
| `Timestamp64` | `timestamp` |`pgtimestamp` |
| `Interval64`| `interval` |`pginterval` |
| `TzDate32` | `text` |  |`pgtext` |
| `TzDatetime64` | `text` |  |`pgtext` |
| `TzTimestamp64` | `text` |  |`pgtext` |
| `Decimal` | `numeric` |`pgnumeric` |
| `DyNumber` | `numeric` |`pgnumeric` |


### Таблица соответствия типов `ToPg` {#topg}

Таблица соответствия типов данных YQL и PostgreSQL при использовании функции `ToPg`:

{% include [topg](_includes/topg.md) %}

### Таблица соответствия типов `FromPg` {#frompg}

Таблица соответствия типов данных PostgreSQL и YQL при использовании функции `FromPg`:

{% include [frompg](_includes/frompg.md) %}

## Вызов PostgreSQL функций {#callpgfunction}

Чтобы вызвать PostgreSQL функцию, необходимо добавить префикс `Pg::` к ее имени:

``` sql
SELECT
    Pg::extract('isodow'p,PgCast('1961-04-12'p,PgDate)), -- 3pn (среда) - работа с датами до 1970 года
    Pg::generate_series(1p,5p), -- [1p,2p,3p,4p,5p] - для функций-генераторов возвращается ленивый список
;
```

Существует также альтернативный способ вызова функций через встроенную функцию `PgCall(<имя функции>, <операнды>)`:

``` sql
SELECT
    PgCall('lower', 'Test'p), -- 'test'p
;
```

При вызове функции, возвращающей набор `pgrecord`, можно распаковать результат в список структур, используя функцию `PgRangeCall(<имя функции>, <операнды>)`:

``` sql
SELECT * FROM
    AS_TABLE(PgRangeCall("json_each", pgjson('{"a":"foo", "b":"bar"}')));
    --- 'a'p,pgjson('"foo"')
    --- 'b'p,pgjson('"bar"')
;
```



## Вызов агрегационных PostgreSQL функций {#pgaggrfunction}

Чтобы вызвать агрегационную PostgreSQL функцию, необходимо добавить префикс `Pg::` к ее имени:

``` sql
SELECT
Pg::string_agg(x,','p)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a,b,c'p

SELECT
Pg::string_agg(x,','p) OVER (ORDER BY x)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'a,b'p,'a,b,c'p
;
```

Также можно использовать агрегационную PostgreSQL функцию для построения фабрики агрегационных функций с последующим применением в `AGGREGATE_BY`:

``` sql
$agg_max = AggregationFactory("Pg::max");

SELECT
AGGREGATE_BY(x,$agg_max)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'c'p

SELECT
AGGREGATE_BY(x,$agg_max) OVER (ORDER BY x),
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'b'p,'c'p
```

В этом случае вызов `AggregationFactory` принимает только имя функции с префиксом `Pg::`, а все аргументы функции передаются в `AGGREGATE_BY`.

Если в агрегационной функции не один аргумент, а ноль или два и более, необходимо использовать кортеж при вызове `AGGREGATE_BY`:

``` sql
$agg_string_agg = AggregationFactory("Pg::string_agg");

SELECT
AGGREGATE_BY((x,','p),$agg_string_agg)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a,b,c'p

SELECT
AGGREGATE_BY((x,','p),$agg_string_agg) OVER (ORDER BY x)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'a,b'p,'a,b,c'p
```

{% note warning "Внимание" %}

Не поддерживается режим `DISTINCT` над аргументами при вызове агрегационных PostgreSQL функций, а также использование `MULTI_AGGREGATE_BY`.

{% endnote %}

## Логические операции

Для выполнения логических операций используются функции `PgAnd`, `PgOr`, `PgNot`:

``` sql
SELECT
    PgAnd(PgBool(true), PgBool(true)), -- PgBool(true)
    PgOr(PgBool(false), null), -- PgCast(null, pgbool)
    PgNot(PgBool(true)), -- PgBool(false)
;
```

