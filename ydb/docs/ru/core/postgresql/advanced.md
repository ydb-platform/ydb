# Расширенные возможности

{% include [./_includes/alert.md](./_includes/alert_preview.md) %}

[YQL](../yql/reference/index.md) предоставляет возможность доступа из YQL к [функциям](https://www.postgresql.org/docs/16/functions.html) и [типам данных](https://www.postgresql.org/docs/16/datatype.html) PostgreSQL.


Система типов в PostgreSQL существенно проще, чем в YQL:
1. Все типы в PostgreSQL являются `nullable` (аналогом типа `int4` в postgres является yql тип `Int32?`).
2. Единственно возможный сложный (контейнерный) тип в PostgreSQL – это массив (`array`) с некоторой размерностью. Вложенные массивы невозможны на уровне типов.

На данный момент поддерживаются все простые типы данных из PostgreSQL, включая массивы.

Имена PostgreSQL типов в YQL получаются добавлением префикса `Pg` к исходному имени типа.
Например `PgVarchar`, `PgInt4`, `PgText`. Имена pg типов (как и вообще всех типов) в YQL являются case-insensitive.

Если исходный тип является типом массива (в PostgreSQL такие типы начинаются с подчеркивания: `_int4` - массив 32-битных целых), то имя типа в YQL тоже начинается с подчеркивания – `_PgInt4`.

## Литералы {#literals}

Строковые и числовые литералы Pg типов можно создавать с помощью специальных суффиксов (аналогично простым [строковым](../yql/reference/yql-core/syntax/lexer#string-literals) и [числовым](../yql/reference/yql-core/syntax/lexer#literal-numbers) литералам).

### Целочисленные литералы {#intliterals}

Суффикс | Тип | Комментарий
----- | ----- | -----
`p` | `PgInt4` | 32-битное знаковое целое (в PostgreSQL нет беззнаковых типов)
`ps`| `PgInt2` | 16-битное знаковое целое
`pi`| `PgInt4` |
`pb`| `PgInt8` | 64-битное знаковое цело
`pn`| `PgNumeric` | знаковое целое произвольной точности (до 131072 цифр)

### Литералы с плавающей точкой {#floatliterals}

Суффикс | Тип | Комментарий
----- | ----- | -----
`p` | `PgFloat8` | число с плавающей точкой (64 бит double)
`pf4`| `PgFloat4` | число с плавающей точкой (32 бит float)
`pf8`| `PgFloat8` |
`pn` | `PgNumeric` | число с плавающей точкой произвольной точности (до 131072 цифр перед запятой, до 16383 цифр после запятой)

### Строковые литералы {#stringliterals}

Суффикс | Тип | Комментарий
----- | ----- | -----
`p` | `PgText` | текстовая строка
`pt`| `PgText` |
`pv`| `PgVarchar` | текстовая строка
`pb`| `PgBytea` | бинарная строка

{% note warning "Внимание" %}

Значения строковых/числовых литералов (т.е. то что идет перед суффиксом) должны быть валидной строкой/числом с точки зрения YQL.
В частности, должны соблюдаться правила эскейпинга YQL, а не PostgreSQL.

{% endnote %}

Пример:

``` sql
SELECT
    1234p, 0x123pb, "тест"pt, 123e-1000pn;
;
```

### Литерал массива

Для построения литерала массива используется функция `PgArray`:

``` sql
SELECT
    PgArray(1p,null,2p), -- {1,NULL,2}, тип _int4
;
```


### Конструктор литералов произвольного типа {#literals_constructor}

Литералы всех типов (в том числе и Pg типов) могут создаваться с помощью конструктора литералов со следующей сигнатурой:
`Имя_типа(<строковая константа>)`.

Напрмер:
``` sql
DECLARE $foo AS String;
SELECT
    PgInt4("1234"), -- то же что и 1234p
    PgInt4(1234),   -- в качестве аргумента можно использовать литеральные константы
    PgInt4($foo),   -- и declare параметры
    PgBool(true),
    PgInt8(1234),
    PgDate("1932-01-07"),
;
```

Также поддерживается встроенная функция `PgConst` со следующей сигнатурой: `PgConst(<строковое значение>, <тип>)`.
Такой способ более удобен для кодогенерации.

Например:

``` sql
SELECT
    PgConst("1234", PgInt4), -- то же что и 1234p
    PgConst("true", PgBool)
;
```

Для некоторых типов в функции `PgConst` можно указать дополнительные модификаторы. Возможные модификаторы для типа `pginterval` перечислены в [документации](https://www.postgresql.org/docs/16/datatype-datetime.html).

``` sql
SELECT
    PgConst(90, pginterval, "day"), -- 90 days
    PgConst(13.45, pgnumeric, 10, 1); -- 13.5
;
```

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

