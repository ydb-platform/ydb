# OVER, PARTITION BY и WINDOW

Механизм оконных функций, появившийся в стандарте SQL:2003 и расширенный в стандарте SQL:2011, позволяет выполнять вычисления над набором строк таблицы, который некоторым образом соотносится с текущей строкой.

В отличие от [агрегатных функций](../builtins/aggregation.md) при этом не происходит группировка нескольких строк в одну – после применения оконных функций число строк в результирующей таблице всегда совпадает с числом строк в исходной.

При наличии в запросе агрегатных и оконных функций сначала производится группировка и вычисляются значения агрегатных функций. Вычисленные значения агрегатных функций могут использоваться в качестве аргументов оконных (но не наоборот). Порядок, в котором вычисляются оконные функции относительно других элементов запроса, описан в разделе [SELECT](select/index.md).

## Синтаксис {#syntax}

Общий синтаксис вызова оконной функции имеет вид

```yql
function_name([expression [, expression ...]]) OVER (window_definition)
```

или

```yql
function_name([expression [, expression ...]]) OVER window_name
```

Здесь `window_name` (*имя окна*) – произвольный идентификатор, уникальный в рамках запроса, `expression` – произвольное выражение не содержащее вызова оконных функций.

В запросе каждому имени окна должно быть сопоставлено *определение окна* (`window_definition`):

```yql
SELECT
    F0(...) OVER (window_definition_0),
    F1(...) OVER w1,
    F2(...) OVER w2,
    ...
FROM my_table
WINDOW
    w1 AS (window_definition_1),
    ...
    w2 AS (window_definition_2)
;
```

Здесь `window_definition` записывается в виде

```antlr
[ PARTITION BY (expression AS column_identifier | column_identifier) [, ...] ]
[ ORDER BY expression [ASC | DESC] ]
[ frame_definition ]
```

Необязательное *определение рамки* (`frame_definition`) может быть задано одним из следующих способов:

* ```ROWS frame_begin```
* ```ROWS BETWEEN frame_begin AND frame_end```
* ```RANGE frame_begin```
* ```RANGE BETWEEN frame_begin AND frame_end```

{% note info %}

Режим `RANGE` доступен начиная с версии языка 2026.01.

{% endnote %}

*Начало рамки* (`frame_begin`) и *конец рамки* (`frame_end`) задаются одним из следующих способов:

* ```UNBOUNDED PRECEDING```
* ```offset PRECEDING```
* ```CURRENT ROW```
* ```offset FOLLOWING```
* ```UNBOUNDED FOLLOWING```

Здесь *смещение рамки* (`offset`) – неотрицательный литерал. Если конец рамки не задан, то подразумевается `CURRENT ROW`.

В режиме `ROWS` смещение всегда является целочисленным литералом.
В режиме `RANGE` тип смещения определяется типом столбца из `ORDER BY` и должен поддерживать с ним операции сложения, вычитания и сравнения. Поддерживаются следующие типы столбца `ORDER BY`:

* все числовые типы YQL: `Int8`, `Uint8`, `Int16`, `Uint16`, `Int32`, `Uint32`, `Int64`, `Uint64`, `Float`, `Double`, `Decimal` – смещение должно иметь один из этих же типов (необязательно тот же). Нельзя использовать значения `NaN` и `Inf` для вещественных типов;
* типы даты и времени YQL: `Date`, `Datetime`, `Timestamp`, `TzDate`, `TzDatetime`, `TzTimestamp`, `Interval` - смещение типа `Interval` или `Interval64`;
* широкие типы даты и времени YQL: `Date32`, `Datetime64`, `Timestamp64`, `TzDate32`, `TzDatetime64`, `TzTimestamp64`, `Interval64` – смещение типа `Interval` или `Interval64`;
* типы PostgreSQL поддержаны аналогичным образом.


Все выражения внутри определения окна не должны содержать вызовов оконных функций.

## Алгоритм вычисления

### Разбиение {#partition}

Указание `PARTITION BY` группирует строки исходной таблицы в *разделы*, которые затем обрабатываются независимо друг от друга.
Если `PARTITION BY` не указан, то все строки исходной таблицы попадают в один раздел. Указание `ORDER BY` определяет порядок строк в разделе.
В `PARTITION BY`, как и в [GROUP BY](group_by.md) можно использовать алиасы и [SessionWindow](group_by.md#session-window).

При отсутствии `ORDER BY` порядок строк в разделе не определен.

### Рамка {#frame}

Определение рамки `frame_definition` задает множество строк раздела, попадающих в *рамку окна*, связанную с текущей строкой.

В режиме `ROWS` в рамку окна попадают строки с указанными смещениями относительно текущей строки раздела. Например, для `ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING` в рамку окна попадут три строки перед текущей, текущая строка и пять строк после текущей строки.

В режиме `RANGE` смещение задаётся не количеством строк, а диапазоном значений столбца из `ORDER BY`. Для использования режима `RANGE` со смещением (`offset PRECEDING` или `offset FOLLOWING`) необходимо, чтобы `ORDER BY` содержал ровно один столбец поддерживаемого типа. Например, для `RANGE BETWEEN 3 PRECEDING AND 5 FOLLOWING` в рамку окна попадут все строки раздела, у которых значение столбца из `ORDER BY` отличается от значения текущей строки не более чем на 3 в меньшую сторону и не более чем на 5 в большую сторону. Вариант `RANGE CURRENT ROW` включает все строки, у которых значение столбца из `ORDER BY` совпадает со значением текущей строки. Варианты `UNBOUNDED PRECEDING` и `UNBOUNDED FOLLOWING` в режиме `RANGE` имеют тот же смысл, что и в режиме `ROWS`.

Множество строк в рамке окна может меняться в зависимости от того, какая строка является текущей. Например, для первой строки раздела в рамку окна `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING` не попадет ни одной строки.

Указание `UNBOUNDED PRECEDING` в качестве начала рамки означает "от первой строки раздела", `UNBOUNDED FOLLOWING` в качестве конца рамки – "до последней строки раздела", `CURRENT ROW` – "от/до текущей строки".

Если `определение_рамки` не указано, то множество строк, попадающих в рамку окна, определяется наличием `ORDER BY` в `определении_окна`.
А именно, при наличии `ORDER BY` неявно подразумевается `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, а при отсутствии – `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

Далее, в зависимости от конкретной оконной функции производится ее вычисление либо на множестве строк раздела, либо на множестве строк рамки окна.

[Список доступных оконных функций](../builtins/window.md)

#### Примеры

```yql
SELECT
    COUNT(*) OVER w AS rows_count_in_window,
    some_other_value -- доступ к текущей строке
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
);
```

```yql
SELECT
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
);
```

```yql
SELECT
    -- AVG (как и все агрегатные функции, используемые в качестве оконных)
    -- вычисляется на рамке окна
    AVG(some_value) OVER w AS avg_of_prev_current_next,
    some_other_value -- доступ к текущей строке
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
);
```

```yql
SELECT
    -- LAG не зависит от положения рамки окна
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY my_column
);
```

```yql
-- RANGE CURRENT ROW: агрегат вычисляется по всем строкам с тем же значением
-- order_column, что и у текущей строки (включая саму текущую строку)
SELECT
    order_column,
    SUM(amount) OVER w AS sum_for_same_order_value
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY order_column
    RANGE CURRENT ROW
);
```

```yql
-- RANGE с числовым смещением: скользящее среднее по значениям,
-- отличающимся от текущего не более чем на 10 единиц в каждую сторону
SELECT
    ts,
    AVG(value) OVER w AS moving_avg
FROM my_table
WINDOW w AS (
    ORDER BY ts
    RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING
);
```

```yql
-- RANGE UNBOUNDED: накопленная сумма, включающая все строки
-- с тем же значением order_column, что и у текущей (а не только до неё)
SELECT
    order_column,
    SUM(amount) OVER w AS cumulative_sum
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY order_column
    RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
);
```

## Особенности реализации

* Функции, вычисляемые на рамке окна `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` либо `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, реализованы эффективно (не требуют дополнительной памяти и вычисляются на разделе за O(размер раздела)). Рамка `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` требует дополнительной памяти для буферизации строк, у которых значение столбца `ORDER BY` совпадает с текущей строкой.

* Для рамки окна `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` есть возможность выбрать стратегию выполнения в оперативной памяти, указав [хинт](lexer.md#sql-hints) `COMPACT` после ключевого слова `PARTITION`.

  Например: `PARTITION /*+ COMPACT() */ BY key` или `PARTITION /*+ COMPACT() */ BY ()` (в случае если `PARTITION BY` изначально отсутствовал).

  При наличии хинта `COMPACT` потребуется дополнительная память в размере O(размер раздела), но при этом не возникнет дополнительной `JOIN` операции.

* Если рамка окна не начинается с `UNBOUNDED PRECEDING`, то для вычисления оконных функций на таком окне потребуется дополнительная память в размере O(максимальное расстояние от границ окна до текущей строки), а время вычисления будет равно O(число_строк_в_разделе * размер_окна).

* Для рамки окна, начинающейся с `UNBOUNDED PRECEDING` и заканчивающейся на `N`, где `N` не равен `CURRENT ROW` или `UNBOUNDED FOLLOWING`, потребуется дополнительная память в размере O(N), а время вычисления будет O(размер раздела).

* Функции `LEAD(expr, N)` и `LAG(expr, N)` всегда потребуют O(N) памяти.

Учитывая вышесказанное, запрос с `ROWS/RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` по возможности стоит переделать в `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, поменяв порядок сортировки в `ORDER BY` на обратный.
