# OVER, PARTITION BY и WINDOW

Механизм оконных функций, появившийся в стандарте SQL:2003 и расширенный в стандарте SQL:2011, позволяет выполнять вычисления над набором строк таблицы, который некоторым образом соотносится с текущей строкой.

В отличие от [агрегатных функций](../../builtins/aggregation.md) при этом не происходит группировка нескольких строк в одну – после применения оконных функций число строк в результирующей таблице всегда совпадает с числом строк в исходной.

При наличии в запросе агрегатных и оконных функций сначала производится группировка и вычисляются значения агрегатных функций. Вычисленные значения агрегатных функций могут использоваться в качестве аргументов оконных (но не наоборот). Порядок, в котором вычисляются оконные функции относительно других элементов запроса, описан в разеделе [SELECT](../select/index.md).

## Синтаксис {#syntax}

Общий синтаксис вызова оконной функции имеет вид
```
function_name([expression [, expression ...]]) OVER (window_definition)
или
function_name([expression [, expression ...]]) OVER window_name
```

Здесь `window_name` (_имя окна_) – произвольный идентификатор, уникальный в рамках запроса, `expression` – произвольное выражение не содержащее вызова оконных функций.

В запросе каждому имени окна должно быть сопоставлено _определение окна_ (`window_definition`):

```
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
```
[ PARTITION BY (expression AS column_identifier | column_identifier) [, ...] ]
[ ORDER BY expression [ASC | DESC] ]
[ frame_definition ]
```

Необязательное *определение рамки* (`frame_definition`) может быть задано одним из двух способов:

* ```ROWS frame_begin```
* ```ROWS BETWEEN frame_begin AND frame_end```

*Начало рамки* (`frame_begin`) и *конец рамки* (`frame_end`) задаются одним из следующих способов:

* ```UNBOUNDED PRECEDING```
* ```offset PRECEDING```
* ```CURRENT ROW```
* ```offset FOLLOWING```
* ```UNBOUNDED FOLLOWING```

Здесь *смещение рамки* (`offset`) – неотрицательный числовой литерал. Если конец рамки не задан, то подразумевается `CURRENT ROW`.

Все выражения внутри определения окна не должны содержать вызовов оконных функций.

## Алгоритм вычисления

### Разбиение {#partition}
Указание `PARTITION BY` группирует строки исходной таблицы в _разделы_, которые затем обрабатываются независимо друг от друга.
Если `PARTITION BY` не указан, то все строки исходной таблицы попадают в один раздел. Указание `ORDER BY` определяет порядок строк в разделе.
В `PARTITION BY`, как и в [GROUP BY](../group_by.md) можно использовать алиасы и [SessionWindow](../group_by.md#session-window).

При отсутствии `ORDER BY` порядок строк в разделе не определен.

### Рамка {#frame}
Определение рамки `frame_definition` задает множество строк раздела, попадающих в *рамку окна* связанную с текущей строкой.

В режиме `ROWS` (в YQL пока поддерживается только он) в рамку окна попадают строки с указанными смещениями относительно текущей строки раздела. Например, для `ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING` в рамку окна попадут три строки перед текущей, текущая строка и пять строк после текущей строки.

Множество строк в рамке окна может меняться в зависимости от того, какая строка является текущей. Например, для первой строки раздела в рамку окна `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING` не попадет ни одной строки.

Указание `UNBOUNDED PRECEDING` в качестве начала рамки означает "от первой строки раздела", `UNBOUNDED FOLLOWING` в качестве конца рамки – "до последней строки раздела", `CURRENT ROW` – "от/до текущей строки".

Если `определение_рамки` не указано, то в множество строк попадающих в рамку окна определяется наличием `ORDER BY` в `определении_окна`.
А именно, при наличии `ORDER BY` неявно подразумевается `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, а при отсутствии – `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

Далее, в зависимости от конкретной оконной функции производится ее вычисление либо на множестве строк раздела, либо на множестве строк рамки окна.

[Список доступных оконных функций](../../builtins/window.md)

**Примеры:**

```sql
SELECT
    COUNT(*) OVER w AS rows_count_in_window,
    some_other_value -- доступ к текущей строке
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
);
```

```sql
SELECT
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
);
```

``` yql
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

``` yql
SELECT
    -- LAG не зависит от положения рамки окна
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY my_column
);
```

## Особенности реализации

* Функции, вычисляемые на рамке окна `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` либо `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, реализованы эффективно (не требуют дополнительной памяти и вычисляются на разделе за O(размер раздела)).

* Для рамки окна `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` есть возможность выбрать стратегию выполнения в оперативной памяти, указав [хинт](../lexer.md#sql-hints) `COMPACT` после ключевого слова `PARTITION`.

  Например: `PARTITION /*+ COMPACT() */ BY key` или `PARTITION /*+ COMPACT() */ BY ()` (в случае если `PARTITION BY` изначально отсутствовал).
  
  При наличии хинта `COMPACT` потребуется дополнительная память в размере O(размер раздела), но при этом не возникнет дополнительной `JOIN` операции.

* Если рамка окна не начинается с `UNBOUNDED PRECEDING`, то для вычисления оконных функций на таком окне потребуется дополнительная память в размере O(максимальное расстояние от границ окна до текущей строки), а время вычисления будет равно O(число_строк_в_разделе * размер_окна).

* Для рамки окна, начинающейся с `UNBOUNDED PRECEDING` и заканчивающейся на `N`, где `N` не равен `CURRENT ROW` или `UNBOUNDED FOLLOWING`, потребуется дополнительная память в размере O(N), а время вычисления будет O(N * число_строк_в_разделе).

* Функции `LEAD(expr, N)` и `LAG(expr, N)` всегда потребуют O(N) памяти.

Учитывая вышесказанное, запрос с `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` по возможности стоит переделать в `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, поменяв порядок сортировки в `ORDER BY` на обратный.
