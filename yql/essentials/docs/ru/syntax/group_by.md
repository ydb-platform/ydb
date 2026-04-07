## GROUP BY

Группирует результаты `SELECT` по значениям указанных столбцов или выражений. Вместе с `GROUP BY` часто применяются [агрегатные функции](../builtins/aggregation.md) (`COUNT`, `MAX`, `MIN`, `SUM`, `AVG`) для выполнения вычислений в каждой группе.

#### Синтаксис

```yql
SELECT                             -- В SELECT можно использовать:
    column1,                       -- ключевые колонки, заданные в GROUP BY
    key_n,                         -- именованные выражения, заданные в GROUP BY
    column1 + key_n,               -- произвольные неагрегатные функции от них
    Aggr_Func1( column2 ),         -- агрегатные функции, содержащие в аргументах любые колонки,
    Aggr_Func2( key_n + column2 ), --   включая именованные выражения, заданные в GROUP BY
    ...
FROM table
GROUP BY
    column1, column2, ...,
    <expr> AS key_n           -- При группировке по выражению ему может быть задано имя через AS,
                              -- которое может быть использовано в SELECT
```

Запрос вида `SELECT * FROM table GROUP BY k1, k2, ...` вернет все колонки, перечисленные в GROUP BY, то есть эквивалентен запросу `SELECT DISTINCT k1, k2, ... FROM table`.

Звездочка может также применяться в качестве аргумента агрегатной функции `COUNT`. `COUNT(*)` означает "число строк в группе".


{% note info %}

Агрегатные функции не учитывают `NULL` в своих аргументах, за исключением функции `COUNT`.

{% endnote %}

Также в YQL доступен механизм фабрик агрегатных функций, реализованный с помощью функций [`AGGREGATION_FACTORY`](../builtins/basic.md#aggregationfactory) и [`AGGREGATE_BY`](../builtins/aggregation.md#aggregateby).

#### Примеры

```yql
SELECT key, COUNT(*) FROM my_table
GROUP BY key;
```

```yql
SELECT double_key, COUNT(*) FROM my_table
GROUP BY key + key AS double_key;
```

```yql
SELECT
   double_key,                           -- ОК: ключевая колонка
   COUNT(*) AS group_size,               -- OK: COUNT(*)
   SUM(key + subkey) AS sum1,            -- ОК: агрегатная функция
   CAST(SUM(1 + 2) AS String) AS sum2,   -- ОК: агрегатная функция с константным аргументом
   SUM(SUM(1) + key) AS sum3,            -- ОШИБКА: вложенные агрегации не допускаются
   key AS k1,                            -- ОШИБКА: использование неключевой колонки key без агрегации
   key * 2 AS dk1,                       -- ОШИБКА в YQL: использование неключевой колонки key без агрегации
FROM my_table
GROUP BY
  key * 2 AS double_key,
  subkey as sk,

```


{% note warning %}

Возможность указывать имя для колонки или выражения в `GROUP BY .. AS foo` является расширением YQL. 

До версии языка [2025.02](../changelog/2025.02.md) такое имя было видимым в `WHERE` несмотря на то, что фильтрация по `WHERE` выполняется [раньше](select/index.md#selectexec) группировки. В частности, если в таблице `T` есть две колонки `foo` и `bar`, то в запросе `SELECT foo FROM T WHERE foo > 0 GROUP BY bar AS foo` фильтрация фактически произойдет по колонке `bar` из исходной таблицы.

{% endnote %}


## GROUP BY ... SessionWindow() {#session-window}

В YQL поддерживаются группировки по сессиям. К обычным выражениям в `GROUP BY` можно добавить специальную функцию `SessionWindow`:

```yql
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- то же что и session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(<time_expr>, <timeout_expr>) AS session_start
```

При этом происходит следующее:

1. Входная таблица партиционируется по ключам группировки, указанным в `GROUP BY`, без учета SessionWindow (в данном случае по `user`). Если кроме SessionWindow в `GROUP BY` ничего нет, то входная таблица попадает в одну партицию
2. Каждая партиция делится на непересекающие подмножества строк (сессии). Для этого партиция сортируется по возрастанию значения выражения `time_expr`. Границы сессий проводятся между соседними элементами партиции, разница значений `time_expr` для которых превышает `timeout_expr`
3. Полученные таким образом сессии и являются финальными партициями, на которых вычисляются агрегатные функции.

Ключевая колонка SessionWindow() (в примере `session_start`) имеет значение "минимальный `time_expr` в сессии".
Кроме того, при наличии SessionWindow() в `GROUP BY` может использоваться специальная агрегатная функция
[SessionStart](../builtins/aggregation.md#session-start).

Поддерживается также расширенный вариант SessionWindow с четырьмя аргументами:

`SessionWindow(<order_expr>, <init_lambda>, <update_lambda>, <calculate_lambda>)`

Здесь:

* `<order_expr>` – выражение по которому сортируется исходная партиция
* `<init_lambda>` – лямбда-функция для инициализации состояния расчета сессий. Имеет сигнатуру `(TableRow())->State`. Вызывается один раз на первом (по порядку сортировки) элементе исходной партиции
* `<update_lambda>` – лямбда-функция для обновления состояния расчета сессий и определения границ сессий. Имеет сигнатуру `(TableRow(), State)->Tuple<Bool, State>`. Вызывается на каждом элементе исходной партиции, кроме первого. Новое значение состояния вычисляется на основе текущей строки таблицы и предыдущего состояния. Если первый элемент возвращенного кортежа имеет значение `True`, то с _текущей_ строки начнется новая сессия. Ключ новой сессии получается путем применения `<calculate_lambda>` ко второму элементу кортежа.
* `<calculate_lambda>` – лямбда-функция для вычисления ключа сессии ("значения" SessionWindow(), которое также доступно через SessionStart()). Функция имеет сигнатуру `(TableRow(), State)->SessionKey`. Вызывается на первом элементе партиции (после `<init_lambda>`) и на тех элементах, для которых `<update_lambda>` вернула `True` в качестве первого элемента кортежа. Стоит отметить, что для начала новой сессии необходимо, чтобы `<calculate_lambda>` вернула значение, которое отличается от предыдущего ключа сессии. При этом сессии с одинаковыми ключами не объединяются. Например, если `<calculate_lambda>` последовательно возвращает `0, 1, 0, 1`, то это будут четыре различные сессии.

С помощью расширенного варианта SessionWindow можно решить, например, такую задачу: разделить партицию на сессии как в варианте SessionWindow с двумя аргументами, но с ограничением максимальной длины сессии некоторой константой:

#### Пример

```yql
$max_len = 1000; -- максимальная длина сессии
$timeout = 100; -- таймаут (timeout_expr в упрощенном варианте SessionWindow)

$init = ($row) -> (AsTuple($row.ts, $row.ts)); -- состояние сессии - тапл из 1) значения временной колонки ts на первой строчке сессии и 2) на текущей строчке
$update = ($row, $state) -> {
  $is_end_session = $row.ts - $state.0 > $max_len OR $row.ts - $state.1 > $timeout;
  $new_state = AsTuple(IF($is_end_session, $row.ts, $state.0), $row.ts);
  return AsTuple($is_end_session, $new_state);
};
$calculate = ($row, $state) -> ($row.ts);
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- то же что и session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(ts, $init, $update, $calculate) AS session_start
```

`SessionWindow` может использоваться в `GROUP BY` только один раз.

## ROLLUP, CUBE и GROUPING SETS {#rollup}

Результаты вычисления агрегатной функции в виде промежуточных итогов для групп и общих итогов для отдельных столбцов или всей таблицы.

#### Синтаксис

```yql
SELECT
    c1, c2,                          -- столбцы, по которым производится группировка

AGGREGATE_FUNCTION(c3) AS outcome_c  -- агрегатная функция (SUM, AVG, MIN, MAX, COUNT)

FROM table_name

GROUP BY
    GROUP_BY_EXTENSION(c1, c2)       -- расширение GROUP BY: ROLLUP, CUBE или GROUPING SETS
```

* `ROLLUP` — группирует значения столбцов в порядке их перечисления в аргументах (строго слева направо), формирует промежуточные итоги для каждой группы и общий итог.
* `CUBE` — группирует значения для всех возможных комбинаций столбцов, формирует промежуточные итоги для каждой группы и общий итог.
* `GROUPING SETS` — задает группы для промежуточных итогов.

`ROLLUP`, `CUBE` и `GROUPING SETS` можно комбинировать через запятую.

### GROUPING {#grouping}

В промежуточном итоге значения столбцов, которые не участвуют в вычислениях, заменяются на `NULL`. В общем итоге на `NULL` заменяются значения всех столбцов. `GROUPING` — функция, которая позволяет отличить исходные значения `NULL` от `NULL`, которые были добавлены при формировании общих и промежуточных итогов.

`GROUPING` возвращает битовую маску:

* `0` — `NULL` для исходного пустого значения.
* `1` — `NULL`, добавленный для промежуточного или общего итога.

#### Пример

```yql
SELECT
    column1,
    column2,
    column3,

    CASE GROUPING(
        column1,
        column2,
        column3,
    )
        WHEN 1  THEN "Subtotal: column1 and column2"
        WHEN 3  THEN "Subtotal: column1"
        WHEN 4  THEN "Subtotal: column2 and column3"
        WHEN 6  THEN "Subtotal: column3"
        WHEN 7  THEN "Grand total"
        ELSE         "Individual group"
    END AS subtotal,

    COUNT(*) AS rows_count

FROM my_table

GROUP BY
    ROLLUP(
        column1,
        column2,
        column3
    ),
    GROUPING SETS(
        (column2, column3),
        (column3)
        -- если добавить сюда ещё (column2), то в сумме
        -- эти ROLLUP и GROUPING SETS дали бы результат,
        -- аналогичный CUBE
    )
;
```

## DISTINCT {#distinct}

Применение [агрегатных функций](../builtins/aggregation.md) только к уникальным значениям столбца.

{% note info %}

Применение `DISTINCT` к вычислимым значениям на данный момент не реализовано. С этой целью можно использовать [подзапрос](select/from.md) или выражение `GROUP BY ... AS ...`.

{% endnote %}

#### Пример

```yql
SELECT
  key,
  COUNT(DISTINCT value) AS count -- топ-3 ключей по количеству уникальных значений
FROM my_table
GROUP BY key
ORDER BY count DESC
LIMIT 3;
```

Также ключевое слово `DISTINCT` может использоваться для выборки уникальных строк через [`SELECT DISTINCT`](select/distinct.md).


## COMPACT

Наличие [SQL хинта](lexer.md#sql-hints) `COMPACT` непосредственно после ключевого слова `GROUP` позволяет более эффективно выполнять агрегацию в тех случаях, когда автору запроса заранее известно, что ни для одной группы, определяемой значениями ключей агрегации, не встречаются большие объемы данных (больше примерно гигабайт или миллионов строк). Если это предположение на практике окажется неверным, то операция может завершиться ошибкой из-за превышения потребления оперативной памяти или работать значительно медленнее не-COMPACT версии.

В отличие от обычного GROUP BY, отключается стадия Map-side combiner и дополнительные Reduce для каждого поля с [DISTINCT](#distinct) агрегацией.

#### Пример

```yql
SELECT
  key,
  COUNT(DISTINCT value) AS count -- топ-3 ключей по количеству уникальных значений
FROM my_table
GROUP /*+ COMPACT() */ BY key
ORDER BY count DESC
LIMIT 3;
```


## HAVING {#having}

Фильтрация выборки `SELECT` по результатам вычисления [агрегатных функций](../builtins/aggregation.md). Синтаксис аналогичен конструкции [`WHERE`](select/where.md).

#### Пример

```yql
SELECT
    key
FROM my_table
GROUP BY key
HAVING COUNT(value) > 100;
```

