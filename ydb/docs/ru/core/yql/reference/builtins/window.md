# Список оконных функций в YQL

Синтаксис вызова оконных функций подробно описан в [отдельной статье](../syntax/select/window.md).

{% if feature_window_functions %}

Оконные функции позволяют выполнять вычисления на наборе строк, связанных с текущей строкой. В отличие от агрегатных функций, оконные функции не группируют строки в одну выходную строку: каждая строка входит в результат запроса по отдельности.

В этом случае на каждой строке оказывается результат агрегации, полученный на множестве строк из [рамки окна](../syntax/select/window.md#frame).

{% note info %}

Оконные функции можно использовать только в `SELECT` и `ORDER BY`.

{% endnote %}

## ROW_NUMBER

Номер строки в рамках [раздела](../syntax/select/window.md#partition). Без аргументов.

### Примеры

```yql
SELECT
    ROW_NUMBER() OVER w AS row_number,
    value
FROM my_table
WINDOW w AS (ORDER BY key);
```

## LAG / LEAD {#lag-lead}

Доступ к значению из строки [раздела](../syntax/select/window.md#partition), отстающей (`LAG`) или опережающей (`LEAD`) текущую на фиксированное число. В первом аргументе указывается выражение, к которому необходим доступ, а во втором — отступ в строках. Также можно указать третий аргумент, который будет использован как значение по умолчанию, если целевая строка находится за пределами раздела (иначе используется `NULL`).

### Примеры

```yql
SELECT
   int_value,
   LAG(int_value, 1) OVER w AS int_value_lag_1,
   LEAD(int_value, 2) OVER w AS int_value_lead_2,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## FIRST_VALUE / LAST_VALUE {#first-last-value}

Доступ к значениям из первой и последней (в порядке `ORDER BY` на окне) строк [рамки окна](../syntax/select/window.md#frame). Единственный аргумент - выражение, к которому необходим доступ.

### Примеры

```yql
SELECT
   int_value,
   FIRST_VALUE(int_value) OVER w AS int_value_first,
   LAST_VALUE(int_value) OVER w AS int_value_last,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## NTH_VALUE

Доступ к значения из заданной строки (в порядке `ORDER BY` на окне) [рамки окна](../syntax/select/window.md#frame). Аргументы - выражение, к которому необходим доступ и номер строки, начиная с 1.

### Примеры

```yql
SELECT
   int_value,
   NTH_VALUE(int_value, 2) OVER w AS int_value_nth_2,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## RANK / DENSE_RANK / PERCENT_RANK {#rank}

Пронумеровать группы соседних строк [раздела](../syntax/select/window.md#partition) с одинаковым значением выражения в аргументе. `DENSE_RANK` нумерует группы подряд, а `RANK` — пропускает `(N - 1)` значений, где `N` — число строк в предыдущей группе. `PERCENT_RANK` возвращает относительный ранг текущей строки: `(RANK - 1) / (общее количество строк раздела - 1)`. Если раздел содержит только одну строку, `PERCENT_RANK` возвращает 0.

Если аргумент не указан, нумерация основывается на порядке, заданном в `ORDER BY` окна.

### Примеры

```yql
SELECT
   int_value,
   RANK(int_value) OVER w AS int_value_rank,
   DENSE_RANK() OVER w AS dense_rank,
   PERCENT_RANK() OVER w AS percent_rank,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## NTILE

Распределяет строки упорядоченного [раздела](../syntax/select/window.md#partition) в заданное количество групп. Группы нумеруются, начиная с единицы. Для каждой строки функция NTILE возвращает номер группы,которой принадлежит строка.

### Примеры

```yql
SELECT
   int_value,
   NTILE(3) OVER w AS int_value_ntile_3,
FROM my_table
WINDOW w AS (ORDER BY key);
```

## CUME_DIST

Возвращает относительную позицию (> 0 и <= 1) строки в рамках [раздела](../syntax/select/window.md#partition). Без аргументов.

### Примеры

```yql
SELECT
   int_value,
   CUME_DIST() OVER w AS int_value_cume_dist,
FROM my_table
WINDOW w AS (ORDER BY key);
```

{% endif %}


## Агрегатные функции {#aggregate-functions}

Все [агрегатные функции](aggregation.md) также могут использоваться в роли оконных.
В этом случае на каждой строке оказывается результат агрегации, полученный на множестве строк из [рамки окна](../syntax/select/window.md#frame).

### Примеры

```yql
SELECT
    SUM(int_column) OVER w1 AS running_total,
    SUM(int_column) OVER w2 AS total,
FROM my_table
WINDOW
    w1 AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w2 AS ();
```


## SessionState() {#session-state}

Нестандартная оконная функция `SessionState()` (без аргументов) позволяет получить состояние расчета сессий из [SessionWindow](../syntax/select/group-by.md#session-window) для текущей строки.

Допускается только при наличии `SessionWindow()` в секции `PARTITION BY` определения окна.

