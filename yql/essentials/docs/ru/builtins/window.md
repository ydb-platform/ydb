
# Список оконных функций в YQL

Синтаксис вызова оконных функций подробно описан в [отдельной статье](../syntax/window.md).


## Агрегатные функции {#aggregate-functions}

Все [агрегатные функции](aggregation.md) также могут использоваться в роли оконных.
В этом случае на каждой строке оказывается результат агрегации, полученный на множестве строк из [рамки окна](../syntax/window.md#frame).

#### Примеры

```yql
SELECT
    SUM(int_column) OVER w1 AS running_total,
    SUM(int_column) OVER w2 AS total,
FROM my_table
WINDOW
    w1 AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w2 AS ();
```


## ROW_NUMBER {#row_number}

Номер строки в рамках [раздела](../syntax/window.md#partition). Без аргументов.

#### Сигнатура

```yql
ROW_NUMBER()->Uint64
```

#### Примеры

```yql
SELECT
    ROW_NUMBER() OVER w AS row_num
FROM my_table
WINDOW w AS (ORDER BY key);
```


## LAG / LEAD {#lag-lead}

Доступ к значению из строки [раздела](../syntax/window.md#partition), отстающей (`LAG`) или опережающей (`LEAD`) текущую на фиксированное число. В первом аргументе указывается выражение, к которому необходим доступ, а во втором — отступ в строках. Отступ можно не указывать, по умолчанию используется соседняя строка — предыдущая или следующая, соответственно, то есть подразумевается 1. В строках, для которых нет соседей с заданным расстоянием (например `LAG(expr, 3)` в первой и второй строках раздела), возвращается `NULL`.

#### Сигнатура

```yql
LEAD(T[,Int32])->T?
LAG(T[,Int32])->T?
```

#### Примеры

```yql
SELECT
   int_value - LAG(int_value) OVER w AS int_value_diff
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT item, odd, LAG(item, 1) OVER w as lag1 FROM (
    SELECT item, item % 2 as odd FROM (
        SELECT AsList(1, 2, 3, 4, 5, 6, 7) as item
    )
    FLATTEN BY item
)
WINDOW w As (
    PARTITION BY odd
    ORDER BY item
);

/* Output:
item  odd  lag1
--------------------
2  0  NULL
4  0  2
6  0  4
1  1  NULL
3  1  1
5  1  3
7  1  5
*/
```


## FIRST_VALUE / LAST_VALUE {#first-last-value}

Доступ к значениям из первой и последней (в порядке `ORDER BY` на окне) строк [рамки окна](../syntax/window.md#frame). Единственный аргумент - выражение, к которому необходим доступ.

Опционально перед `OVER` может указываться дополнительный модификатор `IGNORE NULLS`, который меняет поведение функций на первое или последнее **не пустое** (то есть не `NULL`) значение среди строк рамки окна. Антоним этого модификатора — `RESPECT NULLS` является поведением по умолчанию и может не указываться.

#### Сигнатура

```yql
FIRST_VALUE(T)->T?
LAST_VALUE(T)->T?
```

#### Примеры

```yql
SELECT
   FIRST_VALUE(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT
   LAST_VALUE(my_column) IGNORE NULLS OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

## NTH_VALUE

Доступ к значения из заданной строки (в порядке `ORDER BY` на окне) [рамки окна](../syntax/window.md#frame). Аргументы - выражение, к которому необходим доступ и номер строки, начиная с 1.

Опционально перед `OVER` может указываться дополнительный модификатор `IGNORE NULLS`, который приводит к пропуску строк с NULL в значении первого аргумента. Антоним этого модификатора — `RESPECT NULLS` является поведением по умолчанию и может не указываться.

#### Сигнатура

```yql
NTH_VALUE(T,N)->T?
```

#### Примеры

```yql
SELECT
   NTH_VALUE(my_column, 2) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT
   NTH_VALUE(my_column, 3) IGNORE NULLS OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```


## RANK / DENSE_RANK / PERCENT_RANK {#rank}

Пронумеровать группы соседних строк [раздела](../syntax/window.md#partition) с одинаковым значением выражения в аргументе. `DENSE_RANK` нумерует группы подряд, а `RANK` — пропускает `(N - 1)` значений, где `N` — число строк в предыдущей группе. `PERCENT_RANK` выдает относительный ранг текущей строки: `(RANK - 1)/(число строк в разделе - 1)`.

При отсутствии аргумента использует порядок, указанный в секции `ORDER BY` определения окна.
Если аргумент отсутствует и `ORDER BY` не указан, то все строки считаются равными друг другу.

{% note info %}

Возможность передавать аргумент в `RANK`/`DENSE_RANK`/`PERCENT_RANK` является нестандартным расширением YQL.

{% endnote %}

#### Сигнатура

```yql
RANK([T])->Uint64
DENSE_RANK([T])->Uint64
PERCENT_RANK([T])->Double
```

#### Примеры

```yql
SELECT
   RANK(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

```yql
SELECT
   DENSE_RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);
```

```yql
SELECT
   PERCENT_RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);
```


## NTILE

Распределяет строки упорядоченного [раздела](../syntax/window.md#partition) в заданное количество групп. Группы нумеруются, начиная с единицы. Для каждой строки функция NTILE возвращает номер группы,которой принадлежит строка.

#### Сигнатура

```yql
NTILE(Uint64)->Uint64
```

#### Примеры

```yql
SELECT
    NTILE(10) OVER w AS group_num
FROM my_table
WINDOW w AS (ORDER BY key);
```


## CUME_DIST

Возвращает относительную позицию (> 0 и <= 1) строки в рамках [раздела](../syntax/window.md#partition). Без аргументов.

#### Сигнатура

```yql
CUME_DIST()->Double
```

#### Примеры

```yql
SELECT
    CUME_DIST() OVER w AS dist
FROM my_table
WINDOW w AS (ORDER BY key);
```



## SessionState {#session-state}

Нестандартная оконная функция `SessionState()` (без аргументов) позволяет получить состояние расчета сессий из [SessionWindow](../syntax/group_by.md#session-window) для текущей строки.

Допускается только при наличии `SessionWindow()` в секции `PARTITION BY` определения окна.

