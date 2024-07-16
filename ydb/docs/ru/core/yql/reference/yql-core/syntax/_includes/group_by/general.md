## GROUP BY

Группирует результаты `SELECT` по значениям указанных столбцов или выражений. Вместе с `GROUP BY` часто применяются [агрегатные функции](../../../builtins/aggregation.md) (`COUNT`, `MAX`, `MIN`, `SUM`, `AVG`) для выполнения вычислений в каждой группе.

**Синтаксис**
```sql
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

Запрос вида `SELECT * FROM table GROUP BY k1, k2, ...` вернет все колонки, перечисленные в GROUP BY, то есть экивалентент запросу `SELECT DISTINCT k1, k2, ... FROM table`.

Звездочка может также применяться в качестве аргумента агрегатной функции `COUNT`. `COUNT(*)` означает "число строк в группе".


{% note info %}

Агрегатные функции не учитывают `NULL` в своих аргументах, за исключением функции `COUNT`.

{% endnote %}

Также в YQL доступен механизм фабрик агрегатных функций, реализованный с помощью функций [`AGGREGATION_FACTORY`](../../../builtins/basic.md#aggregationfactory) и [`AGGREGATE_BY`](../../../builtins/aggregation.md#aggregateby).

**Примеры**

```sql
SELECT key, COUNT(*) FROM my_table
GROUP BY key;
```

```sql
SELECT double_key, COUNT(*) FROM my_table
GROUP BY key + key AS double_key;
```

```sql
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


{% note warning "Внимание" %}

Возможность указывать имя для колонки или выражения в `GROUP BY .. AS foo` является расширением YQL. Такое имя становится видимым в `WHERE` несмотря на то, что фильтрация по `WHERE` выполняется [раньше](../../select/index.md#selectexec) группировки. В частности, если в таблице `T` есть две колонки `foo` и `bar`, то в запросе `SELECT foo FROM T WHERE foo > 0 GROUP BY bar AS foo` фильтрация фактически произойдет по колонке `bar` из исходной таблицы.

{% endnote %}
