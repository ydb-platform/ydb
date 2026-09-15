# Обзор SELECT

## Определение

Возвращает результат вычисления выражений, указанных после `SELECT`. Может использоваться в сочетании с другими операциями для получения иного эффекта.

Примеры:

```yql
SELECT "Hello, world!";
```

```yql
SELECT 2 + 2;
```


## Процедура выполнения SELECT {#selectexec}

Результат запроса `SELECT` вычисляется следующим образом:

* определяется набор входных таблиц – вычисляются выражения после [FROM](from.md);
* к входным таблицам применяется [SAMPLE](sample.md) / [TABLESAMPLE](sample.md);
* выполняется [FLATTEN COLUMNS](../flatten.md#flatten-columns) или [FLATTEN BY](../flatten.md); алиасы, заданные во `FLATTEN BY`, становятся видны после этой точки;
* выполняются все [JOIN](../join.md);
* к полученным данным добавляются (или заменяются) колонки, заданные в [GROUP BY ... AS ...](../group_by.md) (выполняется после `WHERE` начиная с версии [2025.02](../../changelog/2025.02.md#group-by-expr-alias-where));
* выполняется [WHERE](where.md) - все данные не удовлетворяющие предикату отфильтровываются;
* выполняется [GROUP BY](../group_by.md), вычисляются значения агрегатных функций;
* выполняется фильтрация [HAVING](../group_by.md#having);
* вычисляются значения [оконных функций](../window.md);
* вычисляются выражения в `SELECT`;
* выражениям в `SELECT` назначаются имена заданные алиасами;
* к полученным таким образом колонкам применяется top-level [DISTINCT](distinct.md);
* таким же образом вычисляются все подзапросы в операторах [UNION [ALL]](operators.md#union), [INTERSECT [ALL]](operators.md#intersect), [EXCEPT [ALL]](operators.md#except) и выполняется их объединение, пересечение, исключение соответственно;
* выполняется сортировка согласно [ORDER BY](order_by.md);
* к полученному результату применяются [OFFSET и LIMIT](limit_offset.md).

## Порядок колонок в YQL {#orderedcolumns}

В стандартном SQL порядок колонок указанных в проекции (в `SELECT`) имеет значение. Помимо того, что порядок колонок должен сохраняться при отображении результатов запроса или при записи в новую таблицу, некоторые конструкции SQL этот порядок используют.
Это относится в том числе к операторам [UNION [ALL]](operators.md#union), [INTERSECT [ALL]](operators.md#intersect), [EXCEPT [ALL]](operators.md#except) и к позиционному [ORDER BY](order_by.md) (ORDER BY ordinal).

По умолчанию в YQL порядок колонок игнорируется:

* порядок колонок в выходных таблицах и в результатах запроса не определен
* схема данных результата `UNION [ALL]`, `INTERSECT [ALL]` и `EXCEPT [ALL]` выводится по именам колонок, а не по позициям

При включении `PRAGMA OrderedColumns;` порядок колонок сохраняется в результатах запроса и выводится из порядка колонок во входных таблицах по следующим правилам:

* `SELECT` с явным перечислением колонок задает соответствующий порядок;
* `SELECT` со звездочкой (`SELECT * FROM ...`) наследует порядок из своего входа;
* порядок колонок после [JOIN](../join.md): сначала колонки левой стороны, потом правой. Если порядок какой-либо из сторон присутствующей в выходе `JOIN` не определен, порядок колонок результата также не определен;
* порядок `UNION [ALL]`, `INTERSECT [ALL]` и `EXCEPT [ALL]` зависит от [режима выполнения](operators.md#positional-mode);
* порядок колонок для [AS_TABLE](from_as_table.md) не определен;


## Комбинация запросов {#combining-queries}

Результаты нескольких `SELECT` (или подзапросов) могут быть объединены с помощью ключевых слов `UNION` и `UNION ALL`.

```yql
query1 UNION [ALL] query2 (UNION [ALL] query3 ...)
```

Объединение более двух запросов интерпретируется как левоассоциативная операция, то есть

```yql
query1 UNION query2 UNION ALL query3
```

интерпретируется как

```yql
(query1 UNION query2) UNION ALL query3
```

Пересечение нескольких `SELECT` (или подзапросов) может быть вычислено с помощью ключевых слов `INTERSECT` и `INTERSECT ALL`.

```yql
query1 INTERSECT query2
```

Пересечение более двух запросов интерпретируется как левоассоциативная операция, то есть

```yql
query1 INTERSECT query2 INTERSECT query3
```

интерпретируется как

```yql
(query1 INTERSECT query2) INTERSECT query3
```

Исключение нескольких `SELECT` (или подзапросов) может быть вычислено с помощью ключевых слов `EXCEPT` и `EXCEPT ALL`.

```yql
query1 EXCEPT query2
```

Исключение более двух запросов интерпретируется как левоассоциативная операция, то есть

```yql
query1 EXCEPT query2 EXCEPT query3
```

интерпретируется как

```yql
(query1 EXCEPT query2) EXCEPT query3
```

Допускается одновременное использование разных операторов `UNION`, `INTERSECT` и `EXCEPT` в одном запросе. Тогда операторы `UNION` и `EXCEPT` имеют равный приоритет, который ниже, чем у `INTERSECT`.

Например, запросы

```yql
query1 UNION query2 INTERSECT query3

query1 UNION query2 EXCEPT query3

query1 EXCEPT query2 UNION query3
```

интерпретируется как

```yql
query1 UNION (query2 INTERSECT query3)

(query1 UNION query2) EXCEPT query3

(query1 EXCEPT query2) UNION query3
```

соответственно.

При наличии `ORDER BY/LIMIT/DISCARD/INTO RESULT` в объединяемых подзапросах применяются следующие правила:

* `ORDER BY/LIMIT/INTO RESULT` допускается только после последнего подзапроса;
* `DISCARD` допускается только перед первым подзапросом;
* указанные операторы действуют на результат `UNION [ALL]`, `INTERSECT [ALL]` или `EXCEPT [ALL]`, а не на подзапрос;
* чтобы применить оператор к подзапросу, подзапрос необходимо взять в скобки.


## Обращение к нескольким таблицам в одном запросе

В стандартном SQL для выполнения запроса по нескольким таблицам используется [UNION ALL](operators.md#union-all), который объединяет результаты двух и более `SELECT`. Это не совсем удобно для сценария использования, в котором требуется выполнить один и тот же запрос по нескольким таблицам (например, содержащим данные на разные даты). В YQL, чтобы было удобнее, в `SELECT` после `FROM` можно указывать не только одну таблицу или подзапрос, но и вызывать встроенные функции, позволяющие объединять данные нескольких таблиц.

Для этих целей определены следующие функции:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` — объединяет все перечисленные в аргументах таблицы.

`EACH($list_of_strings)` или `EACH($list_of_strings VIEW view_name)` — объединяет все таблицы, имена которых перечислены в списке строк. Опционально можно передать несколько списков в отдельных аргументах по аналогии с `CONCAT`.

{% note warning %}

Порядок, в котором будут объединены таблицы, всеми вышеперечисленными функциями не гарантируется.

Список таблиц вычисляется **до** запуска самого запроса. Поэтому созданные в процессе запроса таблицы не попадут в результаты функции.

{% endnote %}

По умолчанию схемы всех участвующих таблиц объединяются по правилам [UNION ALL](operators.md#union-all). Если объединение схем не желательно, то можно использовать функции с суффиксом `_STRICT`, например `CONCAT_STRICT`, которые работают полностью аналогично оригинальным, но считают любое расхождение в схемах таблиц ошибкой.

Все аргументы описанных выше функций могут быть объявлены отдельно через [именованные выражения](../expressions.md#named-nodes). В этом случае в них также допустимы и простые выражения посредством неявного вызова [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

Имя исходной таблицы, из которой изначально была получена каждая строка, можно получить при помощи функции [TablePath()](../../builtins/basic.md#tablepath).

#### Примеры

```yql
USE some_cluster;
SELECT * FROM CONCAT(
  `table1`,
  `table2`,
  `table3`);
```

```yql
USE some_cluster;
$indices = ListFromRange(1, 4);
$tables = ListMap($indices, ($index) -> {
    RETURN "table" || CAST($index AS String);
});
SELECT * FROM EACH($tables); -- идентично предыдущему примеру
```

## Поддерживаемые конструкции в SELECT

* [FROM](from.md)
* [FROM AS_TABLE](from_as_table.md)
* [FROM SELECT](from_select.md)
* [DISTINCT](distinct.md)
* [UNIQUE DISTINCT](unique_distinct_hints.md)
* [UNION INTERSECT EXCEPT](operators.md)
* [WITH](with.md)
* [WITHOUT](without.md)
* [WHERE](where.md)
* [ORDER BY](order_by.md)
* [ASSUME ORDER BY](assume_order_by.md)
* [LIMIT OFFSET](limit_offset.md)
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)
* [CONCAT](concat.md)
