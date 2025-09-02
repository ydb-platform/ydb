## SELECT

Возвращает результат вычисления выражений, указанных после `SELECT`.

Может использоваться в сочетании с другими операциями для получения иного эффекта.

### Примеры

```yql
SELECT "Hello, world!";
```

```yql
SELECT 2 + 2;
```


## Процедура выполнения SELECT {#selectexec}

Результат запроса `SELECT` вычисляется следующим образом:

* определяется набор входных таблиц – вычисляются выражения после [FROM](../select/from.md);
* к входным таблицам применяется [MATCH_RECOGNIZE](match_recognize.md)
* вычисляется [SAMPLE](sample.md) / [TABLESAMPLE](sample.md)
* выполняется [FLATTEN COLUMNS](flatten.md#flatten-columns) или [FLATTEN BY](flatten.md); алиасы, заданные во `FLATTEN BY`, становятся видны после этой точки;
{% if feature_join %}
* выполняются все [JOIN](join.md);
{% endif %}
* к полученным данным добавляются (или заменяются) колонки, заданные в [GROUP BY ... AS ...](group-by.md);
* выполняется [WHERE](where.md): все данные, не удовлетворяющие предикату, отфильтровываются;
* выполняется [GROUP BY](group-by.md), вычисляются значения агрегатных функций;
* выполняется фильтрация [HAVING](group-by.md#having);
{% if feature_window_functions %}
* вычисляются значения [оконных функций](window.md);
{% endif %}
* вычисляются выражения в `SELECT`;
* выражениям в `SELECT` назначаются имена заданные алиасами;
* к полученным таким образом колонкам применяется top-level [DISTINCT](distinct.md);
* таким же образом вычисляются все подзапросы в [UNION ALL](union.md#union-all), выполняется их объединение (см. [PRAGMA AnsiOrderByLimitInUnionAll](../pragma.md#pragmas));
* выполняется сортировка согласно [ORDER BY](order_by.md);
* к полученному результату применяются [OFFSET и LIMIT](limit_offset.md).

## Порядок колонок в YQL {#orderedcolumns}

В стандартном SQL порядок колонок указанных в проекции (в `SELECT`) имеет значение. Помимо того, что порядок колонок должен сохраняться при отображении результатов запроса или при записи в новую таблицу, некоторые конструкции SQL этот порядок используют.
Это относится в том числе к [UNION ALL](union.md#union-all) и к позиционному [ORDER BY](order_by.md) (ORDER BY ordinal).

По умолчанию в YQL порядок колонок игнорируется:

* порядок колонок в выходных таблицах и в результатах запроса не определен
* схема данных результата `UNION ALL` выводится по именам колонок, а не по позициям

При включении `PRAGMA OrderedColumns;` порядок колонок сохраняется в результатах запроса и выводится из порядка колонок во входных таблицах по следующим правилам:

* `SELECT` с явным перечислением колонок задает соответствующий порядок;
* `SELECT` со звездочкой (`SELECT * FROM ...`) наследует порядок из своего входа;

{% if feature_join %}
* порядок колонок после [JOIN](join.md): сначала колонки левой стороны, потом правой. Если порядок какой-либо из сторон, присутствующей в выходе `JOIN`, не определен, порядок колонок результата также не определен;
{% endif %}

* порядок `UNION ALL` зависит от режима выполнения [UNION ALL](union.md#union-all);
* порядок колонок для [AS_TABLE](from_as_table.md) не определен;


## Комбинация запросов {#combining-queries}

Результаты нескольких SELECT (или подзапросов) могут быть объединены с помощью ключевых слов `UNION` и `UNION ALL`.

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

При наличии `ORDER BY/LIMIT/DISCARD/INTO RESULT` в объединяемых подзапросах применяются следующие правила:

* `ORDER BY/LIMIT/INTO RESULT` допускается только после последнего подзапроса;
* `DISCARD` допускается только перед первым подзапросом;
* указанные операторы действуют на результат `UNION [ALL]`, а не на подзапрос;
* чтобы применить оператор к подзапросу, подзапрос необходимо взять в скобки.


## Обращение к нескольким таблицам в одном запросе

В стандартном SQL для выполнения запроса по нескольким таблицам используется [UNION ALL](union.md#union-all), который объединяет результаты двух и более `SELECT`. Это не совсем удобно для сценария использования, в котором требуется выполнить один и тот же запрос по нескольким таблицам (например, содержащим данные на разные даты). В YQL, чтобы было удобнее, в `SELECT` после `FROM` можно указывать не только одну таблицу или подзапрос, но и вызывать встроенные функции, позволяющие объединять данные нескольких таблиц.

Для этих целей определены следующие функции:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` — объединяет все перечисленные в аргументах таблицы.

`EACH($list_of_strings)` или `EACH($list_of_strings VIEW view_name)` — объединяет все таблицы, имена которых перечислены в списке строк. Опционально можно передать несколько списков в отдельных аргументах по аналогии с `CONCAT`.

```RANGE(`prefix`, `min`, `max`, `suffix`, `view`)``` — объединяет диапазон таблиц. Аргументы:

* prefix — каталог для поиска таблиц, указывается без завершающего слеша. Единственный обязательный аргумент, если указан только он, то используются все таблицы в данном каталоге.
* min, max — следующие два аргумента задают диапазон имен для включения таблиц. Диапазон инклюзивный с обоих концов. Если диапазон не указан, используются все таблицы в каталоге prefix. Имена таблиц или директорий, находящихся в указанной в prefix директории, сравниваются с диапазоном `[min, max]` лексикографически, а не конкатенируются, таким образом важно указывать диапазон без лидирующих слешей.
* suffix — имя таблицы. Ожидается без начального слеша. Если suffix не указан, то аргументы `[min, max]` задают диапазон имен таблиц. Если suffix указан, то аргументы `[min, max]` задают диапазон папок, в которых существует таблица с именем, указанным в аргументе suffix.

```LIKE(`prefix`, `pattern`, `suffix`, `view`)` и `REGEXP(`prefix`, `pattern`, `suffix`, `view`)``` — аргумент pattern задается в формате, аналогичном одноименным бинарным операторам: [LIKE](../expressions.md#like) и [REGEXP](../expressions.md#regexp).

```FILTER(`prefix`, `callable`, `suffix`, `view`)``` — аргумент callable должен являться вызываемым выражением с сигнатурой `(String)->Bool`, который будет вызван для каждой таблицы/подкаталога в каталоге prefix. В запросе будут участвовать только те таблицы, для которых вызываемое значение вернуло `true`. В качестве вызываемого значения удобнее всего использовать [лямбда функции](../expressions.md#lambda){% if yql == true %}, либо UDF на [Python](../../udf/python.md) или [JavaScript](../../udf/javascript.md){% endif %}.

{% note warning %}

Порядок, в котором будут объединены таблицы, всеми вышеперечисленными функциями не гарантируется.

Список таблиц вычисляется **до** запуска самого запроса. Поэтому созданные в процессе запроса таблицы не попадут в результаты функции.

{% endnote %}

По умолчанию схемы всех участвующих таблиц объединяются по правилам [UNION ALL](union.md#union-all). Если объединение схем не желательно, то можно использовать функции с суффиксом `_STRICT`, например `CONCAT_STRICT` или `RANGE_STRICT`, которые работают полностью аналогично оригинальным, но считают любое расхождение в схемах таблиц ошибкой.

Для указания кластера объединяемых таблиц нужно указать его перед названием функции.

Все аргументы описанных выше функций могут быть объявлены отдельно через [именованные выражения](../expressions.md#named-nodes). В этом случае в них также допустимы и простые выражения посредством неявного вызова [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

Имя исходной таблицы, из которой изначально была получена каждая строка, можно получить при помощи функции [TablePath()](../../builtins/basic.md#tablepath).

### Примеры

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

```yql
USE some_cluster;
SELECT * FROM RANGE(`my_folder`);
```

```yql
SELECT * FROM some_cluster.RANGE( -- Кластер можно указать перед названием функции
  `my_folder`,
  `from_table`,
  `to_table`);
```

```yql
USE some_cluster;
SELECT * FROM RANGE(
  `my_folder`,
  `from_folder`,
  `to_folder`,
  `my_table`);
```

```yql
USE some_cluster;
SELECT * FROM RANGE(
  `my_folder`,
  `from_table`,
  `to_table`,
  ``,
  `my_view`);
```

```yql
USE some_cluster;
SELECT * FROM LIKE(
  `my_folder`,
  "2017-03-%"
);
```

```yql
USE some_cluster;
SELECT * FROM REGEXP(
  `my_folder`,
  "2017-03-1[2-4]?"
);
```

```yql
$callable = ($table_name) -> {
    return $table_name > "2017-03-13";
};

USE some_cluster;
SELECT * FROM FILTER(
  `my_folder`,
  $callable
);
```

## Поддерживаемые конструкции в SELECT

* [FROM](from.md)
* [FROM AS_TABLE](from_as_table.md)
* [FROM SELECT](from_select.md)
* [DISTINCT](distinct.md)
* [UNIQUE DISTINCT](unique_distinct_hints.md)
* [UNION](union.md)
* [WITH](with.md)
* [WITHOUT](without.md)
* [WHERE](where.md)
* [ORDER BY](order_by.md)
* [ASSUME ORDER BY](assume_order_by.md)
* [LIMIT OFFSET](limit_offset.md)
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)
* [MATCH_RECOGNIZE](match_recognize.md)
{% if feature_join %}
* [JOIN](join.md)
{% endif %}
* [GROUP BY](group-by.md)
* [FLATTEN](flatten.md)
{% if feature_window_functions %}
* [WINDOW](window.md)
{% endif %}

{% if yt %}

* [FOLDER](folder.md)
* [WalkFolders](walk_folders.md)

{% endif %}

{% if feature_mapreduce %}

* [VIEW](view.md)

{% endif %}

{% if feature_temp_table %}

* [TEMPORARY TABLE](temporary_table.md)

{% endif %}

{% if feature_bulk_tables %}

* [CONCAT](concat.md)

{% endif %}

{% if feature_secondary_index %}

* [VIEW secondary_index](secondary_index.md)

* [VIEW vector_index](vector_index.md)

{% endif %}