{% if backend_name != "YDB" %}
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

{% endif %}

{% if backend_name == "YDB" %}

# SELECT

Конструкция `SELECT` осуществляет выборку данных из [строковых](../../../../concepts/datamodel/table.md#row-oriented-tables) и [колоночных](../../../../concepts/datamodel/table.md#column-oriented-tables) таблиц, [внешних таблиц](../../../../concepts/datamodel/external_table.md) и [источников данных](../../../../concepts/datamodel/external_data_source.md), [представлений](../../../../concepts/datamodel/view.md) и [топиков](topics.md). По умолчанию данные выборки возвращаются пользователю, однако `SELECT` может участвовать как часть более сложных конструкций, например, `UPSERT INTO ... SELECT`, `CREATE TABLE ... AS SELECT ...` и т.п.

Запрос `SELECT` может содержать одну выборку или несколько выборок, результаты которых объединяются операциями `UNION`, `EXCEPT` и `INTERSECT`. Ниже приведены схемы обеих форм, затем раскрыты варианты их составных частей.


## Синтаксис {#query-syntax}

### Одиночный запрос {#single-select-overview}

Для запроса с одним `SELECT` общий вид следующий:

```yql
[DISCARD]
SELECT [ALL | DISTINCT] <result_list>
[WITHOUT [IF EXISTS] <column_name> [, ...]]
[FROM <sources_and_joins>]
[WHERE <condition>]
[GROUP [COMPACT] BY [ALL] <grouping_element> [, ...]]
[HAVING <condition>]
[WINDOW <window_name> AS (<window_specification>) [, ...]]
[[ASSUME] ORDER BY <sort_key> [ASC | DESC] [, ...]]
[INTO RESULT <result_name>]
[LIMIT <row_count> [OFFSET <offset>]];
```

Обязательны только `SELECT` и `<result_list>`. Остальные секции добавляются по необходимости с учётом следующих правил:

| Часть запроса | Назначение |
| --- | --- |
| `SELECT <result_list>` | Вычисляет выражения, которые образуют выходные колонки. В списке могут быть колонки, вызовы функций, выражения и звёздочки. |
| `ALL` / [`DISTINCT`](distinct.md) | `ALL` сохраняет повторяющиеся строки и действует по умолчанию. `DISTINCT` оставляет различные строки результата. |
| [`WITHOUT`](without.md) | Исключает указанные колонки из результата со звёздочкой. `IF EXISTS` допускает отсутствие исключаемой колонки. |
| [`FROM`](from.md) | Задаёт [источники строк](#select-sources-overview), их [преобразования](#select-flatten-overview) и [соединения](#select-joins-overview). Без `FROM` можно вычислять выражения, например `SELECT 2 + 2`, но нельзя обращаться к входным колонкам, фильтровать или группировать их. |
| [`WHERE`](where.md) | Оставляет входные строки, для которых условие истинно. |
| [`GROUP BY`](group-by.md) | Объединяет строки в группы по [ключам и наборам группировки](#select-grouping-overview) для вычисления агрегатных функций. |
| [`HAVING`](group-by.md#having) | Фильтрует результаты агрегации. |
| [`WINDOW`](window.md) | Объявляет [именованные окна](#select-windows-overview) для вызовов оконных функций через `OVER`. |
| [`ORDER BY`](order_by.md) / [`ASSUME ORDER BY`](assume_order_by.md) | `ORDER BY` сортирует результат; `ASSUME ORDER BY` объявляет уже имеющуюся сортировку без её выполнения. Направление `ASC` используется по умолчанию, `DESC` задаёт обратный порядок. В качестве `<sort_key>` для `ORDER BY` можно использовать колонку или выражение. В `ASSUME ORDER BY` указываются имена выходных колонок. |
| [`INTO RESULT`](../into_result.md) | Задаёт имя результирующего набора. Имя может быть идентификатором, в том числе в обратных кавычках, или именованным выражением. |
| [`LIMIT` / `OFFSET`](limit_offset.md) | Пропускает `<offset>` строк и возвращает не более `<row_count>` следующих строк. `OFFSET` указывается вместе с `LIMIT`. `<row_count>` и `<offset>` — выражения. `NULL` в `LIMIT` снимает ограничение числа строк. Без `ORDER BY` набор выбранных строк не определён. |
| [`DISCARD`](../discard.md) | Выполняет запрос, но не возвращает его результат клиенту. В одиночном `SELECT` не сочетается с `INTO RESULT`. |

### Составной запрос {#compound-select-overview}

Операции `UNION`, `EXCEPT` и `INTERSECT` принимают результаты отдельных выборок. У каждой выборки свои источники, фильтры, группировка и окна; сортировку и ограничение общего результата можно задать после последней выборки:

```yql
[DISCARD]
SELECT ...
{ UNION | EXCEPT | INTERSECT } [ALL | DISTINCT]
SELECT ...
[...]
[[ASSUME] ORDER BY <sort_key> [ASC | DESC] [, ...]]
[INTO RESULT <result_name>]
[LIMIT <row_count> [OFFSET <offset>]];
```

Здесь каждый `SELECT ...` — часть [схемы одиночного запроса](#single-select-overview) от `SELECT` до `WINDOW` включительно.  Общие завершающие секции записываются в конце последнего `SELECT` без скобок, но действуют на результат всей комбинации.

| Операция | Результат |
| --- | --- |
| `UNION` | Строки, встречающиеся хотя бы в одной из двух выборок. |
| `EXCEPT` | Строки левой выборки, отсутствующие в правой. |
| `INTERSECT` | Строки, встречающиеся в обеих выборках. |

По умолчанию эти операции удаляют повторы, что соответствует `DISTINCT`. Квалификатор `ALL` учитывает число вхождений: `UNION ALL` сохраняет все строки обеих выборок, `EXCEPT ALL` вычитает число вхождений справа из числа вхождений слева, но не ниже нуля, а `INTERSECT ALL` оставляет меньшее из двух чисел вхождений. `UNION DISTINCT`, `EXCEPT` и `INTERSECT` доступны начиная с версии языка YQL `2025.03`.

`INTERSECT` имеет более высокий приоритет, чем `UNION` и `EXCEPT`; операции одного приоритета группируются слева направо. Правила сопоставления колонок при `UNION` описаны в разделе [Объединение результатов подзапросов](union.md).

`DISCARD` ставится перед первой выборкой и относится ко всему оператору. `INTO RESULT` задаёт имя общего результата. Для фактической сортировки результата комбинации используйте `ORDER BY`: операции над выборками сами по себе порядок строк не гарантируют.

Например, следующий запрос возвращает не более десяти первых строк после сортировки общего результата:

```yql
SELECT key FROM first_table
UNION ALL
SELECT key FROM second_table
ORDER BY key
LIMIT 10;
```

Чтобы сортировка и ограничение действовали только на одну выборку, эту выборку заключают в скобки. В следующем запросе из первой таблицы берутся не более десяти строк, а из второй — все строки:

```yql
(SELECT key FROM first_table ORDER BY key LIMIT 10)
UNION ALL
SELECT key FROM second_table;
```

### Список результатов {#select-list-overview}

Вместо `<result_list>` указываются выражения и квалифицированные звёздочки через запятую либо одна обычная звёздочка:

| Элемент | Назначение |
| --- | --- |
| [`<expression> [[AS] <alias>]`](../expressions.md) | Вычисляет выходную колонку; алиас задаёт её имя. Выражение может содержать вызовы обычных, агрегатных и оконных функций. |
| `<source_alias>.*` | Выбирает все колонки указанного источника; допускается вместе с другими элементами списка. |
| `*` | Выбирает все колонки. Обычную звёздочку нельзя смешивать с другими элементами списка. |

Секция `WITHOUT` располагается сразу после списка результатов. Колонку в ней можно указать как `<column_name>` или `<source_alias>.<column_name>`.

Подсказки о свойствах результата задаются в комментариях после `SELECT` и описаны в разделе [UNIQUE и DISTINCT](unique_distinct_hints.md).

### Источники данных {#select-sources-overview}

В секции [`FROM`](from.md) можно указать один источник или соединить несколько источников с помощью `JOIN`. Основные формы одного источника:

| Форма | Назначение |
| --- | --- |
| `[<external_data_source_name>.]<table_name> [VIEW <index_name>]` | Чтение таблицы, при необходимости через [указанный индекс](secondary_index.md). Путь к таблице можно заключить в обратные кавычки. |
| `[<external_data_source_name>.]<topic_name>` | Чтение [локального или внешнего топика](topics.md). Для ожидания новых сообщений используется `WITH (STREAMING = "TRUE")`; количество возвращаемых сообщений ограничивается `LIMIT`. |
| `(SELECT ...)` | Результат подзапроса, в том числе составного. Круглые скобки обязательны. |
| `$<name>` | Источник, заданный именованным выражением: например, результат подзапроса. |
| [`AS_TABLE(<expression>)`](from_as_table.md) | Список структур, преобразованный в табличный источник. |
| `(VALUES (<expression> [, ...]) [, ...])` | Строки, заданные непосредственно в запросе. Все строки должны иметь одинаковое число значений. |

Источнику можно назначить алиас: `[AS] <alias>`. Для `VALUES` после алиаса дополнительно допускается список имён колонок: `AS <alias> (<column_name> [, ...])`.

### Настройки источника (`WITH`) {#select-with-overview}

[Конструкция `WITH`](with.md) задаёт настройки чтения из источника. Доступные параметры и формы их значений зависят от типа источника, например, имеется возможность указать формат хранимых данных для внешних источников, схему для топиков и внешних источников, `WATERMARK` для потоковых запросов. Настройки указываются перед алиасом:

```yql
[<external_data_source_name>.]<table_name> [VIEW <index_name>]
[WITH (<table_hint> [, ...])]
[[AS] <alias>]
```

Каждая `<table_hint>` содержит имя параметра и, если требуется, значение в форме, заданной для этой настройки. Для одной настройки скобки после `WITH` необязательны. Например, `FORMAT = <format>` задаёт формат данных, а `SCHEMA <struct_type>` — схему при чтении файлов из внешнего источника или сообщений [топика](topics.md), в том числе локального.

### Преобразование данных (`FLATTEN`) {#select-flatten-overview}

После источника и его алиаса можно указать одно [преобразование `FLATTEN`](flatten.md):

```yql
FLATTEN COLUMNS
```

или одну из форм `FLATTEN BY`:

```yql
FLATTEN [OPTIONAL | LIST | DICT] BY <column_name> [AS <alias>]
FLATTEN [OPTIONAL | LIST | DICT] BY (<column_name> [AS <alias>] [, ...])
FLATTEN [OPTIONAL | LIST | DICT] BY (<expression> AS <alias> [, ...])
```

`FLATTEN COLUMNS` разворачивает поля структур в отдельные колонки, а `FLATTEN BY` — элементы контейнеров в строки. Список в скобках может содержать и колонки, и выражения. Для произвольного выражения алиас обязателен.

### Соединения источников {#select-joins-overview}

Вместо `<sources_and_joins>` в основной схеме можно указать источник и последовательность соединений. При соединении источников `WITH`, алиас и `FLATTEN` относятся к тому источнику, после которого записаны. Каждое [соединение с условием](join.md) имеет вид:

```yql
FROM [ANY] <source>
    [<join_type>] JOIN [ANY] <source>
        { ON <join_condition> | USING (<column_name> [, ...]) }
    [...]
```

Для `<join_type>` доступны следующие варианты:

| Тип | Назначение |
| --- | --- |
| `INNER` | Только совпавшие строки с обеих сторон; используется по умолчанию. |
| `LEFT [OUTER]` / `RIGHT [OUTER]` | Все строки указанной стороны и совпавшие строки другой; при отсутствии совпадения колонки другой стороны заполняются `NULL`. |
| `FULL [OUTER]` | Все строки обеих сторон с объединением совпавших. |
| `LEFT SEMI` / `RIGHT SEMI` | Только колонки указанной стороны и только строки, имеющие совпадение с другой стороной. |
| `LEFT ONLY` / `RIGHT ONLY` | Только колонки указанной стороны и только строки без совпадения с другой стороной. |
| `EXCLUSION` | Строки обеих сторон, не имеющие совпадения с другой стороной. |

`ON` задаёт равенство ключевых выражений, а `USING` перечисляет одноимённые ключевые колонки. `ANY` перед источником оставляет одну строку для каждого набора ключей соединения с этой стороны; какая именно строка останется, не определено.

Декартово произведение записывается отдельно, без `ON`, `USING` и `ANY`:

```yql
FROM <source>
    CROSS JOIN <source>
```

Соединения разных типов можно чередовать, добавляя следующее соединение после предыдущего. `ANY` без соединения не допускается. Соединение через запятую требует `PRAGMA AnsiImplicitCrossJoin;` и соответствует `CROSS JOIN`.

### Ключи и наборы группировки {#select-grouping-overview}

[Секция `GROUP BY`](group-by.md) содержит один или несколько элементов группировки через запятую. В качестве `<grouping_element>` используются:

| Форма | Назначение |
| --- | --- |
| `<expression> [AS <alias>]` | Ключ группировки: колонка или результат выражения. Алиас позволяет обратиться к вычисленному ключу по имени. |
| `()` | Пустой набор ключей для общего итога. |
| `(<expression> [, ...])` | Набор ключей, например внутри `GROUPING SETS`. |
| [`ROLLUP(<expression> [, ...])`](group-by.md#rollup) | Итоги по последовательным наборам ключей и общий итог. |
| [`CUBE(<expression> [, ...])`](group-by.md#rollup) | Итоги по всем комбинациям ключей. |
| [`GROUPING SETS(<grouping_set> [, ...])`](group-by.md#rollup) | Явно заданные наборы ключей для вычисления итогов. |
| [`SessionWindow(<time_expression>, <session_timeout>) AS <alias>`](group-by.md#session-window) | Группировка событий по сессиям. |
| [`HoppingWindow(<time_expression>, <hop>, <interval>)`](group-by.md#group-by-hopping_window) | Группировка по перекрывающимся временным окнам. |
| [`HOP(<time_expression>, <hop>, <interval>, <delay>)`](group-by.md#group-by-hop) | Группировка по временным окнам с параметром задержки. |

`<grouping_set>` задаёт ключ, набор ключей в скобках, пустой набор или вложенную конструкцию группировки. `<time_expression>` — выражение для времени события; `<session_timeout>` задаёт границу сессии, `<hop>` и `<interval>` — период сдвига и длительность окна, `<delay>` — задержку его закрытия в потоковом режиме.

`COMPACT` выбирает стратегию агрегации для небольшого объёма данных на каждый ключ. `ALL` в `GROUP BY` можно опустить; `GROUP BY DISTINCT` не поддерживается. Секция `HAVING` идёт после `GROUP BY` и позволяет отфильтровать группы по результатам агрегатных функций.

### Определения окон {#select-windows-overview}

[Оконная функция](window.md) вычисляет значение по строкам, связанным с текущей строкой, сохраняя отдельные строки результата. Определение окна можно задать непосредственно после `OVER` или объявить в секции `WINDOW` и использовать по имени:

```yql
<window_function>([<argument> [, ...]]) OVER (<window_specification>)
<window_function>([<argument> [, ...]]) OVER <window_name>
```

Секция `WINDOW` с рамкой `ROWS` имеет вид:

```yql
WINDOW <window_name> AS (
    [PARTITION [COMPACT] BY <expression> [AS <alias>] [, ...]]
    [ORDER BY <sort_key> [ASC | DESC] [, ...]]
    [ROWS { <frame_bound> | BETWEEN <frame_start> AND <frame_end> }]
) [, ...]
```

| Часть определения | Назначение |
| --- | --- |
| [`PARTITION BY`](window.md#partition) | Делит строки на независимые разделы. Без `PARTITION BY` все входные строки образуют один раздел. Ключом может быть колонка или выражение; выражению можно назначить алиас. |
| [`ORDER BY`](window.md#partition) | Задаёт порядок строк внутри раздела. Направление `ASC` используется по умолчанию, `DESC` задаёт обратный порядок. Для упорядочивания результата запроса нужен отдельный `ORDER BY` после секции `WINDOW`. |
| [`ROWS`](window.md#frame) | Задаёт рамку по смещениям в строках относительно текущей строки. На рамке вычисляются агрегатные функции, а также `FIRST_VALUE`, `LAST_VALUE` и `NTH_VALUE`. Функции `ROW_NUMBER`, `LAG` и `LEAD` работают со всем разделом независимо от рамки. |
| Рамка по умолчанию | Если рамка не задана, при наличии оконного `ORDER BY` используется `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, а без него — `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. Для получения последнего значения всего раздела через `LAST_VALUE` явно задайте конец рамки `UNBOUNDED FOLLOWING`. |
| [`COMPACT`](window.md) | Выбирает выполнение в памяти для рамки `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. Потребление памяти зависит от размера раздела. |

Вместо `<frame_bound>`, `<frame_start>` и `<frame_end>` указывается одна из форм:

```yql
UNBOUNDED PRECEDING
<frame_offset> PRECEDING
CURRENT ROW
<frame_offset> FOLLOWING
UNBOUNDED FOLLOWING
```

Формы `PRECEDING` отсчитываются назад, `FOLLOWING` — вперёд; `UNBOUNDED` означает границу раздела. Если указана только одна граница, она задаёт начало, а конец считается равным `CURRENT ROW`. Начало не может быть `UNBOUNDED FOLLOWING`, а конец — `UNBOUNDED PRECEDING`. `<frame_offset>` — неотрицательное целое число. Для однозначного порядка строк при одинаковых значениях `<sort_key>` добавьте в оконный `ORDER BY` ключ, различающий эти строки.

### Другие формы записи {#select-alternative-overview}

Источник можно записать перед `SELECT` в форме [`FROM ... SELECT`](from_select.md). Список результатов и остальные секции сохраняют свой порядок:

```yql
[DISCARD]
FROM <sources_and_joins>
SELECT [ALL | DISTINCT] <result_list>
[WITHOUT [IF EXISTS] <column_name> [, ...]]
[WHERE <condition>]
[GROUP [COMPACT] BY [ALL] <grouping_element> [, ...]]
[HAVING <condition>]
[WINDOW <window_name> AS (<window_specification>) [, ...]]
[[ASSUME] ORDER BY <sort_key> [ASC | DESC] [, ...]]
[INTO RESULT <result_name>]
[LIMIT <row_count> [OFFSET <offset>]];
```

В одной выборке допускается только одна секция `FROM`.

Для ограничения результата также поддерживается форма с запятой:

```yql
LIMIT <offset>, <row_count>
```

Она эквивалентна `LIMIT <row_count> OFFSET <offset>`; порядок двух аргументов меняется.

{% endif %}

## Процедура выполнения SELECT {#selectexec}

Результат запроса `SELECT` вычисляется следующим образом:

* определяется набор входных таблиц – вычисляются выражения после [FROM](../select/from.md);
{% if feature_match_recogznize==true %}
* к входным таблицам применяется [MATCH_RECOGNIZE](match_recognize.md)
{% endif %}
{% if feature_tablesample==true %}
* вычисляется [SAMPLE](sample.md) / [TABLESAMPLE](sample.md)
{% endif %}
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

{% if backend_name != "YDB" %}

## Обращение к нескольким таблицам в одном запросе

В стандартном SQL для выполнения запроса по нескольким таблицам используется [UNION ALL](union.md#union-all), который объединяет результаты двух и более `SELECT`. Это не совсем удобно для сценария использования, в котором требуется выполнить один и тот же запрос по нескольким таблицам (например, содержащим данные на разные даты). В YQL, чтобы было удобнее, в `SELECT` после `FROM` можно указывать не только одну таблицу или подзапрос, но и вызывать встроенные функции, позволяющие объединять данные нескольких таблиц.

Для этих целей определены следующие функции:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` — объединяет все перечисленные в аргументах таблицы.

`EACH($list_of_strings)` или `EACH($list_of_strings VIEW view_name)` — объединяет все таблицы, имена которых перечислены в списке строк. Опционально можно передать несколько списков в отдельных аргументах по аналогии с `CONCAT`.

```RANGE(`prefix`, `min`, `max`, `suffix`, `view`)``` — объединяет диапазон таблиц. Аргументы:

* prefix — каталог для поиска таблиц, указывается без завершающего слеша. Единственный обязательный аргумент, если указан только он, то используются все таблицы в данном каталоге.
* min, max — следующие два аргумента задают диапазон имен для включения таблиц. Диапазон инклюзивный с обоих концов. Если диапазон не указан, используются все таблицы в каталоге prefix. Имена таблиц или директорий, находящихся в указанной в prefix директории, сравниваются с диапазоном `[min, max]` лексикографически, а не конкатенируются, таким образом важно указывать диапазон без лидирующих слешей.
* suffix — имя таблицы. Ожидается без начального слеша. Если suffix не указан, то аргументы `[min, max]` задают диапазон имен таблиц. Если suffix указан, то аргументы `[min, max]` задают диапазон папок, в которых существует таблица с именем, указанным в аргументе suffix.

```LIKE(`prefix`, `pattern`, `suffix`, `view`)` и `REGEXP(`prefix`, `pattern`, `suffix`, `view`)``` — аргумент pattern задается в формате, аналогичном одноименным бинарным операторам: [LIKE](../expressions.md#check-match) и [REGEXP](../expressions.md#check-match).

```FILTER(`prefix`, `callable`, `suffix`, `view`)``` — аргумент callable должен являться вызываемым выражением с сигнатурой `(String)->Bool`, который будет вызван для каждой таблицы/подкаталога в каталоге prefix. В запросе будут участвовать только те таблицы, для которых вызываемое значение вернуло `true`. В качестве вызываемого значения удобнее всего использовать [лямбда функции](../expressions.md#lambda){% if yql == true %}, либо UDF на [Python](../../udf/python.md) или [JavaScript](../../udf/javascript.md){% endif %}.

{% note warning %}

Порядок, в котором будут объединены таблицы, всеми вышеперечисленными функциями не гарантируется.

Список таблиц вычисляется **до** запуска самого запроса. Поэтому созданные в процессе запроса таблицы не попадут в результаты функции.

{% endnote %}

По умолчанию схемы всех участвующих таблиц объединяются по правилам [UNION ALL](union.md#union-all). Если объединение схем не желательно, то можно использовать функции с суффиксом `_STRICT`, например `CONCAT_STRICT` или `RANGE_STRICT`, которые работают полностью аналогично оригинальным, но считают любое расхождение в схемах таблиц ошибкой.

Для указания кластера объединяемых таблиц нужно указать его перед названием функции.

Все аргументы описанных выше функций могут быть объявлены отдельно через [именованные выражения](../expressions.md#named-nodes). В этом случае в них также допустимы и простые выражения посредством неявного вызова [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

Имя исходной таблицы, из которой изначально была получена каждая строка, можно получить при помощи функции {% if feature_bulk_tables %}[TablePath()](../../builtins/basic.md#tablepath){% else %}TablePath(){% endif %}.

### Примеры

```yql
SELECT * FROM CONCAT(
  `table1`,
  `table2`,
  `table3`);
```

```yql
$indices = ListFromRange(1, 4);
$tables = ListMap($indices, ($index) -> {
    RETURN "table" || CAST($index AS String);
});
SELECT * FROM EACH($tables); -- идентично предыдущему примеру
```

```yql
SELECT * FROM RANGE(`my_folder`);
```

```yql
SELECT * FROM some_cluster.RANGE( -- Кластер можно указать перед названием функции
  `my_folder`,
  `from_table`,
  `to_table`);
```

```yql
SELECT * FROM RANGE(
  `my_folder`,
  `from_folder`,
  `to_folder`,
  `my_table`);
```

```yql
SELECT * FROM RANGE(
  `my_folder`,
  `from_table`,
  `to_table`,
  ``,
  `my_view`);
```

```yql
SELECT * FROM LIKE(
  `my_folder`,
  "2017-03-%"
);
```

```yql
SELECT * FROM REGEXP(
  `my_folder`,
  "2017-03-1[2-4]?"
);
```

```yql
$callable = ($table_name) -> {
    return $table_name > "2017-03-13";
};

SELECT * FROM FILTER(
  `my_folder`,
  $callable
);
```

{% endif %}

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
{% if feature_tablesample==true %}
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)
{% endif %}
{% if feature_match_recogznize==true %}
* [MATCH_RECOGNIZE](match_recognize.md)
{% endif %}
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
* [VIEW fulltext_index](fulltext_index.md)
* [Гибридный поиск](hybrid_search.md)
* [VIEW json_index](json_index.md)

{% endif %}
