## Табличные выражения {#table-contexts}

Табличное выражения – это выражение, которое возвращает таблицу. Табличными выражениями в YQL являются:
* подзапросы: `(SELECT key, subkey FROM T)`
* [именованные подзапросы](#named-nodes): `$foo = SELECT * FROM T;` (использование именованного подзапроса `$foo` является табличным выражением)
{% if feature_subquery %}
* [шаблоны подзапросов](../../subquery.md#define-subquery): `DEFINE SUBQUERY $foo($name) AS ... END DEFINE;` (вызов `$foo("InputTable")` является табличным выражением).
{% endif %}

Семантика табличного выражения зависит от контекста в котором оно используется. В YQL табличные выражения могут применяться в следующих контекстах:
* табличный контекст - после [FROM](../../select/from.md).
Здесь табличные выражения работают как ожидается – например `$input = SELECT a, b, c FROM T; SELECT * FROM $input` вернет таблицу с тремя колонками.
Табличный контекст также возникает после [UNION ALL](../../select/index.md#unionall){% if feature_join %}, [JOIN](../../join.md#join){% endif %}{% if feature_mapreduce and process_command == "PROCESS" %}, [PROCESS](../../process.md#process), [REDUCE](../../reduce.md#reduce){% endif %};
* векторный контекст - после [IN](#in). В этом контексте табличное выражение обязано содержать ровно одну колонку (имя этой колонки никак не влияет на результат выражения).
Табличное выражение в векторном контексте типизируется как список (тип элемента списка при этом совпадает с типом колонки). Пример: `SELECT * FROM T WHERE key IN (SELECT k FROM T1)`;
* скалярный контекст возникает _во всех остальных случаях_. Как и в векторном контексте,
табличное выражение должно содержать ровно одну колонку, но значением табличного выражения будет скаляр –
произвольно выбранное значение этой колонки (если получилось ноль строк, то результатом будет `NULL`). Пример: `$count = SELECT COUNT(*) FROM T; SELECT * FROM T ORDER BY key LIMIT $count / 2`;

Порядок строк в табличном контексте, порядок элементов в векторном контексте и правило выбора значения в скалярном контексте (в случае если значений несколько) не определены. На этот порядок также нельзя повлиять с помощью `ORDER BY`: `ORDER BY` без `LIMIT` в табличных выражениях будет игнорироваться с выдачей предупреждения, а использование `ORDER BY` с `LIMIT` определяет множество элементов, но не порядок внутри этого множества.

{% if feature_mapreduce and process_command == "PROCESS" %}

Из этого правила есть исключение. Именованное выражение с [PROCESS](../../process.md#process), будучи использованным в скалярном контексте, ведет себя как в табличном:

```yql
$input = SELECT 1 AS key, 2 AS value;
$process = PROCESS $input;

SELECT FormatType(TypeOf($process)); -- $process используется в скалярном контексте,
                                     -- но результат SELECT при этом - List<Struct<'key':Int32,'value':Int32>>

SELECT $process[0].key; -- вернет 1

SELECT FormatType(TypeOf($input)); -- ошибка: $input в скалярном контексте должен содержать одну колонку
```
{% note warning "Внимание" %}

Часто встречающейся ошибкой является использование выражения в скалярном контексте вместо табличного или векторного. Например:

```yql
$dict = SELECT key, value FROM T1;

DEFINE SUBQUERY $merge_dict($table, $dict) AS
SELECT * FROM $table LEFT JOIN $dict USING(key);
END DEFINE;

SELECT * FROM $merge_dict("Input", $dict); -- $dict здесь используется в скалярном контексте.
                                           -- ошибка - в скалярном контексте ожидается ровно одна колонка

```

Правильное решение в данном случае выглядит так:

```yql
DEFINE SUBQUERY $dict() AS
SELECT key, value FROM T1;
END DEFINE;

DEFINE SUBQUERY $merge_dict($table, $dict) AS
SELECT * FROM $table LEFT JOIN $dict() USING(key); -- использование табличного выражения $dict()
                                                   -- (вызов шаблона подзапроса) в табличном контексте
END DEFINE;

SELECT * FROM $merge_dict("Input", $dict); -- $dict - шаблон позапроса (не табличное выражение)
                                           -- передаваемый в качестве аргумента табличного выражения
```

{% endnote %}
{% endif %}
