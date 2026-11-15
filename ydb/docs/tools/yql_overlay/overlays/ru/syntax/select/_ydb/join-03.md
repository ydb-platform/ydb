### Примеры

```yql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
FULL JOIN b_table AS b USING (key);
```

```yql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
FULL JOIN b_table AS b ON a.key = b.key;
```

```yql
SELECT     a.value as a_value, b.value as b_value, c.column2
FROM       a_table AS a
CROSS JOIN b_table AS b
LEFT  JOIN c_table AS c ON c.ref = a.key and c.column1 = b.value;
```

Для исключения необходимости в полном сканировании правой присоединяемой таблицы может использоваться вторичный индекс над колонками, входящими в условие соединения. Обращение ко вторичному индексу должно быть указано в явном виде, в формате `JOIN table_name VIEW index_name AS table_alias`.

Например, создание индекса для использования в условии соединения:

```yql
ALTER TABLE b_table ADD INDEX b_index_ref GLOBAL ON(ref);
```

Использование созданного индекса:

```yql
SELECT    a.value as a_value, b.value as b_value
FROM      a_table AS a
INNER JOIN b_table VIEW b_index_ref AS b ON a.ref = b.ref;
```

{% if feature_mapreduce %}

Если в выражении помимо `JOIN` выполняется фильтрация данных, то рекомендуется обернуть те условия, про которые известно, что они вернут `true` для большинства строк, в вызов функции `LIKELY(...)`. Если предположение о преобладании положительных значений в условии окажется верно, такая подсказка может положительно сказаться на времени выполнения запроса. Также `LIKELY` может быть полезен в том случае, когда вычисление предиката ресурсоёмко и при этом сам JOIN значительно сокращает число строк.

Перед любым источником данных для `JOIN` можно указать ключевое слово `ANY`, которое служит для подавления дубликатов по ключам `JOIN` с соответствующей стороны. В этом случае из множества строк с одинаковым значением ключей `JOIN` остается только одна (не уточняется какая именно – отсюда и название `ANY`).
Данный синтаксис отличается от принятого в [ClickHouse]{% if lang == "en" %}(https://clickhouse.com/docs/en/sql-reference/statements/select/join/){% endif %}{% if lang == "ru" %}(https://clickhouse.tech/docs/ru/sql-reference/statements/select/join/){% endif %}, где `ANY` пишется перед типом `JOIN` и работает только для правой стороны.

Запрос

```yql
$t1 = AsList(
    AsStruct("1" AS key, "v111" AS value),
    AsStruct("2" AS key, "v121" AS value),
    AsStruct("2" AS key, "v122" AS value),
    AsStruct("3" AS key, "v131" AS value),
    AsStruct("3" AS key, "v132" AS value));

$t2 = AsList(
    AsStruct("2" AS key, "v221" AS value),
    AsStruct("2" AS key, "v222" AS value),
    AsStruct("3" AS key, "v231" AS value),
    AsStruct("3" AS key, "v232" AS value),
    AsStruct("4" AS key, "v241" AS value));

SELECT
  a.key, a.value, b.value
FROM ANY AS_TABLE($t1) AS a
JOIN ANY AS_TABLE($t2) AS b
ON a.key == b.key;
```

выдаст:

|a.key|a.value|b.value|
| --- | --- | --- |
|"3"|"v131"|"v231"|
|"2"|"v121"|"v221"|

а без `ANY` выдал бы:

|a.key|a.value|b.value|
| --- | --- | --- |
|"3"|"v131"|"v231"|
|"3"|"v131"|"v232"|
|"3"|"v132"|"v231"|
|"3"|"v132"|"v232"|
|"2"|"v121"|"v221"|
|"2"|"v121"|"v222"|
|"2"|"v122"|"v221"|
|"2"|"v122"|"v222"|

{% endif %}
