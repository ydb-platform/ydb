# SELECT STREAM ... FROM

Для работы со стримами RTMR необходимо использовать конструкцию `SELECT STREAM` вместо `SELECT` для обычных таблиц в других системах. `FROM` используется для указания источника данных. Как правило, в качестве аргумента `FROM` выступает имя стрима, который ищется в кластере, заданном оператором [USE](../use.md), но может использоваться и результат другого `SELECT STREAM` (подзапрос). Также стрим можно указать через [именованное выражение](../expressions.md#named-nodes), содержащее строку.

В выражениях между `SELECT STREAM` и `FROM` можно указывать имена столбцов из источника (через запятую). Специальный символ `*` в этой позиции обозначает «все столбцы».

**Примеры:**
``` yql
SELECT STREAM key FROM my_stream;
```

``` yql
SELECT STREAM * FROM
  (SELECT STREAM value FROM my_stream);
```

``` yql
$stream_name = "my_" || "stream";
SELECT STREAM * FROM $stream_name;
```

## WHERE

Фильтрация строк в результате `SELECT STREAM` по условию.

**Примеры:**
``` yql
SELECT STREAM key FROM my_stream
WHERE value > 0;
```

## UNION ALL

Конкатенация результатов нескольких `SELECT STREAM`, их схемы объединяются по следующим правилам:

{% include [union all rules](../_includes/select/union_all_rules.md) %}

**Примеры:**
``` yql
SELECT STREAM x FROM my_stream_1
UNION ALL
SELECT STREAM y FROM my_stream_2
UNION ALL
SELECT STREAM z FROM my_stream_3
```
