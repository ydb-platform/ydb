## AS {#as}

Может использоваться в следующих сценариях:

* Присвоение короткого имени (алиаса) столбцам или таблицам в рамках запроса.
* Указание именованных аргументов при вызове функций.
* При явном приведении типов данных для указания целевого типа, см. [CAST](#cast).

{% if select_command != "SELECT STREAM" %}
**Примеры:**

``` yql
SELECT key AS k FROM my_table;
```

``` yql
SELECT t.key FROM my_table AS t;
```

``` yql
SELECT
    MyFunction(key, 123 AS my_optional_arg)
FROM my_table;
```
{% else %}
**Примеры:**
``` yql
SELECT STREAM key AS k FROM my_stream;
```

``` yql
SELECT STREAM s.key FROM my_stream AS s;
```

``` yql
SELECT STREAM
    MyFunction(key, 123 AS my_optional_arg)
FROM my_stream;
```
{% endif %}