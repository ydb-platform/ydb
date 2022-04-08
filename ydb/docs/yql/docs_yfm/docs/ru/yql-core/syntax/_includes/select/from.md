## FROM {#from}

Источник данных для `SELECT`. В качестве аргумента может принимать имя таблицы, результат другого `SELECT` или [именованное выражение](../../expressions.md#named-nodes). Между `SELECT` и `FROM` через запятую указываются имена столбцов из источника или `*` для выбора всех столбцов.

{% if feature_mapreduce %}Таблица по имени ищется в базе данных, заданной оператором [USE](../../use.md).{% endif %}

**Примеры**

``` yql
SELECT key FROM my_table;
```

``` yql
SELECT * FROM
  (SELECT value FROM my_table);
```

``` yql
$table_name = "my_table";
SELECT * FROM $table_name;
```
