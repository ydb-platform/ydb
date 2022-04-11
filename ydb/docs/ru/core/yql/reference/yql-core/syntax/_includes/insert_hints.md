{% if feature_insert_with_truncate %}

Запись может выполняться с одним или несколькими модификаторами. Модификатор указывается после ключевого слова `WITH` после имени таблицы: `INSERT INTO ... WITH SOME_HINT`.
Если у модификатора есть значение, то оно указывается после знака `=`: `INSERT INTO ... WITH SOME_HINT=value`.
При необходимости указать несколько модификаторов они должны заключаться в круглые скобки: `INSERT INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

Чтобы перед записью очистить таблицу от имевшихся данных достаточно добавить модификатор: `INSERT INTO ... WITH TRUNCATE`.

**Примеры:**

``` yql
INSERT INTO my_table WITH TRUNCATE
SELECT key FROM my_table_source;
```

{% endif %}
