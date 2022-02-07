# UPDATE

Изменяет данные в таблице.{% if feature_mapreduce %}  Таблица по имени ищется в базе данных, заданной оператором [USE](../use.md).{% endif %} После ключевого слова `SET` указываются столбцы, значение которых необходимо заменить, и сами новые значения. Список строк задается с помощью условия `WHERE`. Если `WHERE` отсутствует, изменения будут применены ко всем строкам таблицы.

`UPDATE` не может менять значение `PRIMARY_KEY`.

{% note info %}

Изменение состояния таблицы не отслеживается в рамках одной транзакции. Если таблица уже была изменена, для обновления данных в той же транзакции используйте [`UPDATE ON`](#update-on).

{% endnote %}

**Пример**

```sql
UPDATE my_table
SET Value1 = YQL::ToString(Value2 + 1), Value2 = Value2 - 1
WHERE Key1 > 1;
```

## UPDATE ON {#update-on}

Используется для обновления данных, если таблица уже была изменена ранее, в рамках одной транзакции.

**Пример**

```sql
$to_update = (
    SELECT Key, SubKey, "Updated" AS Value FROM my_table
    WHERE Key = 1
);

SELECT * FROM my_table;

UPDATE my_table ON
SELECT * FROM $to_update;
```
