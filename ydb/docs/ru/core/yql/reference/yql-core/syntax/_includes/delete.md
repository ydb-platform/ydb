# DELETE FROM

Удаляет строки из таблицы, заданные с помощью условия `WHERE`.{% if feature_mapreduce %}  Таблица по имени ищется в базе данных, заданной оператором [USE](../use.md).{% endif %}

{% note info %}

Изменение состояния таблицы не отслеживается в рамках одной транзакции. Если таблица уже была изменена, для удаления данных в той же транзакции используйте [`DELETE ON`](#delete-on).

{% endnote %}

**Пример**

```sql
DELETE FROM my_table 
WHERE Key1 == 1 AND Key2 >= "One";
COMMIT;
```

## DELETE FROM ... ON {#delete-on}

Используется для удаления данных, если таблица уже была изменена ранее в рамках одной транзакции.

**Пример**

```sql
$to_delete = (
    SELECT Key, SubKey FROM my_table WHERE Value = "ToDelete"
);

SELECT * FROM my_table;

DELETE FROM my_table ON 
SELECT * FROM $to_delete;
COMMIT;
```
