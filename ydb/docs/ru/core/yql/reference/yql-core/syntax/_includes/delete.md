# DELETE FROM

{% include [OLAP_not_allow](../../../../_includes/not_allow_for_olap.md) %}

{% note info %}

Удалить данные из колоночной таблицы можно с помощью [TTL](../../../../concepts/ttl.md), который можно задать при [создании таблицы](../create_table.md) (`CREATE TABLE`) или при [изменении таблицы](../alter_table.md) (`ALTER TABLE`).

{% endnote %}

Удаляет строки из строковой таблицы, подходящие под условия, заданные в `WHERE`.{% if feature_mapreduce %}  Таблица ищется по имени в базе данных, заданной оператором [USE](../use.md).{% endif %}

{% note info %}

Изменение состояния строковой таблицы не отслеживается в рамках одной транзакции. Если таблица уже была изменена, для удаления данных в той же транзакции используйте [`DELETE ON`](#delete-on).

{% endnote %}

**Пример**

```sql
DELETE FROM my_table 
WHERE Key1 == 1 AND Key2 >= "One";
COMMIT;
```

## DELETE FROM ... ON {#delete-on}

Используется для удаления данных, если строковая таблица уже была изменена ранее в рамках одной транзакции.

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
