
# DELETE FROM

{% note warning %}

{% include [column-and-row-tables-in-read-only-tx](../../../concepts/_includes/limitation-column-row-in-read-only-tx.md) %}

{% endnote %}

Удаляет строки из таблицы, подходящие под условия, заданные в `WHERE`.{% if feature_mapreduce %} Таблица ищется по имени в базе данных, заданной оператором [USE](use.md).{% endif %}

## Пример

```yql
DELETE FROM my_table
WHERE Key1 == 1 AND Key2 >= "One";
```

## DELETE FROM ... ON {#delete-on}

Используется для удаления данных на основе результатов подзапроса. Набор колонок, возвращаемых подзапросом, должен быть подмножеством колонок обновляемой таблицы, и в составе возвращаемых колонок обязательно должны присутствовать все колонки первичного ключа таблицы. Типы данных возвращаемых подзапросом колонок должны совпадать с типами данных соответствующих колонок таблицы.

Для поиска удаляемых из таблицы записей используется значение первичного ключа. Присутствие других (неключевых) колонок таблицы в составе выходных колонок подзапроса не влияет на результаты операции удаления.


### Пример

```yql
$to_delete = (
    SELECT Key, SubKey FROM my_table WHERE Value = "ToDelete" LIMIT 100
);

DELETE FROM my_table ON
SELECT * FROM $to_delete;
```

{% if feature_batch_operations %}

## См. также

* [BATCH DELETE](batch-delete.md)

{% endif %}
