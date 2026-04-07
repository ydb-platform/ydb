
# DELETE FROM

{% include [column-and-row-tables-in-read-only-tx](../_includes/limitation-column-row-in-read-only-tx-warn.md) %}

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

## DELETE FROM ... RETURNING {delete-from-returning}

Используется для удаления строк и одновременного возврата значений из них. Это позволяет получить информацию об удаляемых записях за один запрос, избавляя от необходимости выполнять предварительный SELECT.

### Примеры

* Возврат всех значений удаленных строк

```yql
DELETE FROM orders
WHERE status = 'cancelled'
RETURNING *;
```

* Возврат конкретных столбцов

```yql
DELETE FROM orders
WHERE status = 'cancelled'
RETURNING order_id, order_date;
```

{% if feature_batch_operations %}

## См. также

* [BATCH DELETE](batch-delete.md)

{% endif %}