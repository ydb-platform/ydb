# UPSERT INTO

{% include [column-and-row-tables-in-read-only-tx](../_includes/limitation-column-row-in-read-only-tx-warn.md) %}

UPSERT (расшифровывается как UPDATE or INSERT) обновляет или добавляет множество строк в таблице на основании сравнения по первичному ключу. Отсутствующие строки добавляются. В присутствующих строках обновляются значения заданных столбцов, значения остальных столбцов остаются неизменными.

{% if feature_mapreduce %}

Таблица по имени ищется в базе данных, заданной оператором [USE](use.md).

{% endif %}

{% if feature_replace %}

`UPSERT` и [`REPLACE`](replace_into.md) являются операциями модификации данных, которые не требует их предварительного чтения, за счет чего работают быстрее и дешевле других операций.

{% else %}

`UPSERT` является единственной операцией модификации данных, которая не требует их предварительного чтения, за счет чего работает быстрее и дешевле других операций.

{% endif %}

Сопоставление столбцов при использовании `UPSERT INTO ... SELECT` производится по именам. Используйте `AS` для получения колонки с нужным именем в `SELECT`.

## Примеры

```yql
UPSERT INTO my_table
SELECT pk_column, data_column1, col24 as data_column3 FROM other_table
```

```yql
UPSERT INTO my_table ( pk_column1, pk_column2, data_column2, data_column5 )
VALUES ( 1, 10, 'Some text', Date('2021-10-07')),
       ( 2, 10, 'Some text', Date('2021-10-08'))
```

## UPSERT INTO ... RETURNING {upsert-into-returning}

Используется для вставки или обновления строк и одновременного возврата значений из них. Это позволяет получить информацию об измененной записи за один запрос, избавляя от необходимости выполнять дополнительный SELECT.

### Примеры

* Возврат всех значений измененной строки

```yql
UPSERT INTO orders (order_id, status, amount)
VALUES (1001, 'shipped', 500)
RETURNING *;
```

* Возврат конкретных столбцов

```yql
UPSERT INTO users (user_id, name, email)
VALUES (42, 'John Doe', 'john@example.com')
RETURNING user_id, email;
```