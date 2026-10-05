# STATISTICS

`STATISTICS` объявляет статистику по кортежу столбцов. Оператор [ANALYZE](../analyze.md) собирает её для [стоимостного оптимизатора](../../../../concepts/query_execution/optimizer.md).

Без `STATISTICS` оператор `ANALYZE` уже собирает статистику таблицы и отдельных столбцов. `STATISTICS` нужен, чтобы дополнительно собирать статистику по сочетаниям столбцов.

```yql
CREATE TABLE <table_name> (
    ...
    STATISTICS <statistics_name> ON ( <column_name> [, ...] )
        [WITH ( <statistics_type> [, ...] )],
    PRIMARY KEY ( ... )
);
```

`SHOW CREATE TABLE` показывает эти объявления. Для уже созданной таблицы их можно добавить или удалить с помощью [`ALTER TABLE`](../alter_table/statistics.md).

## Параметры

* `statistics_name` — имя объявления.
* `column_name` — столбцы кортежа. Каждое имя должно соответствовать столбцу таблицы; порядок столбцов важен.
* `statistics_type` — `COUNT_MIN_SKETCH` или `EQ_HEIGHT_HISTOGRAM`. Если `WITH` нет, запрашиваются все поддерживаемые типы.

`COUNT_MIN_SKETCH` имеет смысл объявлять только для двух и более столбцов. Для одного столбца такое объявление не создаёт отдельную статистику. Скетч по столбцу строится, если различных значений меньше 80%.

`EQ_HEIGHT_HISTOGRAM` можно объявлять только для типов, значения которых можно полностью упорядочить. Для `Json`, `Yson` и `JsonDocument` такой порядок не определён, поэтому команда завершится ошибкой.

`ANALYZE` собирает объявленную гистограмму равной высоты, даже если автоматический сбор гистограмм первичного ключа выключен. Подробнее см. [`analyze_collect_primary_key_histogram`](../../../../reference/configuration/statistics_config.md#analyze-collect-primary-key-histogram).

`DROP STATISTICS` удаляет только объявление статистики. Чтобы собрать статистику, выполните [ANALYZE](../analyze.md).

## Пример

```yql
CREATE TABLE orders (
    customer_id Uint64,
    order_date Date,
    status Utf8,
    amount Int64,
    PRIMARY KEY (customer_id, order_date),
    STATISTICS orders_hist ON (customer_id, order_date) WITH (EQ_HEIGHT_HISTOGRAM),
    STATISTICS status_hist ON (status) WITH (EQ_HEIGHT_HISTOGRAM)
);
```
