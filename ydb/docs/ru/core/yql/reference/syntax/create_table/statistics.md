# STATISTICS

`STATISTICS` задаёт многоколоночную статистику для новой таблицы. Чтобы собрать её, выполните [ANALYZE](../analyze.md).

[Стоимостной оптимизатор](../../../../concepts/query_execution/optimizer.md) использует полученные данные при выборе плана запроса. По умолчанию `ANALYZE` собирает сведения о таблице в целом и о каждом столбце по отдельности. `STATISTICS` позволяет также анализировать сочетания значений в нескольких столбцах.

Добавьте `STATISTICS` в описание таблицы:

```yql
CREATE TABLE <table_name> (
    ...
    STATISTICS <statistics_name> ON ( <column_name> [, ...] )
        WITH ( <statistics_type> ),
    PRIMARY KEY ( ... )
);
```

Настройки многоколоночной статистики отображаются в результате [`SHOW CREATE TABLE`](../show_create.md). Для существующей таблицы их можно добавить или удалить с помощью [`ALTER TABLE`](../alter_table/statistics.md).

## Параметры

* `table_name`: имя создаваемой таблицы.
* `statistics_name`: имя многоколоночной статистики.
* `column_name`: имена существующих столбцов таблицы. Порядок перечисления важен.
* `statistics_type`: `COUNT_MIN_SKETCH`. Указывайте `WITH (COUNT_MIN_SKETCH)` явно: в этой версии без `WITH` многоколоночная статистика не собирается.

## Типы статистики

`COUNT_MIN_SKETCH` приблизительно оценивает, как часто встречается комбинация значений из нескольких столбцов. Указывайте не менее двух столбцов: для одного столбца отдельная статистика не создаётся.

Для отдельных столбцов `ANALYZE` строит скетчи без `STATISTICS`, если оценка числа различных значений меньше 80% числа строк.

## Пример

Настроить многоколоночный скетч при создании таблицы и запустить сбор статистики:

```yql
CREATE TABLE orders (
    customer_id Uint64,
    order_date Date,
    status Utf8,
    amount Int64,
    STATISTICS orders_cms ON (customer_id, order_date) WITH (COUNT_MIN_SKETCH),
    PRIMARY KEY (customer_id, order_date)
);

ANALYZE orders;
```
