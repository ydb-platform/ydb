# STATISTICS

`STATISTICS` задаёт многоколоночную статистику для новой таблицы. Чтобы собрать её, выполните [ANALYZE](../analyze.md).

[Стоимостной оптимизатор](../../../../concepts/query_execution/optimizer.md) использует полученные данные при выборе плана запроса. По умолчанию `ANALYZE` собирает сведения о таблице в целом и о каждом столбце по отдельности. `STATISTICS` позволяет также анализировать сочетания значений в нескольких столбцах.

Добавьте `STATISTICS` в описание таблицы:

```yql
CREATE TABLE <table_name> (
    ...
    STATISTICS <statistics_name> ON ( <column_name> [, ...] )
        [WITH ( <statistics_type> [, ...] )],
    PRIMARY KEY ( ... )
);
```

Настройки статистики отображаются в результате [`SHOW CREATE TABLE`](../show_create.md). Для существующей таблицы их можно добавить или удалить с помощью [`ALTER TABLE`](../alter_table/statistics.md).

## Параметры

* `table_name`: имя создаваемой таблицы.
* `statistics_name`: имя статистики. Оно используется в `ALTER TABLE ... DROP STATISTICS`.
* `column_name`: имена существующих столбцов таблицы. Порядок перечисления важен.
* `statistics_type`: `COUNT_MIN_SKETCH` или `EQ_HEIGHT_HISTOGRAM`. Без `WITH` используются все поддерживаемые типы.

## Типы статистики

`COUNT_MIN_SKETCH` приблизительно оценивает, как часто встречается комбинация значений из нескольких столбцов. Указывайте не менее двух столбцов: для одного столбца отдельная статистика не создаётся.

Для отдельных столбцов `ANALYZE` строит скетчи без `STATISTICS`, если оценка числа различных значений меньше 80% числа строк.

`EQ_HEIGHT_HISTOGRAM` описывает распределение значений с помощью гистограммы равной высоты. Она подходит только для типов, значения которых можно полностью упорядочить. Для `Json`, `Yson` и `JsonDocument` такой порядок не определён, поэтому явное указание `WITH (EQ_HEIGHT_HISTOGRAM)` для этих типов завершится ошибкой.

Гистограммы равной высоты по нескольким столбцам помогают оценивать число строк после соединения таблиц. Для этого у обеих таблиц должны быть гистограммы по столбцам соединения в соответствующем порядке и с совпадающими типами.

`ANALYZE` строит такую гистограмму, даже если [автоматическое построение гистограммы первичного ключа](../../../../reference/configuration/statistics_config.md#analyze-collect-primary-key-histogram) выключено.

## Пример

Настроить две гистограммы при создании таблицы и запустить сбор статистики:

```yql
CREATE TABLE orders (
    customer_id Uint64,
    order_date Date,
    status Utf8,
    amount Int64,
    STATISTICS orders_hist ON (customer_id, order_date) WITH (EQ_HEIGHT_HISTOGRAM),
    STATISTICS status_hist ON (status) WITH (EQ_HEIGHT_HISTOGRAM),
    PRIMARY KEY (customer_id, order_date)
);

ANALYZE orders;
```
