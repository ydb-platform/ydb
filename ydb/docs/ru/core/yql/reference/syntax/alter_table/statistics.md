# STATISTICS

```yql
ALTER TABLE <table_name> ADD STATISTICS <statistics_name>
    ON ( <column_name> [, ...] )
    [WITH ( <statistics_type> [, ...] )];

ALTER TABLE <table_name> DROP STATISTICS <statistics_name>;
```

`ADD STATISTICS` добавляет объявление многоколоночной статистики к существующей таблице. `DROP STATISTICS` удаляет это объявление. Допустимые типы статистики и правила для столбцов описаны в [{#T}](../create_table/statistics.md). Чтобы собрать объявленную статистику, выполните [ANALYZE](../analyze.md).

В одном операторе `ALTER TABLE` можно указать несколько таких действий вместе с другими изменениями таблицы.

## Примеры

Добавить две гистограммы равной высоты:

```yql
ALTER TABLE orders
    ADD STATISTICS amount_category ON (amount, category) WITH (EQ_HEIGHT_HISTOGRAM),
    ADD STATISTICS amount_time ON (amount, created_at) WITH (EQ_HEIGHT_HISTOGRAM);
```

Удалить объявление:

```yql
ALTER TABLE orders DROP STATISTICS amount_category;
```
