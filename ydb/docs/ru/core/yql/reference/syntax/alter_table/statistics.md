# STATISTICS

`ADD STATISTICS` задаёт многоколоночную статистику для существующей таблицы. Чтобы собрать её, выполните [ANALYZE](../analyze.md).

`DROP STATISTICS` удаляет многоколоночную статистику с указанным именем.

Типы статистики и требования к столбцам описаны в [CREATE TABLE ... STATISTICS](../create_table/statistics.md). В одном операторе `ALTER TABLE` можно указать несколько таких действий вместе с другими изменениями таблицы.

## Примеры

Добавить две гистограммы равной высоты и запустить `ANALYZE`:

```yql
ALTER TABLE orders
    ADD STATISTICS amount_category ON (amount, category) WITH (EQ_HEIGHT_HISTOGRAM),
    ADD STATISTICS amount_time ON (amount, created_at) WITH (EQ_HEIGHT_HISTOGRAM);

ANALYZE orders;
```

Удалить многоколоночную статистику `amount_category`:

```yql
ALTER TABLE orders DROP STATISTICS amount_category;
```
