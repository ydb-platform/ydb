# statistics_config

Секция `statistics_config` настраивает сбор статистики столбцов для [стоимостного оптимизатора](../../concepts/query_execution/optimizer.md#statistics).

## Параметры конфигурации

| Параметр | Тип | По умолчанию | Описание |
|:---------|:----|:-------------|:---------|
| `enable_background_column_stats_collection` | bool | `false` | Фоновый сбор статистики включён |

## Фоновый сбор {#enable-background-column-stats-collection}

`enable_background_column_stats_collection: true` включает автоматический запуск `ANALYZE` для пользовательских колоночных таблиц. По умолчанию фоновый сбор выключен.

Повторный сбор планируется через 24 часа после начала предыдущего `ANALYZE`. Этот интервал фиксирован и не настраивается. Если уже выполняется сбор статистики для другой таблицы, новый запуск ждёт его завершения.

## Пример конфигурации

Включить фоновый сбор статистики:

```yaml
statistics_config:
  enable_background_column_stats_collection: true
```

## См. также

- [{#T}](../../concepts/query_execution/optimizer.md#statistics)
- [{#T}](../../yql/reference/syntax/analyze.md)
- [CREATE TABLE ... STATISTICS](../../yql/reference/syntax/create_table/statistics.md)
- [ALTER TABLE ... STATISTICS](../../yql/reference/syntax/alter_table/statistics.md)
