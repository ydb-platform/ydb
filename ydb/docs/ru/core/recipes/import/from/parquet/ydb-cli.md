# Импорт Parquet в {{ ydb-short-name }} с помощью YDB CLI

Пошаговый рецепт — загрузка данных из **Parquet-файла** в {{ ydb-short-name }} с помощью [YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

Parquet-файл (или набор файлов) должен быть доступен с машины, где запускается CLI.

## Пошаговая инструкция {#steps}

Подробнее о команде: [import file в YDB CLI](../../../../reference/ydb-cli/export-import/import-file.md).

### Шаг 1. Таблица в {{ ydb-short-name }}

Создайте таблицу с типами, совместимыми с колонками Parquet. Учитывайте [маппинг типов Arrow/YQL](../../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

```yql
CREATE TABLE `mydb/events` (
    `event_date` Date,
    `user_id` Int64,
    `value` Double,
    PRIMARY KEY (`event_date`, `user_id`)
);
```

### Шаг 2. Импорт Parquet

```bash
ydb -e grpc://localhost:2136 -d /local import file parquet \
  --path mydb/events \
  /path/to/events.parquet
```

Несколько файлов передайте через пробел в одной команде или укажите каталог — см. [справочник import file](../../../../reference/ydb-cli/export-import/import-file.md).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```
