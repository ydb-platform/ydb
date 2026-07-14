# Импорт Parquet из S3 в {{ ydb-short-name }} с помощью федеративных запросов

Пошаговый рецепт — загрузка данных из **Parquet-файлов в S3-совместимом объектном хранилище** в {{ ydb-short-name }} с помощью [федеративных запросов](../../../../concepts/query_execution/federated_query/import_and_export.md).

Этот сценарий отличается от [локального импорта Parquet](ydb-cli.md) через YDB CLI: {{ ydb-short-name }} читает файлы **напрямую из бакета** по SQL, без копирования файлов на машину с CLI. Для больших объёмов в **колоночные** таблицы предпочтительны операции `UPSERT` или `REPLACE` — они выполняют параллельную запись.

{% note info %}

При импорте Parquet из S3 учитывайте [маппинг типов YQL и Apache Arrow](../../../../concepts/query_execution/federated_query/s3/arrow_types_mapping.md).

{% endnote %}

## Подготовка {#prerequisites}

{% include notitle [YDB CLI](../../_includes/ydb-cli-prerequisites.md) %}

### Объектное хранилище (S3)

| Требование | Рекомендация |
| --- | --- |
| Бакет | S3-совместимое хранилище с загруженными Parquet-файлами |
| Доступ | Статические ключи для приватного бакета или публичный бакет |
| Сеть | Узлы кластера {{ ydb-short-name }} должны иметь доступ к endpoint хранилища (обычно порт 443) |

Подробнее о подключении: [внешний источник данных S3](../../../../concepts/query_execution/federated_query/s3/external_data_source.md), [внешняя таблица](../../../../concepts/query_execution/federated_query/s3/external_table.md).

## Пошаговая инструкция {#steps}

Ниже — импорт из приватного бакета. Для публичного бакета используйте `AUTH_METHOD="NONE"` без секретов (см. [документацию](../../../../concepts/query_execution/federated_query/s3/external_data_source.md)).

### Шаг 1. Секреты для доступа к S3

```yql
CREATE SECRET aws_access_id WITH (value = "<access_key_id>");
CREATE SECRET aws_access_key WITH (value = "<secret_access_key>");
```

### Шаг 2. Внешний источник данных

```yql
CREATE EXTERNAL DATA SOURCE `external/s3_backup` WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "https://storage.yandexcloud.net/my-bucket/",
    AUTH_METHOD = "AWS",
    AWS_ACCESS_KEY_ID_SECRET_PATH = "aws_access_id",
    AWS_SECRET_ACCESS_KEY_SECRET_PATH = "aws_access_key",
    AWS_REGION = "ru-central1"
);
```

Замените endpoint, имя бакета и регион на свои. Для других S3-совместимых хранилищ укажите соответствующий `LOCATION`.

### Шаг 3. Внешняя таблица на Parquet в бакете

Схема внешней таблицы должна соответствовать колонкам Parquet-файлов:

```yql
CREATE EXTERNAL TABLE `external/events_parquet` (
    `event_date` Date NOT NULL,
    `user_id` Int64 NOT NULL,
    `value` Double NOT NULL
) WITH (
    DATA_SOURCE = "external/s3_backup",
    LOCATION = "/exports/events/",
    FORMAT = "parquet"
);
```

`LOCATION` — путь к каталогу с Parquet-файлами внутри бакета. Поддерживаются маски пути — см. [формат путей](../../../../concepts/query_execution/federated_query/s3/external_data_source.md#path_format).

Проверьте чтение:

```yql
SELECT COUNT(*) FROM `external/events_parquet`;
```

### Шаг 4. Целевая таблица в {{ ydb-short-name }}

```yql
CREATE TABLE `mydb/events` (
    `event_date` Date,
    `user_id` Int64,
    `value` Double,
    PRIMARY KEY (`event_date`, `user_id`)
);
```

### Шаг 5. Импорт данных

```yql
UPSERT INTO `mydb/events`
SELECT * FROM `external/events_parquet`;
```

Для колоночных таблиц `UPSERT` и `REPLACE` выполняют **параллельную** запись; `INSERT` тоже поддерживается, но для идempotent-загрузки предпочтительнее `UPSERT`. Подробнее: [импорт через федеративные запросы](../../../../concepts/query_execution/federated_query/import_and_export.md).

Расширенный пример (TPC-H, экспорт/импорт колоночных таблиц): [импорт и экспорт колоночных таблиц](../../../import-export-column-tables.md#objstorage).

## Проверка результата {#verify}

```bash
ydb -e grpc://localhost:2136 -d /local sql -s "SELECT COUNT(*) FROM mydb/events"
```

Сравните с числом строк во внешней таблице:

```yql
SELECT COUNT(*) FROM `external/events_parquet`;
```
