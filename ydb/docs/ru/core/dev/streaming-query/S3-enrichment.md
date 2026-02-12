# Обогащение данных (S3)

Обогащение данных — добавление к событиям из потока дополнительной информации из справочника. Например, событие содержит только идентификатор, а справочник позволяет добавить к нему название или другие атрибуты.

В [потоковых запросах](../../concepts/streaming-query.md) можно присоединить к потоку данные, хранимые в S3, с помощью конструкции `JOIN`. Поток должен быть слева, справочник из S3 — справа. Справочник полностью загружается в память при запуске запроса. Если данные в S3 изменились, для получения актуальной версии справочника необходимо перезапустить запрос.

Справочник хранится в S3 и подключается через [внешний источник данных](../../concepts/federated_query/s3/external_data_source.md).

## Подготовка источников данных

Создаём два внешних источника: один для YDB (топики), другой для S3 (справочник). Для хранения токена используется [секрет](../../yql/reference/syntax/create-secret.md), источники создаются через [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md).

```sql
-- Секрет с токеном для подключения к YDB
CREATE SECRET `secrets/ydb_token` WITH (value = "<ydb_token>");

-- Источник данных YDB для чтения/записи топиков
CREATE EXTERNAL DATA SOURCE ydb_source WITH (
    SOURCE_TYPE = "Ydb",
    LOCATION = "<location>",
    DATABASE_NAME = "<db_name>",
    AUTH_METHOD = "TOKEN",
    TOKEN_SECRET_NAME = "secrets/ydb_token"
);

-- Источник данных S3 для чтения справочника
CREATE EXTERNAL DATA SOURCE s3_source WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "https://storage.yandexcloud.net/my_public_bucket/",
    AUTH_METHOD = "NONE"
)
```

## Создание потокового запроса

Запрос читает события из входного топика, присоединяет к каждому событию название сервиса из справочника по `ServiceId` и записывает результат в выходной топик. Запрос создаётся с помощью [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

Подробнее о форматах данных (`json_each_row`, `csv_with_names` и др.): [{#T}](streaming-query-formats.md).

```sql
CREATE STREAMING QUERY query_with_join AS
DO BEGIN

-- Чтение событий из входного топика
$topic_data = SELECT
    *
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        ServiceId Uint32 NOT NULL,
        Message String NOT NULL
    )
);

-- Чтение справочника сервисов из S3
$s3_data = SELECT
    *
FROM
    s3_source.`file.csv`
WITH (
    FORMAT = csv_with_names,
    SCHEMA = (
        ServiceId Uint32,
        Name Utf8
    )
);

-- Присоединение справочника к потоку по ServiceId
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    $s3_data AS s
ON
    t.ServiceId = s.ServiceId;

-- Запись результата в выходной топик в формате JSON
INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```

Подробнее о функциях:
- [TableRow](../../yql/reference/builtins/basic#tablerow)
- [Yson::From](../../yql/reference/udf/list/yson#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic#unwrap)
- [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).
