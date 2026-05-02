# Обогащение данных

Обогащение данных — добавление к событиям из потока дополнительной информации из справочника. Например, событие содержит только идентификатор, а справочник позволяет добавить к нему название или другие атрибуты. В качестве справочника можно использовать данные из [локальной таблицы](#enrichment-local-table) или из [объектного хранилища S3](#enrichment-s3).

В [потоковых запросах](../../concepts/streaming-query.md) справочник подключают с помощью конструкции `JOIN`. Поток должен быть слева, справочник — справа.

{% note warning %}

Справочник полностью загружается в память при запуске запроса. Если данные в справочнике изменились, для получения актуальной версии справочника необходимо перезапустить запрос — удалить его с помощью [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) и создать заново с помощью [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

Возможно обогащение данных из [локальных и внешних топиков](./local-and-external-topics.md).

В примерах ниже:

- `ext_source` — заранее созданный [внешний источник данных](../../concepts/datamodel/external_data_source.md) для топиков в другой базе {{ ydb-short-name }};
- `input_topic` и `output_topic` — топики в текущей или внешней базе {{ ydb-short-name }}


## Потоковые запросы для обогащения данных

Запросы в примерах ниже читают события из входного топика, присоединяют к каждому событию название сервиса из справочника по `ServiceId` и записывают результат в выходной топик.

Подробнее об использованных в запросах функциях:

- [TableRow](../../yql/reference/builtins/basic.md#tablerow)
- [Yson::From](../../yql/reference/udf/list/yson.md#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson.md#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic.md#unwrap)
- [ToBytes](../../yql/reference/builtins/basic.md#to-from-bytes).

### Обогащение из локальной таблицы {#enrichment-local-table}

В данном примере справочник хранится в [таблице](../../concepts/datamodel/table.md) `services_dict` в текущей базе данных.

Создайте [потоковый запрос](../../concepts/streaming-query.md), выполняющий обогащение:

```yql
CREATE STREAMING QUERY query_with_table_join AS
DO BEGIN

-- Чтение событий из входного топика
$topic_data = SELECT
    *
FROM
    ext_source.input_topic -- или локальный топик input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        ServiceId Uint32 NOT NULL,
        Message String NOT NULL
    )
);

-- Присоединение справочника к потоку по ServiceId
$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    services_dict AS s
ON
    t.ServiceId = s.ServiceId;

-- Запись в выходной топик (JSON)
INSERT INTO
    output_topic -- или внешний топик ext_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```

### Обогащение из S3 {#enrichment-s3}

Справочник хранится в S3 и подключается через [внешний источник данных](../../concepts/query_execution/federated_query/s3/external_data_source.md).

Создайте дополнительный [внешний источник данных](../../yql/reference/syntax/create-external-data-source.md) для чтения справочника из S3:

```yql
-- Источник данных S3 для чтения справочника
CREATE EXTERNAL DATA SOURCE s3_source WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "<s3_endpoint>",
    AUTH_METHOD = "NONE"
);
```

Где:

- `<s3_endpoint>` — URL S3-хранилища, например `https://storage.yandexcloud.net/<bucket>/` для Yandex Cloud.

Создайте [потоковый запрос](../../concepts/streaming-query.md), выполняющий обогащение:

```yql
CREATE STREAMING QUERY query_with_join AS
DO BEGIN

-- Чтение событий из входного топика
$topic_data = SELECT
    *
FROM
    input_topic -- или внешний топик ext_source.input_topic
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
    ext_source.output_topic -- или локальный топик output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```

Подробнее о форматах данных (`json_each_row`, `csv_with_names` и др.): [{#T}](streaming-query-formats.md).
