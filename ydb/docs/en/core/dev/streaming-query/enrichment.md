# Data enrichment

Data enrichment is adding additional information from a reference to events from a stream. For example, an event contains only an identifier, and the reference allows adding a name or other attributes to it. As a reference, you can use data from a [local table](#enrichment-local-table) or from an [S3 object storage](#enrichment-s3).

In [streaming queries](../../concepts/streaming-query.md), the reference is connected using the `JOIN` construct. The stream must be on the left, the reference on the right.

{% note warning %}

The reference is fully loaded into memory when the query starts. If the data in the reference has changed, to get the current version of the reference, you need to restart the query — delete it using [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md) and recreate it using [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

## Preparing a data source for working with topics

Create an external data source for working with topics. A [secret](../../yql/reference/syntax/create-secret.md) is used to store the token, and the source is created using [CREATE EXTERNAL DATA SOURCE](../../yql/reference/syntax/create-external-data-source.md).


```yql
-- Секрет с токеном для подключения к YDB
CREATE SECRET `secrets/ydb_token` WITH (value = "<ydb_token>");

-- Источник данных YDB для чтения/записи топиков
CREATE EXTERNAL DATA SOURCE ydb_source WITH (
    SOURCE_TYPE = "Ydb",
    LOCATION = "<ydb_endpoint>",
    DATABASE_NAME = "<db_name>",
    AUTH_METHOD = "TOKEN",
    TOKEN_SECRET_PATH = "secrets/ydb_token"
);
```


Where:

- `<ydb_endpoint>` — endpoint of {{ ydb-short-name }}, for example `grpcs://<ydb_host>:2135`.
- `<db_name>` — path to the database {{ ydb-short-name }}, for example `/Root/database`.

## Streaming queries for data enrichment

The queries in the examples below read events from the input topic, attach the service name from the reference by `ServiceId` to each event, and write the result to the output topic.

More details about the functions used in the queries:

- [TableRow](../../yql/reference/builtins/basic.md#tablerow)
- [Yson::From](../../yql/reference/udf/list/yson.md#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson.md#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic.md#unwrap)
- [ToBytes](../../yql/reference/builtins/basic.md#to-from-bytes).

### Enrichment from a local table {#enrichment-local-table}

In this example, the reference is stored in the [table](../../concepts/datamodel/table.md) `services_dict` in the current database.

Create a [streaming query](../../concepts/streaming-query.md) that performs enrichment:


```yql
CREATE STREAMING QUERY query_with_table_join AS
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
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data;

END DO
```


### Enrichment from S3 {#enrichment-s3}

The reference is stored in S3 and connected via an [external data source](../../concepts/query_execution/federated_query/s3/external_data_source.md).

Create an additional [external data source](../../yql/reference/syntax/create-external-data-source.md) to read the reference from S3:


```yql
-- Источник данных S3 для чтения справочника
CREATE EXTERNAL DATA SOURCE s3_source WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "<s3_endpoint>",
    AUTH_METHOD = "NONE"
);
```


Where:

- `<s3_endpoint>` — URL of the S3 storage, for example `https://storage.yandexcloud.net/<bucket>/` for Yandex Cloud.

Create a [streaming query](../../concepts/streaming-query.md) that performs enrichment:


```yql
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


More details about data formats (`json_each_row`, `csv_with_names`, etc.): [{#T}](streaming-query-formats.md).
