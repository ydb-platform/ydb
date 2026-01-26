# Обогащение данных (S3)

В потоковых запросах возможно присоединение к потоку данных из S3 с помощью конструкции `JOIN`. При этом поток обязательно должен находиться в левой части джойна. Механизм имеет ограничения, т.к. правая часть джойна полностью помещается в оперативную память процесса.

Обогащение данных (S3) возможно через [внешние источники данных](../../concepts/federated_query/s3/external_data_source.md).

Подготовка источников данных:

```yql
CREATE SECRET `secrets/ydb_token` WITH (value = "<ydb_token>");

CREATE EXTERNAL DATA SOURCE ydb_source WITH (
    SOURCE_TYPE = "Ydb",
    LOCATION = "<location>",
    DATABASE_NAME = "<db_name>",
    AUTH_METHOD = "TOKEN",
    TOKEN_SECRET_NAME = "secrets/ydb_token"
);

CREATE EXTERNAL DATA SOURCE s3_source WITH (
    SOURCE_TYPE = "ObjectStorage",
    LOCATION = "https://storage.yandexcloud.net/my_public_bucket/",
    AUTH_METHOD = "NONE"
)
```

Создание потокового запроса:

```yql
CREATE STREAMING QUERY query_with_join AS
DO BEGIN

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

$joined_data = SELECT
    s.Name AS Name,
    t.*
FROM
    $topic_data AS t
LEFT JOIN
    $s3_data AS s
ON
    t.ServiceId = s.ServiceId;

INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $joined_data

END DO
```