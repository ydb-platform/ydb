# Отладочное чтение из топика

При разработке [потоковых запросов](../../concepts/streaming-query.md) бывает полезно быстро посмотреть, какие данные поступают в [топик](../../concepts/datamodel/topic.md), без создания полноценного потокового запроса. Для этого можно выполнить обычный `SELECT` с параметром `STREAMING = TRUE`.

{% note warning %}

Этот способ предназначен только для отладки и проверки данных в топике. Для промышленного использования создавайте потоковые запросы через [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md).

{% endnote %}

{% note info %}

В примерах `ydb_source` — это заранее созданный [внешний источник данных](../../concepts/datamodel/external_data_source.md), а `topic_name` / `input_topic` — топики, доступные через него.

{% endnote %}

## Чтение сырых данных

Простейший способ — прочитать сообщения в формате `raw`, без разбора схемы:

```sql
SELECT
    Data
FROM
    ydb_source.topic_name
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    ),
    STREAMING = TRUE
)
LIMIT 1
```

Параметр `LIMIT` обязателен — без него запрос не завершится, так как будет ожидать новые сообщения бесконечно.

## Чтение с разбором JSON

Если данные в топике хранятся в формате JSON, можно сразу разобрать их по полям:

```sql
SELECT
    *
FROM
    ydb_source.topic_name
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Time String NOT NULL,
        Level String NOT NULL,
        Host String NOT NULL
    ),
    STREAMING = TRUE
)
LIMIT 5
```

## См. также

* [{#T}](../../concepts/streaming-query.md)
* [{#T}](../../dev/streaming-query/streaming-query-formats.md) — поддерживаемые форматы данных
* [{#T}](../../yql/reference/syntax/select/streaming.md) — описание `STREAMING = TRUE` в справочнике YQL
