# Потоковое чтение данных из топика

Можно выполнять чтение данных из [топика](../../../../concepts/datamodel/topic.md) обычным `SELECT` без создания [потокового запроса](../../../../concepts/streaming-query.md). Для этого необходимо указать `STREAMING = TRUE` в блоке `WITH` и задать ограничение на количество выходных строк через `LIMIT`, иначе запрос не завершится.

{% note warning %}

Этот способ предназначен только для отладки и проверки данных в топике. Для production процессов создавайте потоковые запросы через [CREATE STREAMING QUERY](../create-streaming-query.md).

{% endnote %}

{% note info %}

В примерах `ydb_source` — это заранее созданный [внешний источник данных](../../../../concepts/datamodel/external_data_source.md), а `topic_name` — топик, доступный через него.

{% endnote %}

## Пример

```yql
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

## См. также

* [{#T}](../../../../recipes/streaming_queries/debug-read.md) — рецепт с дополнительными примерами
* [{#T}](../../../../concepts/streaming-query.md)
* [{#T}](../create-streaming-query.md)
