# Потоковое чтение данных из топика

Можно выполнять чтение данных из [топика](../../../../concepts/datamodel/topic.md) обычным `SELECT` без создания [потокового запроса](../../../../concepts/streaming-query.md). Для этого необходимо указать `STREAMING = "TRUE"` в блоке `WITH` и задать ограничение на количество выходных строк через `LIMIT`, иначе запрос не завершится.

{% note warning %}

Этот способ предназначен только для отладки и проверки данных в топике. Для production процессов создавайте потоковые запросы через [CREATE STREAMING QUERY](../create-streaming-query.md).

{% endnote %}

{% note info %}

В примерах:

- `ext_source` — заранее созданный [`external data source`](../../../../concepts/datamodel/external_data_source.md);
- `input_topic` — локальный или внешний топик.

Подробнее — [локальные и внешние топики](../../../../concepts/query_execution/topics.md#local-external-topics).

{% endnote %}

## Пример

```yql
SELECT
    Data
FROM
    ext_source.input_topic -- или локальный топик input_topic
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    ),
    STREAMING = "TRUE"
)
LIMIT 1
```

## См. также

* [{#T}](../../../../concepts/query_execution/topics.md#streaming-read) — описание потокового чтения из топика
* [{#T}](../../../../concepts/streaming-query.md)
* [{#T}](../create-streaming-query.md)
