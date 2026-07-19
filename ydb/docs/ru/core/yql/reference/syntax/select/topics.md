# Чтение данных из топика

Можно выполнять чтение данных из [топика](../../../../concepts/datamodel/topic.md) обычным [`SELECT`] без создания [потокового запроса](../../../../concepts/streaming-query/streaming-query.md).

{% note warning %}

Этот способ предназначен только для отладки и проверки данных в топике. Для production процессов создавайте потоковые запросы через [CREATE STREAMING QUERY](../create-streaming-query.md).

{% endnote %}

{% note info %}

В примерах:

- `ext_source` — заранее созданный [`external data source`](../../../../concepts/datamodel/external_data_source.md);
- `input_topic` — локальный или внешний топик.

Подробнее — [локальные и внешние топики](../../../../concepts/query_execution/topics.md#local-external-topics).

{% endnote %}

## Примеры

### Чтение текущих данных

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
)
```

### Чтение с ожиданием новых данных

```yql
SELECT
    Data
FROM
    input_topic -- или внешний топик ext_source.input_topic
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    ),
    STREAMING = "TRUE"
)
LIMIT 1
```

Подробнее — [чтение из топика](../../../../concepts/query_execution/topics.md#topic-read).


## См. также

* [{#T}](../../../../concepts/query_execution/topics.md#topic-read)
* [{#T}](../../../../concepts/streaming-query/streaming-query.md)
* [{#T}](../create-streaming-query.md)
