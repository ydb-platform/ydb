# Типичные шаблоны потоковых запросов

Минимальные примеры для быстрого старта.

## Чтение JSON из топика {#topic-read-json}

Запрос читает события из топика в формате JSON. Блок `WITH` указывает формат данных и схему — какие поля ожидаются в каждом сообщении и их типы. Каждое сообщение в топике должно содержать один JSON-объект.

{% note info %}

Работа с топиками выполняется через [external data source](../../concepts/datamodel/external_data_source.md). В примерах `ydb_source` — это заранее созданный external data source, а `input_topic` и `output_topic` — топики, доступные через него.

{% endnote %}

```sql
SELECT
    *
FROM
    -- ydb_source — external data source для работы с топиками
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Каждое сообщение — один JSON-объект
    SCHEMA = (               -- Ожидаемые поля и их типы
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
)
```

Подробнее о форматах: [{#T}](streaming-query-formats.md).

## Запись в топик (JSON) {#topic-json}

Запрос читает события из входного топика, формирует JSON-объект из отдельных полей и записывает результат в выходной топик. Функция `AsStruct` создаёт структуру из указанных полей, `Yson::From` преобразует её в Yson, `Yson::SerializeJson` сериализует в JSON-строку, а `ToBytes` конвертирует в тип `String`, который требуется для записи в топик.

```sql
-- ydb_source — external data source для работы с топиками
INSERT INTO ydb_source.output_topic
SELECT
    -- Формирование JSON из отдельных полей
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(
        AsStruct(Id AS id, Name AS name)
    ))))
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Формат входных данных
    SCHEMA = (               -- Схема входных данных
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
)
```

Подробнее о функциях: [AsStruct](../../yql/reference/builtins/basic#asstruct), [Yson::From](../../yql/reference/udf/list/yson#ysonfrom), [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson).

## Запись в таблицу {#table-write}

Запрос читает события из топика и записывает их в таблицу `output_table`. Таблица должна быть создана заранее со схемой, соответствующей выбираемым колонкам.

{% note warning %}

Запись в таблицы в потоковых запросах поддерживается **только в режиме UPSERT**. Операция `INSERT INTO` для таблиц не поддерживается. При UPSERT, если строка с таким первичным ключом уже существует, она будет обновлена, иначе будет вставлена новая строка.

{% endnote %}

```sql
-- Запись в таблицу (только UPSERT, INSERT не поддерживается)
UPSERT INTO output_table
SELECT
    Id,
    Name
FROM
    -- ydb_source — external data source для работы с топиками
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Формат входных данных
    SCHEMA = (               -- Схема входных данных
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
)
```

Подробнее: [{#T}](table-writing.md).

## См. также

- [{#T}](../../yql/reference/syntax/create-streaming-query.md)
- [{#T}](../../recipes/streaming_queries/topics.md)
