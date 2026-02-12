# Типичные шаблоны потоковых запросов

Минимальные примеры для быстрого старта.

## Чтение JSON из топика {#topic-read-json}

Запрос читает события из топика в формате JSON. Блок `WITH` указывает формат данных и схему — какие поля ожидаются в каждом сообщении и их типы. Каждое сообщение в топике должно содержать один JSON-объект.

{% note info %}

Работа с топиками выполняется через [external data source](../../concepts/datamodel/external_data_source.md). В примерах:
- `ydb_source` — заранее созданный `external data source`;
- `input_topic` - топик, откуда производится чтение данных;
- `output_topic` - топик, куда производится запись результатов;
- `ydb_table` — таблица {{ydb-short-name}}, куда производится запись результатов.

{% endnote %}

```sql
CREATE STREAMING QUERY read_json_example AS
DO BEGIN

-- ydb_source — external data source для работы с топиками
UPSERT INTO ydb_table
SELECT
    *
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Каждое сообщение — один JSON-объект
    SCHEMA = (               -- Ожидаемые поля и их типы
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```

Подробнее о форматах: [{#T}](streaming-query-formats.md).

## Запись в топик (JSON) {#topic-json}

Запрос читает события из входного топика, формирует JSON-объект из отдельных полей и записывает результат в выходной топик. Функция `AsStruct` создаёт структуру из указанных полей, `Yson::From` преобразует её в Yson, `Yson::SerializeJson` сериализует в JSON-строку, а `ToBytes` конвертирует в тип `String`, который требуется для записи в топик.

```sql
CREATE STREAMING QUERY write_json_example AS
DO BEGIN

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
);

END DO
```

Подробнее о функциях: [AsStruct](../../yql/reference/builtins/basic#asstruct), [Yson::From](../../yql/reference/udf/list/yson#ysonfrom), [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson).

## Запись в топик (строка) {#topic-utf8}

Запрос читает события из входного топика и записывает в выходной топик одно поле в виде строки. Для записи в топик строк `SELECT` должен возвращать одну колонку типа `String` или `Utf8`.

```sql
CREATE STREAMING QUERY write_utf8_example AS
DO BEGIN

-- ydb_source — external data source для работы с топиками
INSERT INTO ydb_source.output_topic
SELECT
    CAST(Name AS Utf8)
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_each_row,  -- Формат входных данных
    SCHEMA = (               -- Схема входных данных
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);

END DO
```

Подробнее о форматах записи: [{#T}](streaming-query-formats.md#write_formats).

## Запись в таблицу {#table-write}

Запрос читает события из топика и записывает их в таблицу `output_table`. Таблица должна быть создана заранее со схемой, соответствующей выбираемым колонкам.

{% note warning %}

Запись в таблицы в потоковых запросах поддерживается **только в режиме UPSERT**. Операция `INSERT INTO` не поддерживается, так как при повторной обработке событий (гарантия [at-least-once](../../concepts/streaming-query.md#guarantees)) она привела бы к дублированию строк. При UPSERT, если строка с таким первичным ключом уже существует, она будет обновлена, иначе будет вставлена новая строка.

{% endnote %}

```sql
CREATE STREAMING QUERY write_table_example AS
DO BEGIN

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
);

END DO
```

Подробнее: [{#T}](table-writing.md).

## См. также

- [{#T}](../../yql/reference/syntax/create-streaming-query.md)
- [{#T}](../../recipes/streaming_queries/topics.md)
