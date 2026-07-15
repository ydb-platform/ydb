# Типичные шаблоны потоковых запросов

В этом разделе собраны минимальные примеры [потоковых запросов](../../concepts/streaming-query/streaming-query.md) для наиболее распространённых сценариев. Сначала описан базовый шаблон чтения данных из топика, затем — варианты полноценной работы с данными: обработка данных и запись результатов в топик в формате JSON, в топик в виде строки и в таблицу. Каждый пример можно использовать как отправную точку для собственных задач.

<<<<<<< HEAD
=======
В примерах ниже используются [локальные и внешние топики](../../concepts/query_execution/topics.md#local-external-topics). Обозначения:

- `ext_source` — заранее созданный [`external data source`](../../concepts/datamodel/external_data_source.md);
- `input_topic` — топик, откуда читаются данные;
- `output_topic` — топик, куда записываются результаты;
- `output_table` — таблица {{ ydb-short-name }}, куда записываются результаты.


>>>>>>> bed1a355b29 (YDBDOCS-2109 added docs on topic reading/writing (#39856))
## Чтение данных из топика {#topic-read}

Чтение структурированных сообщений выполняется с помощью `SELECT ... FROM ... WITH (FORMAT, SCHEMA)`. Блок `WITH` задаёт формат входных данных и схему — какие поля ожидаются в каждом сообщении и их типы.

<<<<<<< HEAD
{% note info %}

Работа с топиками выполняется через [external data source](../../concepts/datamodel/external_data_source.md).

В примерах:

- `ydb_source` — заранее созданный `external data source`;
- `input_topic` - топик, откуда производится чтение данных;
- `output_topic` - топик, куда производится запись результатов;
- `output_table` — таблица {{ydb-short-name}}, куда производится запись результатов.

{% endnote %}

Следующий фрагмент показывает чтение событий из топика в формате JSON. Он используется внутри [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) в блоке `DO BEGIN ... END DO`:
=======
Следующий фрагмент используется внутри [CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md) в блоке `DO BEGIN ... END DO`:
>>>>>>> bed1a355b29 (YDBDOCS-2109 added docs on topic reading/writing (#39856))

```yql
SELECT
    Id,
    Name
FROM
<<<<<<< HEAD
    ydb_source.input_topic
=======
    topic_name  -- локальный топик; для внешнего: ext_source.topic_name
>>>>>>> bed1a355b29 (YDBDOCS-2109 added docs on topic reading/writing (#39856))
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);
```

Подробнее о форматах: [{#T}](streaming-query-formats.md).

Этот шаблон используется во всех последующих примерах.

## Запись в топик (JSON) {#topic-json}

Запрос читает события из входного топика, формирует JSON-объект из отдельных полей и записывает результат в выходной топик. Функция `AsStruct` создаёт структуру из указанных полей, `Yson::From` преобразует её в Yson, `Yson::SerializeJson` сериализует в JSON-строку, а `ToBytes` конвертирует в тип `String`, который требуется для записи в топик.

```yql
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

Подробнее о функциях:

- [AsStruct](../../yql/reference/builtins/basic#as-container)
- [Yson::From](../../yql/reference/udf/list/yson#ysonfrom)
- [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson)
- [Unwrap](../../yql/reference/builtins/basic#unwrap)
- [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

## Запись в топик (строка) {#topic-utf8}

Запрос читает события из входного топика и записывает в выходной топик одно поле в виде строки. Для записи в топик строк `SELECT` должен возвращать одну колонку типа `String` или `Utf8`.

```yql
CREATE STREAMING QUERY write_utf8_example AS
DO BEGIN

-- ydb_source — external data source для работы с топиками
INSERT INTO ydb_source.output_topic
SELECT
    Name
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

Запись в таблицы в потоковых запросах поддерживается **только в режиме UPSERT**. Операция `INSERT INTO` не поддерживается, так как при повторной обработке событий (гарантия [at-least-once](../../concepts/streaming-query/streaming-query.md#guarantees)) она привела бы к дублированию строк. При `UPSERT`, если строка с таким первичным ключом уже существует, она будет обновлена, иначе будет вставлена новая строка, a `INSERT INTO` завершится с ошибкой.

{% endnote %}

```yql
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

<<<<<<< HEAD
=======
- [Локальные и внешние топики](../../concepts/query_execution/topics.md#local-external-topics)
>>>>>>> bed1a355b29 (YDBDOCS-2109 added docs on topic reading/writing (#39856))
- [{#T}](../../yql/reference/syntax/create-streaming-query.md)
- [{#T}](../../recipes/streaming_queries/topics.md)
