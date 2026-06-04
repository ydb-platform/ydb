# SQL-синтаксис над топиками {#sql-syntax}

Для чтения и записи сообщений в [топики](../datamodel/topic.md) используются привычные SQL-конструкции: [SELECT](../../yql/reference/syntax/select/index.md) для чтения сообщений из топика, [INSERT](../../yql/reference/syntax/insert_into.md) для записи сообщений.

## Чтение из топика

```sql
SELECT
    Data    -- тело сообщения
FROM
    topic_name
LIMIT 10;
```

По умолчанию чтение топика происходит с первого и до последнего смещений, хранящихся в топике.
Если происходит постоянная запись в топик, то запрос будет остановлен при достижении последнего смещения, полученного на момент запуска запроса.

Доступно чтение "новых" сообщений через опцию `WITH (STREAMING = "TRUE")` (см. [Потоковое чтение данных из топика](../../yql/reference/syntax/select/streaming.md)), при этом чтение происходит с текущего времени и до бесконечности (для остановки используйте `LIMIT`).

```sql
SELECT
    Data
FROM
    topic_name
WITH (STREAMING = "TRUE")
LIMIT 10;
```

По умолчанию чтение происходит без использования читателя (для использования читателя см. [Использование читателя](../../yql/reference/syntax/create-streaming-query.md#consumer-usage)).
Для обработки постоянно поступающих данных можно использовать [потоковые запросы](../glossary.md#streaming-query).

Данные из топика можно переложить в таблицу через [UPSERT INTO](../../yql/reference/syntax/upsert_into).

{% cut "Пример запроса" %}

```yql
UPSERT INTO
    table_name
SELECT
    Data            -- можно использовать любые преобразования
FROM
    topic_name;
```

{% endcut %}

При чтении можно указать формат сообщения (см. также примеры в [Форматы данных](../../dev/streaming-query/streaming-query-formats.md)):

{% cut "Пример запроса" %}

```sql
SELECT
    Id,
    Name
FROM
    topic_name
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);
```

{% endcut %}

## Запись в топик

```yql
INSERT INTO
    topic_name
SELECT
    "my_message";       -- тело сообщения
```

Для записи в топик данных из таблицы с несколькими колонками, нужно сформировать JSON-объект из отдельных полей. Функция `TableRow` создаёт структуру из всех колонок таблицы, `Yson::From` преобразует её в Yson, `Yson::SerializeJson` сериализует в JSON-строку, а `ToBytes` конвертирует в тип `String`, который требуется для записи в топик:

```yql
INSERT INTO
    topic_name
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    table_name;
```

{% note warning %}

При записи в топик через YQL/`INSERT INTO` транзакционная запись не поддерживается, поэтому в топике можно видеть частичные результаты запроса.

{% endnote %}

## Чтение и запись в топики другой базы данных

Для работы с топиками, находящимися в другой базе данных, нужно использовать [внешние источники данных](../datamodel/external_data_source.md), см. * [Локальные и внешние топики в потоковых запросах](../../dev/streaming-query/local-and-external-topics.md).

{% cut "Пример запроса" %}

```yql
CREATE EXTERNAL DATA SOURCE ydb_source WITH (
    SOURCE_TYPE = "Ydb",
    LOCATION = "localhost:2136",
    DATABASE_NAME = "/local",
    AUTH_METHOD = "NONE"
);

-- Пример чтения из топика
SELECT
    Data
FROM
    ydb_source.topic_name
LIMIT 10;

-- Пример записи в топик
INSERT INTO
    ydb_source.topic_name
SELECT
    "my_message";
```

{% endcut %}

### Служебные поля

При чтении можно указывать служебные поля:

```sql
SELECT
    Data,                                                   -- тело сообщения
    SystemMetadata('create_time') AS CreateTime,            -- время создания сообщения
    SystemMetadata('write_time') AS WriteTime,              -- время записи сообщения
    SystemMetadata('offset') AS Offset,                     -- смещение сообщения в топике
    SystemMetadata('partition_id') AS Partition,            -- номер партиции, в которой находится сообщение
    SystemMetadata('message_group_id') AS MessageGroupId,   -- идентификатор группы сообщений
    SystemMetadata('seq_no') AS SeqNo                       -- порядковый номер сообщения внутри партиции
FROM
    topic_name
LIMIT 10;
```

Можно указывать фильтры по служебным полям, при этом предикаты будут вычислены до чтения данных из топика, что существенно ограничит объем данных, которые будут считываться.
При вычисления предикатов поддерживаются условия сравнения `=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`, логические условия `AND`, `OR`, поддерживаются служебные поля `partition_id`, `write_time` и `offset` (предикаты по другим служебные полям не будут ограничивать объем считываемых данных).

```sql
SELECT
    Data
FROM
    topic_name
WHERE
    SystemMetadata('partition_id') = 42
        AND SystemMetadata('offset') >= 1000
        AND SystemMetadata('offset') <= 1100
        AND SystemMetadata('write_time') > CurrentUtcTimestamp() - Interval("PT2H");
```

## Ограничения {#limitations}

{% note warning %}

- Запись [пользовательских атрибутов](#message) не поддерживается.
- Транзакционная запись через SQL/YQL `INSERT INTO` не поддерживается.

{% endnote %}

# См. также

* [CREATE TOPIC](../../yql/reference/syntax/create-topic.md)
* [ALTER TOPIC](../../yql/reference/syntax/alter-topic.md)
* [DROP TOPIC](../../yql/reference/syntax/drop-topic.md)
* [Потоковые запросы](../../dev/streaming-query/index.md)
