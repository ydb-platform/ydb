# YQL-запросы к топикам {#yql-syntax}

Для чтения и записи сообщений в [топики](../datamodel/topic.md) используются привычные YQL-конструкции: [SELECT](../../yql/reference/syntax/select/index.md) для чтения и [INSERT](../../yql/reference/syntax/insert_into.md) для записи.

## Локальные и внешние топики {#local-external-topics}

YQL-запросы к топикам работают одинаково независимо от того, находится топик в текущей базе или в другой базе {{ ydb-short-name }}. Источником и приёмником сообщений может быть как топик **в той же базе данных**, в которой выполняется запрос, так и топик **в другой базе**.


### Локальные топики {#local-topics}

**Локальные топики** — топики, созданные в **той же базе** {{ ydb-short-name }}, что и выполняемый запрос.

В тексте запроса к ним обращаются **по короткому имени** — так же, как к таблице в текущей базе:

```yql
SELECT * FROM input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```

```yql
INSERT INTO output_topic SELECT ...;
```

### Внешние топики {#external-topics}

**Внешние топики** — топики, расположенные **в другой базе** {{ ydb-short-name }}.

Доступ к ним выполняется только через заранее созданный [внешний источник данных](../datamodel/external_data_source.md) с типом источника YDB.

После создания источника, например с именем `ext_source`, обращение к топику `input_topic` во внешней базе записывается так:

```yql
SELECT * FROM ext_source.input_topic WITH (FORMAT = json_each_row, SCHEMA = (...));
```

Имя `ext_source` в документации **условное** — в вашей базе источник может называться иначе; важно, чтобы оно совпадало в `CREATE EXTERNAL DATA SOURCE` и в префиксе перед именем топика.

## Чтение из топика {#topic-read}

Чтение из топика можно выполнять в [табличном](#table-read) и [потоковом](#streaming-read) режимах (не путать с [потоковыми запросами](../streaming-query.md)).

### Табличное чтение {#table-read}

В табличном режиме чтение выполняется от первого до последнего смещения, хранящегося в топике на момент запуска запроса. Если в топик продолжается запись данных, то запрос остановится после достижения последнего смещения, известного на момент запуска. Указание фильтров по [Служебным полям](#system-metadata) ускоряет чтение, так как чтение происходит только по указанным диапазонам.

```yql
SELECT
    Data    -- тело сообщения
FROM
    input_topic  -- локальный топик; для внешнего: ext_source.input_topic
LIMIT 10;
```

### Потоковое чтение {#streaming-read}

Для чтения новых сообщений используйте опцию `WITH (STREAMING = "TRUE")` — подробнее в разделе [Потоковое чтение данных из топика](../../yql/reference/syntax/select/streaming.md). Чтение начинается с текущего момента и продолжается до тех пор, пока не будет прочитано заданное в выражении `LIMIT` количество сообщений. Параметр `LIMIT` обязателен — без него запрос не завершится, так как будет ожидать новые сообщения бесконечно.


```yql
SELECT
    Data
FROM
    ext_source.input_topic  -- внешний топик; для локального: input_topic
WITH (STREAMING = "TRUE")
LIMIT 10;
```

Для непрерывной обработки поступающих данных используйте [потоковые запросы](../glossary.md#streaming-query).

### Формат и схема сообщений {#format-schema}

При чтении из топика тело сообщения можно получить двумя способами: [сырые данные](#raw-read) и [форматированные данные](#formatted-read).

#### Сырые данные {#raw-read}

Используйте, когда содержимое сообщения не нужно разбирать — достаточно прочитать тело как есть.

```yql
SELECT
    Data
FROM
    input_topic  -- локальный топик; для внешнего: ext_source.input_topic
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    )
)
LIMIT 10;
```

В результате доступна только колонка `Data` — тело сообщения в исходном виде.

Тот же результат можно получить без блока `WITH` — см. [табличное чтение](#table-read).

#### Форматированные данные {#formatted-read}

Используйте, когда сообщения сериализованы в известном формате (JSON, CSV и др.). Параметр `FORMAT` задаёт способ разбора, а `SCHEMA` — имена и типы полей, которые появятся в результате `SELECT`:

```yql
SELECT
    Id,
    Name
FROM
    input_topic  -- локальный топик; для внешнего: ext_source.input_topic
WITH (
    FORMAT = json_each_row,
    SCHEMA = (
        Id Uint64 NOT NULL,
        Name Utf8 NOT NULL
    )
);
```

Поля из `SCHEMA` доступны в `SELECT` по имени — как колонки таблицы.

Подробнее о поддерживаемых форматах: [{#T}](../../dev/streaming-query/streaming-query-formats.md).

### Использование читателя {#consumer-usage}

{% include [consumer-usage](../../_includes/consumer-usage.md) %}

### Перенос данных из топика в таблицу через UPSERT {#upsert-from-topic}

Данные из топика можно переложить в таблицу через [UPSERT INTO](../../yql/reference/syntax/upsert_into.md):

```yql
UPSERT INTO
    table_name
SELECT
    Data            -- можно использовать любые преобразования
FROM
    ext_source.input_topic;  -- внешний топик; для локального: input_topic
```

### Служебные поля {#system-metadata}

При чтении можно запрашивать служебные поля:

```yql
SELECT
    Data,                                                   -- тело сообщения
    SystemMetadata('create_time') AS CreateTime,            -- время создания сообщения
    SystemMetadata('write_time') AS WriteTime,              -- время записи сообщения
    SystemMetadata('offset') AS Offset,                     -- смещение сообщения в топике
    SystemMetadata('partition_id') AS Partition,            -- номер партиции
    SystemMetadata('message_group_id') AS MessageGroupId,   -- идентификатор группы сообщений
    SystemMetadata('seq_no') AS SeqNo                       -- порядковый номер внутри партиции
FROM
    input_topic  -- локальный топик; для внешнего: ext_source.input_topic
LIMIT 10;
```

Фильтры по служебным полям вычисляются до чтения данных из топика и существенно сокращают объём считываемых сообщений. Поддерживаются операторы сравнения (`=`, `<>`, `<`, `<=`, `>`, `>=`, `IN`), логические условия (`AND`, `OR`) и поля `partition_id`, `write_time`, `offset`. Предикаты по остальным служебным полям не ограничивают объём чтения.

```yql
SELECT
    Data
FROM
    ext_source.input_topic  -- внешний топик; для локального: input_topic
WHERE
    SystemMetadata('partition_id') = 42
        AND SystemMetadata('offset') >= 1000
        AND SystemMetadata('offset') <= 1100
        AND SystemMetadata('write_time') > CurrentUtcTimestamp() - Interval("PT2H");
```

## Запись в топик {#topic-write}

### Запись одного сообщения {#simple-write}

```yql
INSERT INTO
    output_topic  -- локальный топик; для внешнего: ext_source.output_topic
SELECT
    "my_message";       -- тело сообщения
```

### Запись данных из таблицы {#write-from-table}

Чтобы записать в топик строку таблицы с несколькими колонками, сформируйте JSON-объект. Функция `TableRow` создаёт структуру из всех колонок, `Yson::From` преобразует её в Yson, `Yson::SerializeJson` сериализует в JSON-строку, а `ToBytes` конвертирует результат в тип `String`, который требуется для записи в топик:

```yql
INSERT INTO
    ext_source.output_topic  -- внешний топик; для локального: output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    table_name;
```

## Ограничения {#limitations}

{% note warning %}

Чтение и запись [пользовательских атрибутов](../datamodel/topic.md#message) не поддерживаются.

{% endnote %}

{% note warning %}

Транзакционная запись через YQL/`INSERT INTO` не поддерживается — в топике могут появиться частичные результаты запроса.

{% endnote %}

## См. также

- [Локальные и внешние топики](local-and-external-topics.md)
- [CREATE TOPIC](../../yql/reference/syntax/create-topic.md)
- [ALTER TOPIC](../../yql/reference/syntax/alter-topic.md)
- [DROP TOPIC](../../yql/reference/syntax/drop-topic.md)
- [Потоковые запросы](../../dev/streaming-query/index.md)
