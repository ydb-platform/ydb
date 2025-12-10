# Форматы данных при чтении/записи топиков

В данном разделе описываются поддерживаемые в {{ydb-full-name}} форматы данных, поддерживаемуе при чтении из топиков и список поддерживаемых [YQL типов](../../yql/reference/types/index.md) для каждого формата данных.

## Поддерживаемые форматы данных {#formats}

Список поддерживаемых в {{ ydb-short-name }} форматов данных приведен в таблице ниже.

| Формат                              | Чтение | Запись |
|-------------------------------------|--------|--------|
| [`csv_with_names`](#csv_with_names) | ✓      |        |
| [`tsv_with_names`](#tsv_with_names) | ✓      |        |
| [`json_list`](#json_list)           | ✓      |        |
| [`json_each_row`](#json_each_row)   | ✓      |        |
| [`json_as_string`](#json_as_string) | ✓      |        |
| [`parquet`](#parquet)               | ✓      |        |
| [`raw`](#raw)                       | ✓      | ✓      |

## Форматы при записи данных {#write-formats}

Для записи небходимо указывать одну колонку, поддерживаются типы: String, Json, Yson. При этом колонка должна быть неопциональная. Например:

```sql
INSERT INTO source_name.output_topic_name
SELECT
    CAST(Data as String)
FROM
    ...
```

Чтобы записать значение нескольких колонок в формате Json (одним полем) можно воспользоваться выражением:

```sql

INSERT INTO source_name.output_topic_name
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    ...
```
Описание функций вы можете найти в документации: [TableRow](../../yql/reference/builtins/basic#tablerow), [Yson::From](../../yql/reference/udf/list/yson#ysonfrom), [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson), [Unwrap](../../yql/reference/builtins/basic#unwrap), [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

### Формат csv_with_names {#csv_with_names}

Данный формат основан на формате [CSV](https://ru.wikipedia.org/wiki/CSV). Данные размещены в колонках, разделены запятыми, в первой строке находятся имена колонок.

Пример данных (в одном сообщении):

```text
Year,Manufacturer,Model,Price
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```

{% cut "Пример запроса" %}

```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
PRAGMA pq.Consumer = 'ConsumerName';

$input = (SELECT
    *
    FROM source_name.input_topic_name WITH
        (
            FORMAT = "csv_with_names",
            SCHEMA =(Year Int32 NOT NULL, Manufacturer String NOT NULL, Model String NOT NULL, Price Double NOT NULL)
        );
);

INSERT INTO source_name.output_topic_name
SELECT
    Model
FROM
    $input
;
```
{% endcut %}

### Формат tsv_with_names {#tsv_with_names}

Данный формат основан на формате [TSV](https://ru.wikipedia.org/wiki/TSV). Данные размещены в колонках, разделены символами табуляции (код `0x9`), в первой строке находятся имена колонок.

Пример данных (в одном сообщении):

```text
Year    Manufacturer    Model   Price
1997    Man_1   Model_1    3000.00
1999    Man_2   Model_2    4900.00
```

{% cut "Пример запроса" %}

```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
PRAGMA pq.Consumer = 'ConsumerName';

$input = (SELECT
    *
    FROM source_name.input_topic_name WITH
        (
            FORMAT = "tsv_with_names",
            SCHEMA =(Year Int32 NOT NULL, Manufacturer String NOT NULL, Model String NOT NULL, Price Double NOT NULL)
        );
);

INSERT INTO source_name.output_topic_name
SELECT
    Model
FROM
    $input
;
```

{% endcut %}

### Формат json_list {#json_list}

Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. В этом формате внутри каждого сообщения должен находиться объект в корректном JSON-представлении.

Пример корректных данных (данные представлены в виде списка объектов JSON):

```json
[
    { "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 },
    { "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
]
```

Пример **НЕ**корректных данных (в каждом сообщении находится отдельный объект в формате JSON, но эти объекты не объединены в список):

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

### Формат json_each_row {#json_each_row}

Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. В этом формате внутри каждого сообщения в топике должен находиться объект в корректном JSON-представлении. Такой формат используется при передаче данных через потоковые системы, например, Apache Kafka или [Топики {{ydb-full-name}}](../datamodel/topic.md).
Несколько отдельных json в одном сообщении не поддерживается; JSON-список также не поддерживается.

Пример данных (в одном сообщении):

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
```

{% cut "Пример запроса" %}

```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
PRAGMA pq.Consumer = 'ConsumerName';

$input = (
    SELECT
        *
    FROM
        source_name.input_topic_name WITH (
            FORMAT = 'json_each_row',
            SCHEMA (Year Int32, Manufacturer Utf8, Model Utf8, Price Double)
        )
);

$filtered = (
    SELECT
        *
    FROM
        $input
    WHERE
        level == 'error'
);

$number_errors = (
    SELECT
        COUNT(*) AS error_count,
        CAST(HOP_START() AS String) AS ts
    FROM
        $filtered
    GROUP BY
        HOP (CAST(time AS Timestamp), 'PT600S', 'PT600S', 'PT0S'),
        host
);

$json = (
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
    FROM
        $number_errors
);

INSERT INTO source_name.output_topic_name
SELECT
    *
FROM
    $json
;
END DO;

```

{% endcut %}

### Формат json_as_string {#json_as_string}

Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных.

В этом формате внутри каждого сообщения должен находиться:

- объект в корректном JSON-представлении в каждой отдельной строке файла;
- объекты в корректном JSON-представлении, объединенные в список.

Формат `json_as_string` не разбивает входной JSON-документ на поля, а представляет каждое сообщение в виде одного объекта JSON (или одной строки). Такой формат удобен, если список полей не одинаков во всех строках, а может изменяться.

Пример корректных данных:

```json
{ "Year": 1997, "Attrs": { "Manufacturer": "Man_1", "Model": "Model_1" }, "Price": 3000.0 }
{ "Year": 1999, "Attrs": { "Manufacturer": "Man_2", "Model": "Model_2" }, "Price": 4900.00 }
```

В этом формате схема читаемых данных должна состоять только из одной колонки с одним из разрешённых типов данных, подробнее см. [ниже](#schema).

{% cut "Пример запроса" %}

```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
PRAGMA pq.Consumer = 'ConsumerName';

$input = (
    SELECT
        CAST(JSON_VALUE(Data, "$.Year") AS Int32) AS Year,
        JSON_VALUE(Data, "$.Attrs.Manufacturer") AS Manufacturer,
        JSON_VALUE(Data, "$.Attrs.Model") AS Model,
        CAST(JSON_VALUE(Data, "$.Price") AS Double) AS Price
    FROM
        source_name.input_topic_name WITH (
            FORMAT = 'json_as_string',
            SCHEMA (Data Json)
        )
);

$json = (
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
    FROM
        $input
);

INSERT INTO source_name.output_topic_name
SELECT
    *
FROM
    $json
```

{% endcut %}

### Формат parquet {#parquet}

Данный формат позволяет считывать содержимое сообщений в формате [Apache Parquet](https://parquet.apache.org).


{% cut "Пример запроса" %}

```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
PRAGMA pq.Consumer = 'ConsumerName';

$input = (
    SELECT
        *
    FROM
        source_name.input_topic_name WITH (
            FORMAT = 'parquet',
            SCHEMA (Year Int32, Manufacturer Utf8, Model Utf8, Price Double)
        )
);

$json = (
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
    FROM
        $input
);

INSERT INTO source_name.output_topic_name
SELECT
    *
FROM
    $json
```

{% endcut %}

### Формат raw {#raw}

Данный формат позволяет считывать содержимое сообщений как есть, в "сыром" виде. Считанные таким образом данные можно обработать средствами [YQL](../../../yql/reference/udf/list/string). Cхема по умолчанию: `SCHEMA(Data String)`.

{% cut "Пример запроса" %}

```sql
CREATE STREAMING QUERY `my_queries/query_name` AS
DO BEGIN
$input = (
    SELECT
        CAST(data AS Json) AS json
    FROM
        source_name.input_topic_name WITH (
            FORMAT = 'raw',
            SCHEMA = (data String)
        )
);

$parsed = (
    SELECT
        JSON_VALUE (json, '$.field1') AS field1,
        JSON_VALUE (json, '$.field2') AS field2
    FROM
        $input
);

INSERT INTO source_name.output_topic_name
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    $parsed
;
END DO;
```

{% endcut %}

### Поддерживаемые типы данных {#schema}

Таблица всех поддерживаемых типов в схеме запроса:
|Тип                                  |csv_with_names|tsv_with_names|json_list|json_each_row|json_as_string|parquet|raw|
|-------------------------------------|--------------|--------------|---------|-------------|--------------|-------|---|
|`Int8`, `Int16`, `Int32`, `Int64`,<br/>`Uint8`, `Uint16`, `Uint32`, `Uint64`,<br/>`Float`, `Double`|✓|✓|✓||✓ |✓   |   |
|`Bool`                               |✓             |✓             |✓        |✓            |              |✓      |   |
|`DyNumber`                           |              |              |         |             |              |       |   |
|`String`, `Utf8`                     |✓             |✓             |✓        |✓            |✓             |✓      |✓  |
|`Json`                               |              |              |         |             |✓             |       |✓  |
|`JsonDocument`                       |              |              |         |             |              |       |   |
|`Yson`                               |              |              |         |             |              |       |   |
|`Uuid`                               |              |              |         |✓            |              |       |   |
|`Date`, `Datetime`, `Timestamp`,<br/>`TzDate`, `TzDateTime`, `TzTimestamp`| | | |          |              |       |   |
|`Interval`                           |              |              |         |             |              |       |   |
|`Date32`, `Datetime64`, `Timestamp64`,<br/>`Interval64`,<br/>`TzDate32`, `TzDateTime64`, `TzTimestamp64`|| ||||   |   |
|`Optional<T>`                        |✓             |✓             |✓        |✓            |✓             |✓      |✓  |

