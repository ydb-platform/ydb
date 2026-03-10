# Форматы данных при чтении/записи из топиков

В данном разделе описываются форматы данных [потоковых запросов](../../concepts/glossary.md#streaming-query), поддерживаемые в {{ydb-full-name}} при чтении из топиков, и список поддерживаемых [YQL типов](../../yql/reference/types/index.md) для каждого формата данных.

## Поддерживаемые форматы данных {#formats}

{{ ydb-short-name }} поддерживает встроенные форматы данных (предопределённые по умолчанию) и пользовательские форматы (настраиваемые пользователем под специфические задачи).

Список встроенных в {{ ydb-short-name }} форматов данных приведен ниже:

- [`csv_with_names`](#csv_with_names)
- [`tsv_with_names`](#tsv_with_names)
- [`json_list`](#json_list)
- [`json_each_row`](#json_each_row)
- [`json_as_string`](#json_as_string)
- [`parquet`](#parquet)
- [`raw`](#raw)

[Примеры парсинга данных в пользовательских форматах](#parsing).

В примерах на этой странице `ydb_source` — это заранее созданный [внешний источник данных](../../concepts/datamodel/external_data_source.md), `input_topic` и `output_topic` — топики, доступные через него, а `output_table` — таблица {{ ydb-short-name }}.

## Форматы при записи данных {#write_formats}

При записи в топик `SELECT` должен возвращать одну колонку типа `String`, `Utf8`, `Json` или `Yson`. Колонка не может быть `Optional`.

Запись одной колонки:

```sql
CREATE STREAMING QUERY write_string_example AS
DO BEGIN

    INSERT INTO ydb_source.output_topic
    SELECT
        CAST(Data AS String)
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = raw,
        SCHEMA = (
            Data String
        )
    );

END DO
```

Чтобы записать несколько колонок, сериализуйте их в JSON:

```sql
CREATE STREAMING QUERY write_json_example AS
DO BEGIN

    INSERT INTO ydb_source.output_topic
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = json_each_row,
        SCHEMA = (
            Id Uint64 NOT NULL,
            Name Utf8 NOT NULL
        )
    );

END DO
```

Подробнее о функциях: [TableRow](../../yql/reference/builtins/basic#tablerow), [Yson::From](../../yql/reference/udf/list/yson#ysonfrom), [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson), [Unwrap](../../yql/reference/builtins/basic#unwrap), [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

## Форматы при чтении данных {#read_formats}

### Формат csv_with_names {#csv_with_names}

Данный формат основан на формате [CSV](https://ru.wikipedia.org/wiki/CSV). Данные размещены в колонках, разделены запятыми, в первой строке находятся имена колонок.

Пример данных (в одном сообщении):

```text
Year,Manufacturer,Model,Price
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```

Пример запроса:

```sql
CREATE STREAMING QUERY csv_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = csv_with_names,
        SCHEMA = (
            Year Int32 NOT NULL,
            Manufacturer String NOT NULL,
            Model String NOT NULL,
            Price Double NOT NULL
        )
    );

END DO
```

### Формат tsv_with_names {#tsv_with_names}

Данный формат основан на формате [TSV](https://ru.wikipedia.org/wiki/TSV). Данные размещены в колонках, разделены символами табуляции (код `0x9`), в первой строке находятся имена колонок.

Пример данных (в одном сообщении):

```text
Year    Manufacturer    Model   Price
1997    Man_1   Model_1    3000.00
1999    Man_2   Model_2    4900.00
```

Пример запроса:

```sql
CREATE STREAMING QUERY tsv_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = tsv_with_names,
        SCHEMA = (
            Year Int32 NOT NULL,
            Manufacturer String NOT NULL,
            Model String NOT NULL,
            Price Double NOT NULL
        )
    );

END DO
```

### Формат json_list {#json_list}

Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. В этом формате каждое сообщение должно представлять собой JSON‑массив объектов.

Пример корректных данных (в виде списка объектов JSON):

```json
[
    { "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 },
    { "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
]
```

Пример некорректных данных (в каждом сообщении находится отдельный объект в формате JSON, но эти объекты не объединены в список):

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

Пример запроса:

```sql
CREATE STREAMING QUERY json_list_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = json_list,
        SCHEMA = (
            Year Int32 NOT NULL,
            Manufacturer String NOT NULL,
            Model String NOT NULL,
            Price Double NOT NULL
        )
    );

END DO
```

### Формат json_each_row {#json_each_row}

Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. В этом формате каждое сообщение должно представлять собой JSON-объект. Такой формат используется при передаче данных через потоковые системы, например, Apache Kafka или [Топики {{ydb-full-name}}](../../concepts/datamodel/topic.md).
Несколько отдельных JSON в одном сообщении не поддерживается; JSON-список также не поддерживается.

Пример корректных данных (в одном сообщении):

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
```

Пример некорректных данных:

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

Пример запроса:

```sql
CREATE STREAMING QUERY json_each_row_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = json_each_row,
        SCHEMA = (
            Year Int32 NOT NULL,
            Manufacturer Utf8 NOT NULL,
            Model Utf8 NOT NULL,
            Price Double NOT NULL
        )
    );

END DO
```

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

Пример запроса:

```sql
CREATE STREAMING QUERY json_as_string_example AS
DO BEGIN

    INSERT INTO ydb_source.output_topic
    SELECT
        *
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = json_as_string,
        SCHEMA = (
            Data Json
        )
    );

END DO
```

### Формат parquet {#parquet}

Данный формат позволяет считывать содержимое сообщений в формате [Apache Parquet](https://parquet.apache.org).

Пример запроса:

```sql
CREATE STREAMING QUERY parquet_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = parquet,
        SCHEMA = (
            Year Int32 NOT NULL,
            Manufacturer Utf8 NOT NULL,
            Model Utf8 NOT NULL,
            Price Double NOT NULL
        )
    );

END DO
```

### Формат raw {#raw}

Данный формат позволяет считывать содержимое сообщений как есть, в "сыром" виде. Считанные таким образом данные можно обработать средствами [YQL](../../yql/reference/udf/list/string). Cхема по умолчанию: `SCHEMA(Data String)`.

Пример запроса:

```sql
CREATE STREAMING QUERY raw_example AS
DO BEGIN

    INSERT INTO ydb_source.output_topic
    SELECT
        *
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = raw,
        SCHEMA = (
            Data String
        )
    );

END DO
```

### Поддерживаемые типы данных {#schema}

Таблица всех поддерживаемых типов в схеме запроса:

|Тип                                  |csv_with_names|tsv_with_names|json_list|json_each_row|json_as_string|parquet|raw|
|-------------------------------------|--------------|--------------|---------|-------------|--------------|-------|---|
|`Int8`, `Int16`, `Int32`, `Int64`,<br/>`Uint8`, `Uint16`, `Uint32`, `Uint64`,<br/>`Float`, `Double`|✓|✓|✓|✓|✓ |✓   |   |
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

## Примеры парсинга данных в пользовательских форматах {#parsing}

### Парсинг JSON встроенными функциями {#json_builtins}

Пример данных:

```json
{"key": 1997, "value": "42"}
```

Пример запроса:

```sql
CREATE STREAMING QUERY json_builtins_example AS
DO BEGIN

    INSERT INTO ydb_source.output_topic
    SELECT
        JSON_VALUE(Data, "$.key") AS Key,
        JSON_VALUE(Data, "$.value") AS Value
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = raw,
        SCHEMA = (
            Data Json
        )
    );

END DO
```

Подробнее о функциях: [JSON_VALUE](../../yql/reference/builtins/json.md#json_value).

### Парсинг JSON библиотекой Yson {#json_yson}

Пример данных (формат [Change Data Capture](../../concepts/cdc.md)):

```json
{"update":{"volume":10,"product":"bWlsaw=="},"key":[6],"ts":[1765192622420,18446744073709551615]}
```

Пример запроса:

```sql
CREATE STREAMING QUERY json_yson_example AS
DO BEGIN

    $input = SELECT
        Yson::ConvertTo(
            Data, Struct<
                update: Struct<volume: Uint64>,
                key: List<Uint64>,
                ts: List<Uint64>
            >
        )
    FROM
        ydb_source.input_topic
    WITH (
        FORMAT = json_as_string,
        SCHEMA = (
            Data Json
        )
    );

    INSERT INTO ydb_source.output_topic
    SELECT
        ts[0] AS Ts,
        update.volume AS Volume
    FROM
        $input
    FLATTEN COLUMNS;

END DO
```

Подробнее о функциях:

- [Yson::ConvertTo](../../yql/reference/udf/list/yson#ysonconvertto)
- [FLATTEN COLUMNS](../../yql/reference/syntax/select/flatten.md#flatten-columns).

### Парсинг DSV (TSKV) {#dsv}

Пример данных:

```json
name=Elena  uid=95792365232151958
name=Denis  uid=78086244452810046
name=Mikhail    uid=70609792906901286
```

Пример запроса:

```sql
CREATE STREAMING QUERY dsv_example AS
DO BEGIN

    $input = SELECT
        Dsv::Parse(Data, "\t") AS Data
    FROM
        ydb_source.input_topic
    FLATTEN LIST BY (
        String::SplitToList(Data, "\n", TRUE AS SkipEmpty) AS Data
    );

    INSERT INTO ydb_source.output_topic
    SELECT
        DictLookup(Data, "name") AS Name,
        DictLookup(Data, "uid") AS Uid
    FROM
        $input;

END DO
```

Подробнее о функциях:

- [String::SplitToList](../../yql/reference/udf/list/string.md#splittolist)
- [DictLookup](../../yql/reference/builtins/dict.md#dictlookup)
- [FLATTEN LIST BY](../../yql/reference/syntax/select/flatten.md#flatten-by).
