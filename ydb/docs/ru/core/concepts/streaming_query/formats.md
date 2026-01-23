# Форматы данных при чтении/записи топиков

В данном разделе описываются форматы данных, поддерживаемые в {{ydb-full-name}} при чтении из топиков, и список поддерживаемых [YQL типов](../../yql/reference/types/index.md) для каждого формата данных.

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

Примеры парсинга данных в других форматах см. [Примеры парсинга данных в различных форматах](#parsing).

## Форматы при записи данных {#write_formats}

Для записи необходимо указывать одну колонку, поддерживаются типы: `String`, `Utf8`, `Json`, `Yson`. При этом колонка должна быть неопциональной. Например:

```yql
INSERT INTO
    ydb_source.output_topic
SELECT
    CAST(Data AS String) ?? "Unknown"
FROM
    ...
```

Чтобы записать значение нескольких колонок в формате `Json` одним полем можно воспользоваться выражением:

```yql
INSERT INTO
    ydb_source.output_topic
SELECT
    ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
FROM
    ...
```

Описание функций вы можете найти в документации: [TableRow](../../yql/reference/builtins/basic#tablerow), [Yson::From](../../yql/reference/udf/list/yson#ysonfrom), [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson), [Unwrap](../../yql/reference/builtins/basic#unwrap), [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

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

```yql
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
)
LIMIT 1
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

```yql
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
)
LIMIT 1
```

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

Пример запроса:

```yql
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
)
LIMIT 1
```

### Формат json_each_row {#json_each_row}

Данный формат основан на [JSON-представлении](https://ru.wikipedia.org/wiki/JSON) данных. В этом формате внутри каждого сообщения в топике должен находиться объект в корректном JSON-представлении. Такой формат используется при передаче данных через потоковые системы, например, Apache Kafka или [Топики {{ydb-full-name}}](../datamodel/topic.md).
Несколько отдельных JSON в одном сообщении не поддерживается; JSON-список также не поддерживается.

Пример данных (в одном сообщении):

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
```

Пример запроса:

```yql
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
)
LIMIT 1
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

```yql
SELECT
    *
FROM
    ydb_source.input_topic
WITH (
    FORMAT = json_as_string,
    SCHEMA = (
        Data Json
    )
)
LIMIT 1
```

### Формат parquet {#parquet}

Данный формат позволяет считывать содержимое сообщений в формате [Apache Parquet](https://parquet.apache.org).

Пример запроса:

```yql
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
)
LIMIT 1
```

### Формат raw {#raw}

Данный формат позволяет считывать содержимое сообщений как есть, в "сыром" виде. Считанные таким образом данные можно обработать средствами [YQL](../../../yql/reference/udf/list/string). Cхема по умолчанию: `SCHEMA(Data String)`.

Пример запроса:

```yql
SELECT
    *
FROM
    ydb_source.input_topic
WITH (
    FORMAT = raw,
    SCHEMA = (
        Data String
    )
)
LIMIT 1
```

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

## Примеры парсинга данных в различных форматах {#parsing}

### Парсинг JSON встроенными функциями {#json_builtins}

Пример данных:

```json
{"key": 1997, "value": "42"}
```

Пример запроса:

```yql
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
)
LIMIT 1
```

### Парсинг JSON библиотекой Yson {#json_yson}

Пример данных (формат [Change Data Capture](../concepts/cdc)):

```json
{"update":{"volume":10,"product":"bWlsaw=="},"key":[6],"ts":[1765192622420,18446744073709551615]}
```

Пример запроса:

```yql
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

SELECT
    ts[0] AS Ts,
    update.volume AS Volume
FROM
    $input
FLATTEN COLUMNS
LIMIT 1
```

### Парсинг DSV (TSKV) {#dsv}

Пример данных:

```json
name=Elena  uid=95792365232151958
name=Denis  uid=78086244452810046
name=Mikhail    uid=70609792906901286
```

Пример запроса:

```yql
$input = SELECT
    Dsv::Parse(Data, "\t") AS Data
FROM
    ydb_source.input_topic
FLATTEN LIST BY (
    String::SplitToList(Data, "\n", TRUE AS SkipEmpty) AS Data
);

SELECT
    DictLookup(Data, "name") AS Name,
    DictLookup(Data, "uid") AS Uid
FROM
    $input
LIMIT 2
```
