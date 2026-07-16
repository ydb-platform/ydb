# Data formats for reading/writing from topics

This section describes the data formats of [streaming queries](../../concepts/glossary.md#streaming-query) supported in {{ydb-full-name}} when reading from topics, and the list of supported [YQL types](../../yql/reference/types/index.md) for each data format.

## Supported data formats {#formats}

{{ ydb-short-name }} supports built-in data formats (predefined by default) and custom formats (configurable by the user for specific tasks).

The list of built-in data formats in {{ ydb-short-name }} is given below:

- [`csv_with_names`](#csv_with_names)
- [`tsv_with_names`](#tsv_with_names)
- [`json_list`](#json_list)
- [`json_each_row`](#json_each_row)
- [`json_as_string`](#json_as_string)
- [`parquet`](#parquet)
- [`raw`](#raw)

[Examples of parsing data in custom formats](#parsing).

In the examples on this page, `input_topic` and `output_topic` are [topics](../../concepts/datamodel/topic.md) in the current database, and `output_table` is a {{ ydb-short-name }} table. For topics in another database, see [local and external topics](../../concepts/query_execution/topics.md#local-external-topics).

## Formats for writing data {#write_formats}

When writing to a topic, `SELECT` must return a single column of type `String`, `Utf8`, `Json`, or `Yson`. The column cannot be `Optional`.

Writing a single column:


```sql
CREATE STREAMING QUERY write_string_example AS
DO BEGIN

    INSERT INTO output_topic
    SELECT
        CAST(Data AS String)
    FROM
        input_topic
    WITH (
        FORMAT = raw,
        SCHEMA = (
            Data String
        )
    );

END DO
```


To write multiple columns, serialize them into JSON:


```sql
CREATE STREAMING QUERY write_json_example AS
DO BEGIN

    INSERT INTO output_topic
    SELECT
        ToBytes(Unwrap(Yson::SerializeJson(Yson::From(TableRow()))))
    FROM
        input_topic
    WITH (
        FORMAT = json_each_row,
        SCHEMA = (
            Id Uint64 NOT NULL,
            Name Utf8 NOT NULL
        )
    );

END DO
```


For more details about the functions: [TableRow](../../yql/reference/builtins/basic#tablerow), [Yson::From](../../yql/reference/udf/list/yson#ysonfrom), [Yson::SerializeJson](../../yql/reference/udf/list/yson#ysonserializejson), [Unwrap](../../yql/reference/builtins/basic#unwrap), [ToBytes](../../yql/reference/builtins/basic#to-from-bytes).

## Formats for reading data {#read_formats}

### csv_with_names format {#csv_with_names}

This format is based on the [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) format. Data is placed in columns separated by commas, with column names in the first row.

Example data (in a single message):


```text
Year,Manufacturer,Model,Price
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```


Example query:


```sql
CREATE STREAMING QUERY csv_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        input_topic
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


### tsv_with_names format {#tsv_with_names}

This format is based on the [TSV](https://en.wikipedia.org/wiki/Tab-separated_values) format. Data is placed in columns separated by tab characters (code `0x9`), with column names in the first row.

Example data (in a single message):


```text
Year    Manufacturer    Model   Price
1997    Man_1   Model_1    3000.00
1999    Man_2   Model_2    4900.00
```


Example query:


```sql
CREATE STREAMING QUERY tsv_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        input_topic
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


### json_list format {#json_list}

This format is based on the [JSON representation](https://en.wikipedia.org/wiki/JSON) of data. In this format, each message must be a JSON array of objects.

Example of correct data (as a list of JSON objects):


```json
[
    { "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 },
    { "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
]
```


Example of incorrect data (each message contains a separate JSON object, but these objects are not combined into a list):


```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```


Example query:


```sql
CREATE STREAMING QUERY json_list_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        input_topic
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


### json_each_row format {#json_each_row}

This format is based on the [JSON representation](https://en.wikipedia.org/wiki/JSON) of data. In this format, each message must be a JSON object. This format is used when transferring data through streaming systems, such as Apache Kafka or [{{ydb-full-name}} Topics](../../concepts/datamodel/topic.md).
Multiple separate JSON objects in a single message are not supported; a JSON list is also not supported.

Example of correct data (in a single message):


```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
```


Example of incorrect data:


```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```


Example query:


```sql
CREATE STREAMING QUERY json_each_row_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        input_topic
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


### json_as_string format {#json_as_string}

This format is based on the [JSON representation](https://en.wikipedia.org/wiki/JSON) of data.

In this format, each message must contain:

- an object in valid JSON representation on each separate line of the file;
- objects in valid JSON representation combined into a list.

The `json_as_string` format does not split the input JSON document into fields, but represents each message as a single JSON object (or a single line). This format is convenient when the list of fields is not the same across all lines and can vary.

Example of correct data:


```json
{ "Year": 1997, "Attrs": { "Manufacturer": "Man_1", "Model": "Model_1" }, "Price": 3000.0 }
{ "Year": 1999, "Attrs": { "Manufacturer": "Man_2", "Model": "Model_2" }, "Price": 4900.00 }
```


In this format, the schema of the read data must consist of only one column with one of the allowed data types; see [below](#schema) for details.

Example query:


```sql
CREATE STREAMING QUERY json_as_string_example AS
DO BEGIN

    INSERT INTO output_topic
    SELECT
        *
    FROM
        input_topic
    WITH (
        FORMAT = json_as_string,
        SCHEMA = (
            Data Json
        )
    );

END DO
```


### parquet format {#parquet}

This format allows reading the contents of messages in the [Apache Parquet](https://parquet.apache.org) format.

Example query:


```sql
CREATE STREAMING QUERY parquet_example AS
DO BEGIN

    UPSERT INTO output_table
    SELECT
        *
    FROM
        input_topic
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


### raw format {#raw}

This format allows reading the contents of messages as-is, in "raw" form. Data read in this way can be processed using [YQL](../../yql/reference/udf/list/string). Default schema: `SCHEMA(Data String)`.

Example query:


```sql
CREATE STREAMING QUERY raw_example AS
DO BEGIN

    INSERT INTO output_topic
    SELECT
        *
    FROM
        input_topic
    WITH (
        FORMAT = raw,
        SCHEMA = (
            Data String
        )
    );

END DO
```


### Supported data types {#schema}

Table of all supported types in the query schema:

| Type | csv_with_names | tsv_with_names | json_list | json_each_row | json_as_string | parquet | raw |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `Int8`, `Int16`, `Int32`, `Int64`,<br/>`Uint8`, `Uint16`, `Uint32`, `Uint64`,<br/>`Float`, `Double` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |  |
| `Bool` | ✓ | ✓ | ✓ | ✓ |  | ✓ |  |
| `DyNumber` |  |  |  |  |  |  |  |
| `String`, `Utf8` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `Json` |  |  |  |  | ✓ |  | ✓ |
| `JsonDocument` |  |  |  |  |  |  |  |
| `Yson` |  |  |  |  |  |  |  |
| `Uuid` |  |  |  | ✓ |  |  |  |
| `Date`, `Datetime`, `Timestamp`,<br/>`TzDate`, `TzDateTime`, `TzTimestamp` |  |  |  |  |  |  |  |
| `Interval` |  |  |  |  |  |  |  |
| `Date32`, `Datetime64`, `Timestamp64`,<br/>`Interval64`,<br/>`TzDate32`, `TzDateTime64`, `TzTimestamp64` |  |  |  |  |  |  |  |
| `Optional<T>` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

## Examples of parsing data in custom formats {#parsing}

### Parsing JSON with built-in functions {#json_builtins}

Example data:


```json
{"key": 1997, "value": "42"}
```


Example query:


```sql
CREATE STREAMING QUERY json_builtins_example AS
DO BEGIN

    INSERT INTO output_topic
    SELECT
        JSON_VALUE(Data, "$.key") AS Key,
        JSON_VALUE(Data, "$.value") AS Value
    FROM
        input_topic
    WITH (
        FORMAT = raw,
        SCHEMA = (
            Data Json
        )
    );

END DO
```


For more details about functions, see [JSON_VALUE](../../yql/reference/builtins/json.md#json_value).

### Parsing JSON with the Yson library {#json_yson}

Example data ( [Change Data Capture](../../concepts/cdc.md) format):


```json
{"update":{"volume":10,"product":"bWlsaw=="},"key":[6],"ts":[1765192622420,18446744073709551615]}
```


Example query:


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
        input_topic
    WITH (
        FORMAT = json_as_string,
        SCHEMA = (
            Data Json
        )
    );

    INSERT INTO output_topic
    SELECT
        ts[0] AS Ts,
        update.volume AS Volume
    FROM
        $input
    FLATTEN COLUMNS;

END DO
```


For more details on functions:

- [Yson::ConvertTo](../../yql/reference/udf/list/yson#ysonconvertto)
- [FLATTEN COLUMNS](../../yql/reference/syntax/select/flatten.md#flatten-columns).

### Parsing DSV (TSKV) {#dsv}

Example data:


```json
name=Elena  uid=95792365232151958
name=Denis  uid=78086244452810046
name=Mikhail    uid=70609792906901286
```


Example query:


```sql
CREATE STREAMING QUERY dsv_example AS
DO BEGIN

    $input = SELECT
        Dsv::Parse(Data, "\t") AS Data
    FROM
        input_topic
    FLATTEN LIST BY (
        String::SplitToList(Data, "\n", TRUE AS SkipEmpty) AS Data
    );

    INSERT INTO output_topic
    SELECT
        DictLookup(Data, "name") AS Name,
        DictLookup(Data, "uid") AS Uid
    FROM
        $input;

END DO
```


For more details on functions:

- [String::SplitToList](../../yql/reference/udf/list/string.md#splittolist)
- [DictLookup](../../yql/reference/builtins/dict.md#dictlookup)
- [FLATTEN LIST BY](../../yql/reference/syntax/select/flatten.md#flatten-by).
