# Data formats and compression algorithms

This section describes the data formats supported in {{ydb-full-name}} that are stored in S3, the supported compression algorithms, and the list of supported [YQL types](../../../../yql/reference/types/index.md) for each data format.

## Supported data formats {#formats}

The list of data formats supported in {{ ydb-short-name }} is provided in the table below.
| Format | Reading | Writing |
|-------------------------------------|--------|--------|
| [`csv`](#csv) | ✓ | |
| [`csv_with_names`](#csv_with_names) | ✓ | ✓ |
| [`tsv_with_names`](#tsv_with_names) | ✓ | ✓ |
| [`json_list`](#json_list) | ✓ | ✓ |
| [`json_each_row`](#json_each_row) | ✓ | ✓ |
| [`json_as_string`](#json_as_string) | ✓ | |
| [`parquet`](#parquet) | ✓ | ✓ |
| [`raw`](#raw) | ✓ | ✓ |

### CSV format {#csv}

This format is based on the [CSV](https://en.wikipedia.org/wiki/CSV) format and differs from [`csv_with_names`](#csv_with_names) in that it does not have a header with column names. All lines in the file are treated as data, and column names are taken from the [schema](external_data_source.md#schema) of the query in the order they are declared in `SCHEMA`.

{% note info %}

The `csv` format is available only for reading data from S3. Writing in this format is not supported.

{% endnote %}

Example of data (without a header row):

```text
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```

{% cut "Example query" %}

```yql
SELECT
    *
FROM external_source.path
WITH
(
    FORMAT = "csv",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

The result of the query execution:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### Csv_with_names format {#csv_with_names}

This format is based on the [CSV format](https://en.wikipedia.org/wiki/CSV). The data is arranged in columns, separated by commas, and the first line of the file contains the column names. Unlike the [`csv` format](#csv), where column names are taken from the [query schema](external_data_source.md#schema) and matched positionally, in the `csv_with_names` format, column names are read directly from the header line of the file and matched to the schema by name.

Example data:

```text
Year,Manufacturer,Model,Price
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```

{% cut "Example query" %}

```yql
SELECT
    *
FROM external_source.path
WITH
(
    FORMAT = "csv_with_names",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

The result of the query execution:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### tsv_with_names format {#tsv_with_names}

This format is based on the [TSV format](https://en.wikipedia.org/wiki/TSV). The data is arranged in columns and separated by tab characters (code `0x9`). The first line of the file contains the column names.

Example of data:

```text
Year    Manufacturer    Model   Price
1997    Man_1   Model_1    3000.00
1999    Man_2   Model_2    4900.00
```

{% cut "Example query" %}

```yql
SELECT
    *
FROM external_source.path
WITH
(
    FORMAT = "tsv_with_names",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

The result of the query execution:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### json_list format {#json_list}

This format is based on [JSON representation](https://en.wikipedia.org/wiki/JSON) of data. In this format, each file must contain an object in a valid JSON representation.

Example of valid data (data is presented as a list of JSON objects):

```json
[
    { "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 },
    { "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
]
```

Example of **IN**correct data (each separate line contains a separate JSON object, but these objects are not combined into a list):

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

{% cut "Example query" %}

```yql
SELECT
    *
FROM external_source.path
WITH
(
    FORMAT = "json_list",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

Result of query execution:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### json_each_row format {#json_each_row}

This format is based on [JSON representation](https://en.wikipedia.org/wiki/JSON) of data. In this format, each line of the file must contain a separate object in a valid JSON representation, but these objects are not combined into a JSON list. This format is used when transferring data through streaming systems, such as Apache Kafka or {{ydb-full-name}} topics (../../../datamodel/topic.md).

Example of correct data (each line contains a separate JSON object, but these objects are not combined into a list):

```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 },
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

{% cut "Example query" %}

```yql
SELECT
    *
FROM external_source.path
WITH
(
    FORMAT = "json_each_row",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

The result of the query execution:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### json_as_string format {#json_as_string}

This format is based on [JSON representation](https://en.wikipedia.org/wiki/JSON). The `json_as_string` format does not split the input JSON document into fields, but represents each line of the file as a single JSON object (or a single string). This format is convenient if the list of fields is not the same in all lines and can vary.

In this format, each file should contain:

- a JSON object in a valid JSON representation on each separate line of the file;
- JSON objects in a valid JSON representation, combined into a list.

Example of valid data (data is presented as a list of JSON objects):

```json
{ "Year": 1997, "Attrs": { "Manufacturer": "Man_1", "Model": "Model_1" }, "Price": 3000.0 }
{ "Year": 1999, "Attrs": { "Manufacturer": "Man_2", "Model": "Model_2" }, "Price": 4900.00 }
```

In this format, the [schema](external_data_source.md#schema) of the data being read must consist of only one column with one of the allowed data types; see [below](#types) for details.

{% cut "Example query" %}

```yql
SELECT
    CAST(JSON_VALUE(Data, "$.Year") AS Int32) AS Year,
    JSON_VALUE(Data, "$.Attrs.Manufacturer") AS Manufacturer,
    JSON_VALUE(Data, "$.Attrs.Model") AS Model,
    CAST(JSON_VALUE(Data, "$.Price") AS Double) AS Price
FROM external_source.path
WITH
(
    FORMAT = "json_as_string",
    SCHEMA =
    (
        Data Json
    )
)
```

The result of the query execution:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### Parquet format {#parquet}

This format allows reading the contents of files in the [Apache Parquet](https://parquet.apache.org) format.

Supported data compression algorithms inside Parquet files for reading from S3:

- No compression
- SNAPPY
- GZIP
- BROTLI
- LZ4
- ZSTD
- LZ4_RAW

{% note info %}

Parquet format records will be written using the [Snappy](https://en.wikipedia.org/wiki/Snappy_(library)) compression algorithm.

{% endnote %}

{% cut "Example query" %}

```yql
SELECT
    *
FROM external_source.path
WITH
(
    FORMAT = "parquet",
    SCHEMA =
    (
        Year Int32,
        Manufacturer Utf8,
        Model Utf8,
        Price Double
    )
)
```

The result of the query execution:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### Raw format {#raw}

This format allows you to read the contents of files as is, in "raw" form. Data read in this way can be processed using [YQL](../../../../yql/reference/udf/list/string) by dividing it into rows and columns.

This format allows reading the contents of files as is, in raw form. The data read in this way can be processed using [YQL](../../../../yql/reference/udf/list/string) tools, splitting into rows and columns.

The size of each of the files read in `raw` format cannot exceed the overall memory consumption limit for a single query in {{ ydb-short-name }}.

{% endnote %}

{% cut "Example query" %}

To parse the following data, where rows are separated by a semicolon and values by a comma:

```text
1997,Man_1,Model_1,3000.00;
1999,Man_2,Model_2,4900.00;
```

You can use the following query:

```yql
$input = SELECT
    String::SplitToList(        -- splitting each row by ','
        String::Strip(RowData), -- removing all whitespace characters from the beginning and end of the rows
        ","
    ) AS Row
FROM external_source.path
WITH
(
    FORMAT = "raw",
    SCHEMA =
    (
        FileData Utf8
    )
)
FLATTEN LIST BY (String::SplitToList(FileData, ";", TRUE AS SkipEmpty) AS RowData); -- splitting the file by ';'

SELECT -- Getting the required columns
    CAST(Row[0] AS Int32) AS Year,
    Row[1] AS Manufacturer,
    Row[2] AS Model,
    CAST(Row[3] AS Double) AS Price
FROM $input
```

The result of the query execution:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

## Supported compression algorithms {#compression}

The use of compression algorithms depends on the file formats. For all file formats except Parquet, the following compression algorithms can be used:

| Algorithm | Name in {{ydb-full-name}} | Reading | Writing |
|----|-----|------|------|
| [Gzip](https://en.wikipedia.org/wiki/Gzip) | gzip | ✓ | ✓ |
| [Zstd](https://en.wikipedia.org/wiki/Zstandard) | zstd | ✓ | ✓ |
| [LZ4](https://en.wikipedia.org/wiki/LZ4) | lz4 | ✓ | ✓ |
| [Brotli](https://en.wikipedia.org/wiki/Brotli) | brotli | ✓ | ✓ |
| [Bzip2](https://en.wikipedia.org/wiki/Bzip2) | bzip2 | ✓ | ✓ |
| [Xz](https://en.wikipedia.org/wiki/XZ) | xz | ✓ | ✓ |

For Parquet file format, native internal compression algorithms are supported:

| Compression format | Read | Write |
|-------------|------|------|
| None | ✓ | |
| [Snappy](https://en.wikipedia.org/wiki/Snappy_(library)) | ✓ | ✓ |
| [Gzip](https://en.wikipedia.org/wiki/Gzip) | ✓ | |
| [Brotli](https://en.wikipedia.org/wiki/Brotli) | ✓ | |
| [LZ4](https://en.wikipedia.org/wiki/LZ4) | ✓ | |
| [Zstd](https://en.wikipedia.org/wiki/Zstandard) | ✓ | |
| LZ4_RAW | ✓ | |

In {{ydb-full-name}}, working with externally compressed Parquet files, such as files of the form `<myfile>.parquet.gz` or similar, is not supported. All Parquet files must be without external compression.

## Supported data types {#types}

A table of all supported types when reading from S3 in the [query schema](external_data_source.md#schema):

| Type | csv | csv_with_names | tsv_with_names | json_list | json_each_row | json_as_string | parquet | raw |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `Bool`,<br/>`Int8`, `Int16`, `Int32`, `Int64`,<br/>`Uint8`, `Uint16`, `Uint32`, `Uint64`,<br/>`Float`, `Double` | ✓ | ✓ | ✓ | ✓ | ✓ | | ✓ | |
| `DyNumber` | | | | ✓ | | | | |
| `String`, `Utf8`, `Json` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `JsonDocument` | | | | | | | ✓ | |
| `Yson` | | | | ✓ | | | ✓ | ✓ |
| `Uuid` | ✓ | ✓ | ✓ | | ✓ | | | |
| `Date`, `Datetime`, `Timestamp`,<br/>`TzDate`, `TzDateTime`, `TzTimestamp` | ✓ | ✓ | ✓ | | ✓ | | ✓ | |
| `Interval` | ✓ | ✓ | ✓ | | ✓ | | ✓ | |
| `Date32`, `Datetime64`, `Timestamp64`,<br/>`Interval64`,<br/>`TzDate32`, `TzDateTime64`, `TzTimestamp64` | | | | | | | ✓ | |
| `Optional<T>` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |

The table of all supported types for writing to S3:

| Type | csv_with_names | tsv_with_names | json_list | json_each_row | parquet | raw |
| --- | --- | --- | --- | --- | --- | --- |
| `Bool`,<br/>`Int8`, `Int16`, `Int32`, `Int64`,<br/>`Uint8`, `Uint16`, `Uint32`, `Uint64`,<br/>`Float`, `Double` | ✓ | ✓ | ✓ | ✓ | ✓ | |
| `DyNumber` | | | ✓ | | | |
| `String`, `Utf8`, `Json` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `JsonDocument` | | | | | | |
| `Yson` | | | ✓ | | | ✓ |
| `Uuid` | ✓ | ✓ | | ✓ | | |
| `Date`, `Datetime`, `Timestamp`,<br/>`TzDate`, `TzDateTime`, `TzTimestamp` | ✓ | ✓ | | ✓ | ✓ | |
| `Interval` | | | | | | |
| `Date32`, `Datetime64`, `Timestamp64`,<br/>`Interval64`,<br/>`TzDate32`, `TzDateTime64`, `TzTimestamp64` | | | | | | |
| `Optional<T>` | ✓ | ✓ | ✓ | ✓ | ✓ | |

For all S3 read and write formats, except `json_list`, the `Optional<T>` type can be used only if `T` is a [primitive YQL type](../../../../yql/reference/types/primitive.md). For more information about optional types, see the article [{#T}](../../../../yql/reference/types/optional.md).