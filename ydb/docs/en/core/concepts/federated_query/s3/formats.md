# Data formats and compression algorithms

This section describes the data formats supported in {{ ydb-full-name }} for storage in S3 and the supported compression algorithms.

## Supported data formats {#formats}

The table below lists the data formats supported in {{ ydb-short-name }}.

|Format|Read|Write|
|----|-----|------|
|[`csv_with_names`](#csv_with_names)|✓|✓|
|[`tsv_with_names`](#tsv_with_names)|✓||
|[`json_list`](#json_list)|✓||
|[`json_each_row`](#json_each_row)|✓||
|[`json_as_string`](#json_as_string)|✓||
|[`parquet`](#parquet)|✓|✓|
|[`raw`](#raw)|✓||

### Format csv_with_names {#csv_with_names}
This format is based on the [CSV](https://en.wikipedia.org/wiki/CSV) format. Data is placed in columns separated by commas, with column names in the file's first row.

Example data:
```
Year,Manufacturer,Model,Price
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```

{% cut "Example query" %}

```sql
SELECT
    AVG(Price)
FROM `connection`.`path`
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

Query result:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### Format tsv_with_names {#tsv_with_names}

This format is based on the [`TSV`](https://en.wikipedia.org/wiki/Tab-separated_values) format. Data is placed in columns separated by tab characters (code `0x9`), with column names in the file's first row.

Example data:
```
Year    Manufacturer    Model   Price
1997    Man_1   Model_1    3000.00
1999    Man_2   Model_2    4900.00
```

{% cut "Example query" %}

```sql
SELECT
    AVG(Price)
FROM `connection`.`path`
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

Query result:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### Format json_list {#json_list}
This format is based on the [JSON representation](https://en.wikipedia.org/wiki/JSON) of data. Each file must contain an array of JSON objects.

Example of valid data (data presented as a list of JSON objects):
```json
[
    { "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 },
    { "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
]
```

Example of **INVALID** data (each line contains a separate JSON object, but they are not combined into a list):
```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

### Format json_each_row {#json_each_row}

This format is based on the [JSON representation](https://en.wikipedia.org/wiki/JSON) of data. Each file must contain a JSON object on each line without combining them into a JSON array. This format is used for data streaming systems like Apache Kafka or [{{ydb-full-name}} Topics](../../topic.md).

Example of valid data (each line contains a separate JSON object):
```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

{% cut "Example query" %}

```sql
SELECT
    AVG(Price)
FROM `connection`.`path`
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

Query result:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### Format json_as_string {#json_as_string}
This format is based on the [JSON representation](https://en.wikipedia.org/wiki/JSON) of data. The `json_as_string` format does not split the input JSON document into fields but represents each file line as a single JSON object (or string). This format is helpful when the list of fields is not the same in all rows and may vary.

Each file must contain:
- a JSON object on each line, or
- JSON objects combined into an array.

Example of valid data (data presented as a list of JSON objects):
```json
{ "Year": 1997, "Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000.0 }
{ "Year": 1999, "Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900.00 }
```

{% cut "Example query" %}

```sql
SELECT
    *
FROM `connection`.`path`
WITH
(
    FORMAT = "json_as_string",
    SCHEMA =
    (
        Data Json
    )
)
```

Query result:

|#|Data|
|-|-|
|1|`{"Manufacturer": "Man_1", "Model": "Model_1", "Price": 3000, "Year": 1997}`|
|2|`{"Manufacturer": "Man_2", "Model": "Model_2", "Price": 4900, "Year": 1999}`|

{% endcut %}

### Format parquet {#parquet}

This format allows reading the contents of files in [Apache Parquet](https://parquet.apache.org) format.

Supported data compression algorithms for Parquet files:

- Uncompressed
- SNAPPY
- GZIP
- LZO
- BROTLI
- LZ4
- ZSTD
- LZ4_RAW

{% cut "Example query" %}

```sql
SELECT
    AVG(Price)
FROM `connection`.`path`
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

Query result:

|#|Manufacturer|Model|Price|Year|
|-|-|-|-|-|
|1|Man_1|Model_1|3000|1997|
|2|Man_2|Model_2|4900|1999|

{% endcut %}

### Format raw {#raw}

This format allows reading the contents of files as is, in raw form. The data read in this way can be processed using [YQL](../../../yql/reference/udf/list/string) tools, splitting into rows and columns.

This format should be used if the built-in parsing capabilities in {{ ydb-full-name }} are insufficient.

{% cut "Example query" %}

```sql
SELECT
    *
FROM `connection`.`path`
WITH
(
    FORMAT = "raw",
    SCHEMA =
    (
        Data String
    )
)
```

Query result:

```
Year,Manufacturer,Model,Price
1997,Man_1,Model_1,3000.00
1999,Man_2,Model_2,4900.00
```

{% endcut %}

## Supported compression algorithms {#compression}

The use of compression algorithms depends on the file formats. For all file formats except Parquet, the following compression algorithms can be used:

|Algorithm|Name in {{ydb-full-name}}|Read|Write|
|----|-----|------|------|
|[Gzip](https://en.wikipedia.org/wiki/Gzip)|gzip|✓|✓|
|[Zstd](https://en.wikipedia.org/wiki/Zstandard)|zstd|✓||
|[LZ4](https://en.wikipedia.org/wiki/LZ4)|lz4|✓|✓|
|[Brotli](https://en.wikipedia.org/wiki/Brotli)|brotli|✓||
|[Bzip2](https://en.wikipedia.org/wiki/Bzip2)|bzip2|✓||
|[Xz](https://en.wikipedia.org/wiki/XZ)|xz|✓||

For Parquet file format, the following internal compression algorithms are supported:

|Compression format|Name in {{ ydb-full-name }}|Read|Write|
|--|--|----|-----|
|[Raw](https://en.wikipedia.org/wiki/Gzip)|raw|✓||
|[Snappy](https://en.wikipedia.org/wiki/Snappy_(compression))|snappy|✓|✓|

{{ydb-full-name}} does not support working with externally compressed Parquet files, such as files named "<myfile>.parquet.gz" or similar. All files in Parquet format must be without external compression.