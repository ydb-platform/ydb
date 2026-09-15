# Executing parameterized queries

## Overview

{{ ydb-short-name }} CLI supports executing parameterized queries. To work with parameters, the query text must contain their definitions using the [YQL `DECLARE` command](../../yql/reference/syntax/declare.md).

The main tool for executing parameterized queries in {{ ydb-short-name }} CLI is the [{{ ydb-cli }} sql](sql.md) command.

## Why use parameterized queries?

Using parameterized queries provides several important benefits:

* **Improved performance:** Parameterized queries significantly improve performance when executing many similar operations that differ only in input values. This is due to query plan caching. After the first compilation of a query, its plan is stored in the [LRU cache](https://en.wikipedia.org/wiki/Cache_replacement_policies) of the [{{ ydb-short-name }} node](../../concepts/glossary.md#node). All subsequent calls of the same query with new parameters skip the compilation stage and start executing immediately, saving time and resources.
* **Protection against SQL injection:** Another important advantage of using parameterized queries is protection against [SQL injection](https://en.wikipedia.org/wiki/SQL_injection). This security feature ensures proper handling of input data, reducing the risk of malicious code execution.

## Single query execution {#one-request}

This command supports passing parameters via command-line options, a file, and also via `stdin`. When passing parameters via `stdin` or a file, repeated streaming execution of the query with different parameter values and batching capability is supported. For this purpose, the [{{ ydb-cli }} sql](sql.md) command provides the following parameters:

| Name | Description |
| --- | --- |
| `-p, --param` | Value of a single query parameter in `$name=value` or `name=value` format, where `name` is the parameter name and `value` is its value (a valid [JSON value](https://www.json.org/index.html)). |
| `--input-file` | Name of a file in [JSON](https://en.wikipedia.org/wiki/JSON) format encoded in [UTF-8](https://en.wikipedia.org/wiki/UTF-8), which contains parameter values mapped to query parameters by key names. At most one parameter file can be used. |
| `--input-format` | Format for representing parameter values. Applies to all methods of passing them (via a command parameter, file, or `stdin`).<br/>Possible values:<ul><li>`json` (default): [JSON](https://en.wikipedia.org/wiki/JSON) format.</li><li>`csv` — [CSV](https://en.wikipedia.org/wiki/CSV) format. By default, parameter names must be in the CSV file header. For a single query execution, only one row is allowed in the file, not counting the header.</li><li>`tsv` — [TSV](https://en.wikipedia.org/wiki/TSV) format.</li><li>`raw`: The input stream from `stdin` or `--input-file` contains only the parameter value as binary data. The parameter name must be specified with the `--input-param-name` option.</li></ul> |
| `--input-binary-strings` | Encoding format for parameter values of the 'binary string' type (`DECLARE $par AS String`). Specifies how binary strings from the input stream should be interpreted.<br/>Possible values:<ul><li>`unicode`: Each byte in a binary string that is not a printable ASCII character (codes 32-126) must be encoded in [UTF-8](https://en.wikipedia.org/wiki/UTF-8).</li><li>`base64`: Binary strings are represented in [Base64](https://en.wikipedia.org/wiki/Base64) encoding. This allows passing binary data, which {{ ydb-short-name }} CLI will decode from Base64.</li></ul> |

If values are specified for all parameters that do not allow `NULL` (i.e., with the NOT NULL type), [as part of the `DECLARE` statement](../../yql/reference/syntax/declare.md), the query will be executed on the server. If a value is missing for at least one such parameter, the command will fail with an error and the message "Parameter value not set".

### More specific options for using input parameters {#specific-param-options}

The following options are not shown in the `--help` output. Their description can only be seen in the `-hh` output.

| Name | Description |
| --- | --- |
| `--input-framing` | Sets the framing for the input stream (`stdin` or `--input-file`). Determines how the input stream is split into separate parameter sets.<br/>Possible values:<br/><ul><li>`no-framing` (default): One parameter set is expected from the input stream, the query is executed once.</li><li>`newline-delimited`: A newline character marks the end of one parameter set in the input stream, separating it from the next. A parameter set is considered complete each time a newline character is received in the input stream.</li></ul> |
| `--input-param-name` | The name of the parameter whose value is passed to the input stream. Specified without the `$` character. Required when using the `raw` format in `--input-format`.<br/><br/>When used with the JSON format, the input stream is interpreted not as a JSON document, but as a JSON value, with the value passed to the parameter with the specified name. |
| `--input-columns` | A string with column names replacing the header of the CSV/TSV document read from the input stream. When this option is specified, the header is considered absent. The option is only valid with CSV and TSV formats. |
| `--input-skip-rows` | The number of lines from the beginning of the data read from stdin to skip, not including the header line if present. The option is only valid with stdin formats CSV and TSV. |
| `--input-batch` | Batching mode for parameter set values received from the input stream (`stdin` or `--input-file`).<br/>Possible values:<br/><ul><li>`iterative` (default): Batching is [disabled](#streaming-iterate). The query is executed for each parameter set (exactly once if option `--input-framing` uses `no-framing`)</li><li>`full`: Full batch. The query is executed once after reading the input stream is complete; all received parameter sets are wrapped into `List<...>`, the parameter name is set by option `--input-param-name`</li><li>`adaptive`: Adaptive batching. The query is executed each time a limit on the number of parameter sets in a single query (`--input-batch-max-rows`) or a processing delay limit (`--input-batch-max-delay`) is triggered. All parameter sets received up to that point are wrapped into `List<...>`, the parameter name is set by option `--input-param-name`.</li></ul> |
| `--input-batch-max-rows` | Maximum number of parameter sets in a batch for adaptive batching mode. The next batch will be sent for execution together with the query if the number of data sets in it reaches the specified value. Setting to `0` removes the limit.<br/><br/>Default value — `1000`.<br/><br/>Parameters are passed to the query without streaming, and the total size of a single gRPC request that includes parameter values has an upper limit of about 5 MB. |
| `--input-batch-max-delay` | Maximum delay for sending a received parameter set for processing in adaptive batching mode. Specified as a number with a time dimension - `s` (seconds), `ms` (milliseconds), `m` (minutes), etc. Default value — `1s` (1 second).<br/><br/>{{ ydb-short-name }} CLI will count time from the moment the first parameter set for a batch is received and will send the accumulated batch for execution as soon as the time exceeds the specified value. This parameter allows efficient batching in case of an unpredictable rate of new parameter sets arriving at `stdin`. |

### Examples {#examples-one-request}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

#### Passing a single parameter value {#example-simple}

On the command line, via the `--param` option:

```bash
{{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --param '$a=10'
```

Via a file in JSON format (which is used by default):

```bash
echo '{"a":10}' > p1.json
{{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --input-file p1.json
```

Via `stdin`, passing a JSON string as a single parameter set:

```bash
echo '{"a":10}' | {{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a'
```

Via `stdin`, passing only the parameter value and specifying the parameter name via the `--input-param-name` option:

```bash
echo '10' | {{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --input-param-name a
```

#### Passing parameter values of different types from multiple sources {#example-multisource}

```bash
# Create a JSON file with fields 'a', 'b', and 'x', where 'x' will be ignored in the query
echo '{ "a":10, "b":"Some text", "x":"Ignore me" }' > p1.json

# Run the query using ydb-cli, passing in 'a' and 'b' from the input file, and 'c' as a direct parameter
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS Int64;
      DECLARE $b AS Utf8;
      DECLARE $c AS Int64;

      SELECT $a, $b, $c' \
  --input-file p1.json \
  --param '$c=30'
```

Command output:

```text
┌─────────┬─────────────┬─────────┐
│ column0 │ column1     │ column2 │
├─────────┼─────────────┼─────────┤
│ 10      │ "Some text" │ 30      │
└─────────┴─────────────┴─────────┘
```

#### Passing binary strings in Base64 encoding {#example-base64}

```bash
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS String;
      SELECT $a' \
  --input-format json \
  --input-binary-strings base64 \
  --param '$a="SGVsbG8sIHdvcmxkCg=="'
```

Command output:

```text
┌──────────────────┐
| column0          |
├──────────────────┤
| "Hello, world\n" |
└──────────────────┘
```

#### Direct binary content passing {#example-raw}

```bash
curl -Ls http://ydb.tech/docs/en | {{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS String;
      SELECT LEN($a)' \
  --input-format raw \
  --input-param-name a
```

Command output (exact number of bytes may differ):

```text
┌─────────┐
| column0 |
├─────────┤
| 66426   |
└─────────┘
```

#### Passing a file in CSV format {#example-csv}

```bash
echo '10,Some text' | {{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS Int32;
      DECLARE $b AS String;
      SELECT $a, $b' \
  --input-format csv \
  --input-columns 'a,b'
```

Command output:

```text
┌─────────┬─────────────┐
| column0 | column1     |
├─────────┼─────────────┤
| 10      | "Some text" |
└─────────┴─────────────┘
```

## Iterative streaming processing {#streaming-iterate}

{{ ydb-short-name }} CLI supports the ability to execute a query multiple times with different sets of parameter values, when they are passed via `stdin` **or** an input file (not simultaneously). In this case, the database connection is established once, and the query execution plan is cached, which significantly improves the performance of this approach compared to separate CLI invocations.

To use this feature, you need to pass different sets of values for the same parameters one after another to the command input, telling {{ ydb-short-name }} CLI the rule by which these sets can be separated from each other.

The query is executed as many times as the number of parameter value sets received. Each set received via the input stream (`stdin` or `--input-file`) is merged with the parameter values defined via the `--param` options. Command execution will finish after the input stream ends. Each query is executed in its own transaction.

The rule for separating parameter sets from each other (framing) complements the description of the parameter representation format on the input stream, specified by the `--input--framing` parameter:

| Name | Description |
| --- | --- |
| `--input-framing` | Specifies framing for the input stream (file or `stdin`). <br/>Possible values:<ul><li>`no-framing` (default) — one parameter set is expected at the input, the query is executed once after reading the stream completes.</li><li>`newline-delimited` — a newline character marks the end of one parameter set at the input, separating it from the next. The query is executed each time a newline character is received.</li></ul> |

{% note warning %}

When using a newline character as a separator for parameter sets, you must ensure it is absent inside the parameter sets. Quoting text does not make a newline inside such text valid. Multiline JSON documents are not allowed.

{% endnote %}

### Example {#example-streaming-iterate}

#### Streaming processing of multiple parameter sets {#example-iterate}

{% list tabs %}

- JSON

  Suppose we need to execute the query three times, with the following sets of parameter values `a` and `b`:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file that will contain lines with JSON representation of these sets:

  ```bash
  echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' | tee par1.txt
  ```

  Command output:

  ```text
  {"a":10,"b":20}
  {"a":15,"b":25}
  {"a":35,"b":48}
  ```

  Execute the query by passing the contents of this file to `stdin`, with output formatting in JSON:

  ```bash
  cat par1.txt | \
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a+$b' \
    --input-framing newline-delimited \
    --format json-unicode
  ```

  Command output:

  ```text
  {"column0":30}
  {"column0":40}
  {"column0":83}
  ```

  Or simply by passing the file name to the `--input-file` option:

  ```bash
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-file par1.txt \
    --input-framing newline-delimited \
    --format json-unicode
  ```

  Command output:

  ```text
  {"column0":30}
  {"column0":40}
  {"column0":83}
  ```

  The result obtained in this format can be used as input to the command executing the next query.

- CSV

  Suppose we need to execute the query three times, with the following sets of parameter values `a` and `b`:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file with sets of parameter values in CSV format:

  ```bash
  echo -e 'a,b\n10,20\n15,25\n35,48' | tee par1.txt
  ```

  Command output:

  ```text
  a,b
  10,20
  15,25
  35,48
  ```

  Let's execute the query by passing the contents of this file to `stdin`, with output formatting in CSV:

  ```bash
  cat par1.txt | \
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-format csv \
    --input-framing newline-delimited \
    --format csv
  ```

  Command output:

  ```text
  30
  40
  83
  ```

  Or simply by passing the file name to the `--input-file` option:

  ```bash
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-file par1.txt \
    --input-format csv \
    --input-framing newline-delimited \
    --format csv
  ```

  Command output:

  ```text
  30
  40
  83
  ```

  The result obtained in this format can be used as input to the command executing the next query, by specifying the header data in CSV format with the `--input-columns` option.

- TSV

  Suppose we need to execute the query three times, with the following sets of parameter values `a` and `b`:

  1. `a` = 10, `b` = row1
  2. `a` = 15, `b` = row  2
  3. `a` = 35, `b` = "row"\n3

  Let's create a file with sets of parameter values in TSV format:

  ```bash
  echo -e 'a\tb\n10\t20\n15\t25\n35\t48' | tee par1.txt
  ```

  Command output:

  ```text
  a  b
  10 20
  15 25
  35 48
  ```

  Let's execute the query by passing the contents of this file to `stdin`, with output formatting in TSV:

  ```bash
  cat par1.txt | \
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Utf8;
        SELECT $a, $b' \
    --input-framing newline-delimited \
    --input-format tsv \
    --format tsv
  ```

  Command output:

  ```text
  30
  40
  83
  ```

  Or simply by passing the file name to the `--input-file` option:

  ```bash
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-file par1.txt \
    --input-format tsv \
    --input-framing newline-delimited \
    --format tsv
  ```

  Command output:

  ```text
  30
  40
  83
  ```

  The result obtained in this format can be used as input to the command executing the next query, by specifying the header data in TSV format with the `--input-columns` option.

{% endlist %}

#### Stream processing with merging parameter values from different sources {#example-iterate-union}

Suppose we need to execute the query three times, with the following sets of parameter values `a` and `b`:

1. `a` = 10, `b` = 100
2. `a` = 15, `b` = 100
3. `a` = 35, `b` = 100

```bash
echo -e '10\n15\n35' | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS Int64;
      DECLARE $b AS Int64;
      SELECT $a + $b AS sum1' \
  --param '$b=100' \
  --input-framing newline-delimited \
  --input-param-name a \
  --format json-unicode
```

Command output:

```text
{"sum1":110}
{"sum1":115}
{"sum1":135}
```

## Batch stream processing {#streaming-batch}

The {{ ydb-short-name }} CLI supports automatic conversion of parameter sets into `List<...>`, allowing a single server request to process multiple parameter sets in one transaction, which can further significantly improve performance compared to executing queries "one by one".

Two batching modes are supported:

- Full (`full`)
- Adaptive (`adaptive`)

### Full batching mode {#batch-full}

The full (`full`) mode is a simplified batching option where the query is executed once, wrapping all parameter sets received from the input stream into `List<...>`. If the query size turns out to be too large, an error will be raised.

This batching option is necessary when atomicity must be guaranteed by applying all parameters in a single transaction.

### Adaptive batching mode {#batch-adaptive}

When operating in adaptive (`adaptive`) mode, the input stream processing is split into multiple transactions, with automatic selection of the batch size for each of them.

This mode allows efficient processing of a wide range of input loads, with unpredictable or infinite amounts of data, as well as unpredictable or rapidly changing input rates. In particular, this profile is typical when feeding the output of another command to `stdin` via the `|` operator.

Adaptive mode solves two main problems of dynamic stream processing:

1. Limiting the maximum batch size.
2. Limiting the maximum data processing delay.

### Syntax {#batch-syntax}

To use the batching feature, you need to declare a parameter of type `List<...>` or `List<Struct<...>>` in the DECLARE section of the query, and select the mode with the following parameter:

| Name | Description |
| --- | --- |
| `--input-batch` | Mode for batching values of parameter sets obtained from the input stream (`stdin` or `--input-file`).<br/>Possible values:<br/><ul><li>`iterative` (default): Batching is [disabled](#streaming-iterate). The query is executed for each parameter set (exactly once if the `--input-framing` option uses `no-framing`)</li><li>`full`: Full batch. The query will run once after the input stream is read, all received parameter sets are wrapped in `List<...>`, the parameter name is set by the `--input-param-name` option</li><li>`adaptive`: Adaptive batching. The query is executed each time the limit on the number of parameter sets in one query (`--input-batch-max-rows`) or the processing delay (`--input-batch-max-delay`) is triggered. All parameter sets received by this point are wrapped in `List<...>`, the parameter name is set by the `--input-param-name` option.</li></ul> |

Additional parameters are available in adaptive batching mode:

| Name | Description |
| --- | --- |
| `--input-batch-max-rows` | Maximum number of parameter sets in a batch for adaptive batching mode. The next batch will be sent for execution together with the query if the number of data sets in it has reached the specified value. Setting to `0` removes the limit.<br/><br/>Default value — `1000`.<br/><br/>Parameters are passed to the query without streaming, and the total size of a single gRPC request that includes parameter values has an upper limit of about 5 MB. |
| `--input-batch-max-delay` | Maximum delay for sending a received parameter set for processing in adaptive batching mode. Specified as a number with a time dimension - `s` (seconds), `ms` (milliseconds), `m` (minutes), etc. Default value — `1s` (1 second).<br/><br/>The {{ ydb-short-name }} CLI will count time from the moment the first parameter set for a batch is received and will send the accumulated batch for execution as soon as the time exceeds the specified value. This parameter allows efficient batching in case of an unpredictable rate of new parameter sets arriving at `stdin`. |

### Examples - full batch processing {#example-batch-full}

```bash
echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $x AS List<Struct<a:Int64,b:Int64>>;
      SELECT ListLength($x), $x' \
  --input-framing newline-delimited \
  --input-param-name x \
  --input-batch full
```

Command output:

```text
┌─────────┬───────────────────────────────────────────────────┐
| column0 | column1                                           |
├─────────┼───────────────────────────────────────────────────┤
| 3       | [{"a":10,"b":20},{"a":15,"b":25},{"a":35,"b":48}] |
└─────────┴───────────────────────────────────────────────────┘
```

### Examples - adaptive batch processing {#example-batch-adaptive}

#### Limiting the maximum processing delay {#example-adaptive-delay}

To demonstrate adaptive batching with the processing delay limit trigger, the first line of the command below generates 1000 lines with a 0.2 second delay in `stdout`, which are passed to the `stdin` query execution command. The query execution command, in turn, displays parameter batches in each subsequent query call.

```bash
for i in $(seq 1 1000); do echo "Line$i"; sleep 0.2; done | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $x AS List<Utf8>;
      SELECT ListLength($x), $x' \
  --input-framing newline-delimited \
  --input-format raw \
  --input-param-name x \
  --input-batch adaptive
```

Command output (exact values may differ):

```text
┌─────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
| column0 | column1                                                                                                                |
├─────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
| 14      | ["Line1","Line2","Line3","Line4","Line5","Line6","Line7","Line8","Line9","Line10","Line11","Line12","Line13","Line14"] |
└─────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
┌─────────┬─────────────────────────────────────────────────────────┐
| column0 | column1                                                 |
├─────────┼─────────────────────────────────────────────────────────┤
| 6       | ["Line15","Line16","Line17","Line18","Line19","Line20"] |
└─────────┴─────────────────────────────────────────────────────────┘
┌─────────┬─────────────────────────────────────────────────────────┐
| column0 | column1                                                 |
├─────────┼─────────────────────────────────────────────────────────┤
| 6       | ["Line21","Line22","Line23","Line24","Line25","Line26"] |
└─────────┴─────────────────────────────────────────────────────────┘
^C
```

The first batch includes all lines accumulated at the input during the database connection opening time, and therefore it is larger than subsequent ones.

You can interrupt the command execution with Ctrl+C, or wait until 200 seconds have passed, during which the input is generated.

#### Limiting by number of records {#example-adaptive-limit}

To demonstrate adaptive batching with the trigger by the number of parameter sets, the first line of the command below generates 200 lines. The command will display parameter batches in each subsequent query call, taking into account the specified limit `--input-batch-max-rows` equal to 20 (default 1000).

This example also shows the ability to combine parameters from different sources and generate JSON output.

```bash
for i in $(seq 1 200); do echo "Line$i"; done | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $x AS List<Utf8>;
      DECLARE $p2 AS Int64;
      SELECT ListLength($x) AS count, $p2 AS p2, $x AS items' \
  --input-framing newline-delimited \
  --input-format raw \
  --input-param-name x \
  --input-batch adaptive \
  --input-batch-max-rows 20 \
  --param '$p2=10' \
  --format json-unicode
```

Command output:

```text
{"count":20,"p2":10,"items":["Line1","Line2","Line3","Line4","Line5","Line6","Line7","Line8","Line9","Line10","Line11","Line12","Line13","Line14","Line15","Line16","Line17","Line18","Line19","Line20"]}
{"count":20,"p2":10,"items":["Line21","Line22","Line23","Line24","Line25","Line26","Line27","Line28","Line29","Line30","Line31","Line32","Line33","Line34","Line35","Line36","Line37","Line38","Line39","Line40"]}
...
{"count":20,"p2":10,"items":["Line161","Line162","Line163","Line164","Line165","Line166","Line167","Line168","Line169","Line170","Line171","Line172","Line173","Line174","Line175","Line176","Line177","Line178","Line179","Line180"]}
{"count":20,"p2":10,"items":["Line181","Line182","Line183","Line184","Line185","Line186","Line187","Line188","Line189","Line190","Line191","Line192","Line193","Line194","Line195","Line196","Line197","Line198","Line199","Line200"]}
```

#### Deleting multiple records from a row table {{ ydb-short-name }} by primary keys {#example-adaptive-delete-pk}

If you try to delete a large number of rows from a large table using a simple `DELETE FROM large_table WHERE id > 10;` query, you may encounter an error due to exceeding the limit on the number of records in a transaction. This example shows how to delete an unlimited number of records from {{ ydb-short-name }} tables without violating this limit.
Let's create a test row table:

```bash
{{ ydb-cli }} -p quickstart sql -s 'CREATE TABLE test_delete_1(id UInt64 NOT NULL, PRIMARY KEY (id))'
```

Let's insert 100,000 records into it:

```bash
for i in $(seq 1 100000); do echo "$i";done | \
{{ ydb-cli }} -p quickstart import file csv -p test_delete_1
```

Let's delete all records with `id` values greater than 10:

```bash
{{ ydb-cli }} -p quickstart sql \
  -s 'SELECT t.id FROM test_delete_1 AS t WHERE t.id > 10' \
  --format json-unicode | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $lines AS List<Struct<id:UInt64>>;
      DELETE FROM test_delete_1 WHERE id IN (SELECT tl.id FROM AS_TABLE($lines) AS tl)' \
  --input-framing newline-delimited \
  --input-param-name lines \
  --input-batch adaptive \
  --input-batch-max-rows 10000
```

#### Processing messages read from a topic {#example-adaptive-pipeline-from-topic}

Examples of processing messages read from a topic are given in the article [{#T}](topic-pipeline.md#example-read-to-yql-param).

## See also {#see-also}

* [Parameterized queries in {{ ydb-short-name }} SDK](../ydb-sdk/parameterized_queries.md)
