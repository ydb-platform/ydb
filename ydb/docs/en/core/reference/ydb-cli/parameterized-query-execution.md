# Executing Parameterized Queries

## Overview

{{ ydb-short-name }} CLI supports executing parameterized queries. To work with parameters, their definitions must be present in the query text using the [YQL command `DECLARE`](../../yql/reference/syntax/declare.md).

The main tool for executing parameterized queries in {{ ydb-short-name }} CLI is the [{{ ydb-cli }} sql](sql.md) command.

## Why Use Parameterized Queries?

Using parameterized queries provides several important advantages:

* **Improved Performance:** Parameterized queries significantly boost performance when executing multiple similar operations that differ only in input values. This is achieved through query plan caching. After the first compilation of a query, its plan is stored in the [LRU cache](https://en.wikipedia.org/wiki/Cache_replacement_policies) of the [node of {{ ydb-short-name }}](../../concepts/glossary.md#node). All subsequent calls of the same query with new parameters skip the compilation stage and start executing immediately, saving time and resources.

* **Protection Against SQL Injection:** Another important advantage of using parameterized queries is protection against [SQL injection](https://en.wikipedia.org/wiki/SQL_injection). This security feature ensures proper handling of input data, reducing the risk of malicious code execution.

## Single Query Execution {#one-request}

This command supports passing parameters via command-line options, a file, and also via `stdin`. When passing parameters via `stdin` or a file, multiple streaming executions of the query with different parameter values and batching capability are supported. For this purpose, the [{{ ydb-cli }} sql](sql.md) command provides the following parameters:

| Name | Description |
| ---|--- |
| `-p, --param`| Value of a single query parameter in the format`$name=value `or`name=value`, where ` name`— the parameter name, and`value` — its value (a valid [JSON value](https://www.json.org/json-ru.html)). |
| `--input-file` | File name in the format [JSON](https://en.wikipedia.org/wiki/JSON) in encoding [UTF-8](https://en.wikipedia.org/wiki/UTF-8), which contains parameter values mapped to query parameters by key names. At most one parameter file can be used. |
| `--input-format`| Format for representing parameter values. Applies to all methods of passing them (via command parameter, file, or`stdin`).<br/>Possible values:<ul><li>`json` (default): Format [JSON](https://en.wikipedia.org/wiki/JSON).</li><li>`csv` — format [CSV](https://en.wikipedia.org/wiki/CSV). By default, parameter names must be in the CSV file header. For a single query execution, only one row is allowed in the file, excluding the header.</li><li>`tsv` — format [TSV](https://en.wikipedia.org/wiki/TSV).</li><li>`raw`: Input stream from `stdin`or`--input-file`contains only the parameter value as binary data. The parameter name must be specified with the option`--input-param-name`.</li></ul> |
| `--input-binary-strings` | Encoding format for parameter values of type "binary string" (`DECLARE $par AS String`). It specifies how binary strings from the input stream should be interpreted.<br/>Possible values:<ul><li>`unicode`: Each byte in the binary string that is not a printable ASCII character (codes 32-126) must be encoded in the encoding [UTF-8](https://en.wikipedia.org/wiki/UTF-8).</li><li>`base64`: Binary strings are represented in the encoding [Base64](https://en.wikipedia.org/wiki/Base64). This feature allows transferring binary data, which will be decoded from Base64 by {{ ydb-short-name }} CLI.</li></ul> |

If values are specified for all parameters that do not allow `NULL`(i.e., with the NOT NULL type), [as part of the `DECLARE`](../../yql/reference/syntax/declare.md) statement, the query will be executed on the server. If a value is missing for at least one such parameter, the command execution will fail with the error message "Parameter value not set".

## Iterative streaming processing {#streaming-iterate}

{{ ydb-short-name }} CLI supports executing a query multiple times with different sets of parameter values, passed via `stdin` **or** an input file (not simultaneously). In this case, the database connection is established once, and the query execution plan is cached, which significantly improves the performance of this approach compared to separate CLI invocations.

To use this feature, you need to pass different sets of values for the same parameters one after another to the command input, telling {{ ydb-short-name }} CLI the rule by which these sets can be separated from each other.

The query is executed as many times as the number of parameter value sets received. Each set received via the input stream (`stdin`or`--input-file`) is merged with the parameter values defined via the `--param` options. The command execution will finish after the input stream ends. Each query is executed in its own transaction.

The rule for separating parameter sets from each other (framing) complements the description of the parameter representation format in the input stream, specified by the `--input-framing` parameter:

| Name | Description |[disabled](#streaming-iterate)
| ---|--- |
| `--input-framing`| Sets framing for the input stream (file or`stdin`). <br/>Possible values:<ul><li>`no-framing` (default) — one parameter set is expected on input, the query is executed once after reading the stream is complete.</li><li>`newline-delimited` — newline character marks the end of one parameter set on input, separating it from the next. The query is executed each time a newline character is received.</li></ul> |

### More specific options for using input parameters {#specific-param-options}

The following options are not shown in the `--help`output. Their description can only be seen in the`-hh` output.[ydb-cli-profile](../../_includes/ydb-cli-profile.md)

| Name | Description |
| ---|--- |
| `--input-framing` | Sets framing for the input stream (`stdin`or`--input-file`). Determines how the input stream is split into individual parameter sets.<br/>Possible values:<br/><ul><li>`no-framing` (default): The input stream is expected to contain one parameter set; the query is executed once.</li><li>`newline-delimited`: A newline character marks the end of one parameter set in the input stream, separating it from the next. A parameter set is considered complete each time a newline character is received in the input stream.</li></ul> |
| `--input-param-name`| Parameter name whose value is passed in the input stream. Specified without the `$`character. Required when using the`raw` in`--input-format`.<br/><br/>When used with JSON format, the input stream is interpreted not as a JSON document but as a JSON value, passing the value to the parameter with the specified name. |
| `--input-columns` | String with column names replacing the header of the CSV/TSV document read from the input stream. When this option is specified, the header is considered absent. The option is only valid with CSV and TSV formats.|
| `--input-skip-rows` | Number of lines from the beginning of the data read from stdin to skip, not including the header line if present. The option is only valid with stdin CSV and TSV formats. |
| `--input-batch` | Batch mode for parameter value sets obtained from the input stream (`stdin`or`--input-file`).<br/>Possible values:<br/><ul><li>`iterative`(default): Batching is [disabled](#streaming-iterate). The query is executed for each parameter set (exactly once if`no-framing`is specified for`--input-framing`).</li><li>`full`: Full batch. The query runs once after reading the input stream ends, all received parameter sets are wrapped into `List<...>`, the parameter name is set with the `--input-param-name` option.</li><li>`adaptive`: Adaptive batching. The query runs each time the limit on the number of parameter sets in one request (`--input-batch-max-rows`) or the processing delay (`--input-batch-max-delay`) is triggered. All parameter sets received up to that point are wrapped into `List<...>`, the parameter name is set with the `--input-param-name` option.</li></ul> |
| `--input-batch-max-rows`| Maximum number of parameter sets in a batch for adaptive batching mode. The next batch will be sent for execution together with the query if the number of data sets in it reaches the specified value. Setting to `0` removes the limit.<br/><br/>Default value is`1000`.<br/><br/>Parameters are passed to the query without streaming, and the total size of a single gRPC request that includes parameter values has an upper limit of about 5 MB. |
| `--input-batch-max-delay`| Maximum delay before sending a received parameter set for processing in adaptive batching mode. Specified as a number with a time unit -`s`(seconds),`ms`(milliseconds),`m`(minutes), etc. Default value is`1s`(1 second).<br/><br/>{{ ydb-short-name }} The CLI will start counting time from the moment the first parameter set for a batch is received and will send the accumulated batch for execution as soon as the time exceeds the specified value. This parameter enables efficient batching when the arrival rate of new parameter sets is unpredictable on`stdin`. |[disabled](#streaming-iterate)

### Examples {#examples-one-request}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

#### Passing a single parameter value {#example-simple}

In the command line, using the option `--param`:

```bash
{{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --param '$a=10'
```
echo '{"a":10}' > p1.json
{{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --input-file p1.json
```bash
echo '{"a":10}' | {{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a'
```
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
```bash
┌─────────┬─────────────┬─────────┐
│ column0 │ column1     │ column2 │
├─────────┼─────────────┼─────────┤
│ 10      │ "Some text" │ 30      │
└─────────┴─────────────┴─────────┘
```
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS String;
      SELECT $a' \
  --input-format json \
  --input-binary-strings base64 \
  --param '$a="SGVsbG8sIHdvcmxkCg=="'
```bash
┌──────────────────┐
| column0          |
├──────────────────┤
| "Hello, world\n" |
└──────────────────┘
```
curl -Ls http://ydb.tech/docs/en | {{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS String;
      SELECT LEN($a)' \
  --input-format raw \
  --input-param-name a
```
┌─────────┐
| column0 |
├─────────┤
| 66426   |
└─────────┘
```
echo '10,Some text' | {{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS Int32;
      DECLARE $b AS String;
      SELECT $a, $b' \
  --input-format csv \
  --input-columns 'a,b'
```text
┌─────────┬─────────────┐
| column0 | column1     |
├─────────┼─────────────┤
| 10      | "Some text" |
└─────────┴─────────────┘
```

## Iterative streaming processing {#streaming-iterate}

{{ ydb-short-name }} CLI supports executing a query multiple times with different sets of parameter values, passed via `stdin` **or** an input file (not simultaneously). In this case, the database connection is established once, and the query execution plan is cached, which significantly improves the performance of this approach compared to separate CLI invocations.

To use this feature, you need to pass different sets of values for the same parameters one after another to the command input, telling {{ ydb-short-name }} CLI the rule by which these sets can be separated from each other.

The query is executed as many times as the number of parameter value sets received. Each set received via the input stream (`stdin`or`--input-file`) is merged with the parameter values defined via the `--param` options. The command execution will finish after the input stream ends. Each query is executed in its own transaction.

The rule for separating parameter sets from each other (framing) complements the description of the parameter representation format in the input stream, specified by the `--input-framing` parameter:

| Name | Description |
| ---|--- |
| `--input-framing`| Sets framing for the input stream (file or`stdin`). <br/>Possible values:<ul><li>`no-framing` (default) — one parameter set is expected on input, the query is executed once after reading the stream is complete.</li><li>`newline-delimited` — newline character marks the end of one parameter set on input, separating it from the next. The query is executed each time a newline character is received.</li></ul> |

{% note warning %}

When using a newline character as a delimiter for parameter sets, you must ensure that it is not present inside the parameter sets. Quoting the text does not make a newline valid inside such text. Multiline JSON documents are not allowed.

{% endnote %}

### Example {#example-streaming-iterate}

#### Streaming multiple parameter sets {#example-iterate}

{% list tabs %}

- JSON

  Suppose we need to execute the query three times, with the following sets of parameter values `a `and` b`:

  1. `a `= 10,` b` = 20
  2. `a `= 15,` b` = 25
  3. `a `= 35,` b` = 48

  Let's create a file that will contain lines with the JSON representation of these sets:

  ```bash
  echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' | tee par1.txt
  ```
  {"a":10,"b":20}
  {"a":15,"b":25}
  {"a":35,"b":48}
  ```text

  ```
  cat par1.txt | \
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-framing newline-delimited \
    --format json-unicode
  ```bash
  {"column0":30}
  {"column0":40}
  {"column0":83}
  ```
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-file par1.txt \
    --input-framing newline-delimited \
    --format json-unicode
  ```text
  {"column0":30}
  {"column0":40}
  {"column0":83}
  ```

- CSV

  Suppose we need to execute the query three times, with the following sets of parameter values `a `and` b`:

  1. `a `= 10,` b` = 20
  2. `a `= 15,` b` = 25
  3. `a `= 35,` b` = 48

  Let's create a file with the sets of parameter values in CSV format:

  ```bash
  echo -e 'a,b\n10,20\n15,25\n35,48' | tee par1.txt
  ```
  a,b
  10,20
  15,25
  35,48
  ```text

  ```
  cat par1.txt | \
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-format csv \
    --input-framing newline-delimited \
    --format csv
  ```bash
  30
  40
  83
  ```
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-file par1.txt \
    --input-format csv \
    --input-framing newline-delimited \
    --format csv
  ```text
  30
  40
  83
  ```

  This output can be passed as input to another command running a different parameterized query.

{% endlist %}

#### Streaming processing with combining parameter values from different sources {#example-iterate-union}

Suppose we need to execute the query three times, with the following sets of parameter values `a `and` b`:

1. `a `= 10,` b` = 100
2. `a `= 15,` b` = 100
3. `a `= 35,` b`= 100```bash
echo -e '10\n15\n35' | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS Int64;
      DECLARE $b AS Int64;
      SELECT $a + $b AS sum1' \
  --param '$b=100' \
  --input-framing newline-delimited \
  --input-param-name a \
  --format json-unicode
  ```bash
{"sum1":110}
{"sum1":115}
  ```

## Batch streaming processing {#streaming-batch}

{{ ydb-short-name }} CLI supports automatic conversion of parameter sets into `List<...>`, allowing a single server request to process multiple parameter sets in one transaction, which can significantly improve performance compared to executing queries "one by one".

Two batching modes are supported:

- Full (`full`)
- Adaptive (`adaptive`)

### Full batching mode {#batch-full}

Full (`full`) mode is a simplified batching variant where the query is executed once, wrapping all parameter sets received from the input stream into `List<...>`. If the query size becomes too large, an error is returned.

This batching variant is necessary when you need to guarantee atomicity by applying all parameters in a single transaction.

### Adaptive batching mode {#batch-adaptive}

In adaptive (`adaptive`) mode, the input stream processing is split into multiple transactions, with automatic batch size selection for each transaction.

This mode efficiently handles a wide range of input workloads with unpredictable or infinite data volumes, as well as unpredictable or rapidly changing input rates. In particular, this profile is typical when feeding the output of another command to `stdin`via the`|` operator.

Adaptive mode addresses two main issues of dynamic stream processing:

1. Limiting the maximum batch size.
2. Limiting the maximum data processing delay.

### Syntax {#batch-syntax}

To use batching, you need to declare a parameter of type `List<...>`or`List<Struct<...>>` in the DECLARE section of the query, and select the mode with the following parameter:

| Name | Description |
| ---|--- |
| `--input-batch` | Batch mode for parameter value sets obtained from the input stream (`stdin`or`--input-file`).<br/>Possible values:<br/><ul><li>`iterative` (default): Batching [disabled](#streaming-iterate)<br/>|

In adaptive batching mode, additional parameters are available:

| Name | Description |
| ---|--- |
| `--input-batch-max-rows`| Maximum number of parameter sets in a batch for adaptive batching mode. The next batch will be sent for execution together with the query if the number of data sets in it reaches the specified value. Setting to `0` removes the limit.<br/><br/>Default value —`1000`.<br/><br/>Parameters are passed to the query without streaming, and the total size of a single gRPC request that includes parameter values has an upper limit of about 5 MB. |
| `--input-batch-max-delay`| Maximum delay for sending a received parameter set for processing in adaptive batching mode. Specified as a number with a time dimension -`s`(seconds),`ms`(milliseconds),`m`(minutes), etc. Default value —`1s`(1 second).<br/><br/>{{ ydb-short-name }} The CLI will count time from the moment the first parameter set for a batch is received and send the accumulated batch for execution as soon as the time exceeds the specified value. This parameter enables efficient batching in case of unpredictable rate of new parameter sets on`stdin`. |

### Examples - Full batch processing {#example-batch-full}

  ```
echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $x AS List<Struct<a:Int64,b:Int64>>;
      SELECT ListLength($x), $x' \
  --input-framing newline-delimited \
  --input-param-name x \
  --input-batch full
  ```
┌─────────┬───────────────────────────────────────────────────┐
| column0 | column1                                           |
├─────────┼───────────────────────────────────────────────────┤
| 3       | [{"a":10,"b":20},{"a":15,"b":25},{"a":35,"b":48}] |
└─────────┴───────────────────────────────────────────────────┘
  ```bash

### Examples - adaptive batch processing {#example-batch-adaptive}

#### Limiting the maximum processing delay {#example-adaptive-delay}

To demonstrate adaptive batching with the processing delay limit trigger, the first line of the command below generates 1000 rows with a 0.2-second delay in `stdout`, which are passed to the `stdin`query execution command. The query execution command, in turn, displays parameter batches in each subsequent query call.```bash
for i in $(seq 1 1000); do echo "Line$i"; sleep 0.2; done | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $x AS List<Utf8>;
      SELECT ListLength($x), $x' \
  --input-framing newline-delimited \
  --input-format raw \
  --input-param-name x \[disabled](#streaming-iterate)
  --input-batch adaptive
  ```
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

The first batch includes all rows accumulated at the input while the database connection was being opened, so it is larger than subsequent batches.

You can interrupt the command execution by pressing Ctrl+C, or wait for 200 seconds while the input is being generated.

#### Limit on the number of records {#example-adaptive-limit}

To demonstrate adaptive batching triggered by the number of parameter sets, the first line of the command below generates 200 rows. The command will display parameter batches in each subsequent query call, respecting the specified limit `--input-batch-max-rows` of 20 (default is 1000).

This example also shows the ability to combine parameters from different sources and generate JSON output.

  ```
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
  ```bash
{"count":20,"p2":10,"items":["Line1","Line2","Line3","Line4","Line5","Line6","Line7","Line8","Line9","Line10","Line11","Line12","Line13","Line14","Line15","Line16","Line17","Line18","Line19","Line20"]}
{"count":20,"p2":10,"items":["Line21","Line22","Line23","Line24","Line25","Line26","Line27","Line28","Line29","Line30","Line31","Line32","Line33","Line34","Line35","Line36","Line37","Line38","Line39","Line40"]}
...
{"count":20,"p2":10,"items":["Line161","Line162","Line163","Line164","Line165","Line166","Line167","Line168","Line169","Line170","Line171","Line172","Line173","Line174","Line175","Line176","Line177","Line178","Line179","Line180"]}
{"count":20,"p2":10,"items":["Line181","Line182","Line183","Line184","Line185","Line186","Line187","Line188","Line189","Line190","Line191","Line192","Line193","Line194","Line195","Line196","Line197","Line198","Line199","Line200"]}
  ```

#### Deleting multiple records from a string table {{ ydb-short-name }} by primary keys {#example-adaptive-delete-pk}

If you try to delete a large number of records from a large table using a simple query `DELETE FROM large_table WHERE id > 10;`, you may encounter an error due to exceeding the limit on the number of records in a transaction. This example shows how you can delete an unlimited number of records from tables {{ ydb-short-name }} without violating this limit.
Create a test string table:

  ```text
for i in $(seq 1 100000); do echo "$i";done | \
{{ ydb-cli }} -p quickstart import file csv -p test_delete_1
  ```
{{ ydb-cli }} -p quickstart sql \
  -s 'SELECT t.id FROM test_delete_1 AS t WHERE t.id > 10' \
  --format json-unicode | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $lines AS List<Struct<id:UInt64>>;
      DELETE FROM test_delete_1 WHERE id IN (SELECT tl.id FROM AS_TABLE($lines) AS tl)' \
  --input-framing newline-delimited \
  --input-param-name lines \[disabled](#streaming-iterate)
  --input-batch adaptive \
  --input-batch-max-rows 10000
  ```bash

#### Processing messages read from a topic {#example-adaptive-pipeline-from-topic}

Examples of processing messages read from a topic are given in the article [{#T}](topic-pipeline.md#example-read-to-yql-param).

## See also {#see-also}

* [Parameterized queries in the {{ ydb-short-name }} SDK](../ydb-sdk/parameterized_queries.md)
  ```
