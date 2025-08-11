# Running parameterized queries

## Overview

{{ ydb-short-name }} CLI can execute parameterized queries. To use parameters, you need to declare them using [the YQL `DECLARE` command](../../yql/reference/syntax/declare.md) in your query text.

The preferred way to run parameterized queries in {{ ydb-short-name }} CLI is to use the [`ydb sql`](sql.md) command.

Parameter values can be set via the command-line arguments, uploaded from [JSON](https://en.wikipedia.org/wiki/JSON) files, and read from `stdin` in binary or JSON format. Binary data can be encoded as base64 or UTF-8. While reading from `stdin` or a file, you can stream multiple parameter values, triggering multiple query executions with batching options.

## Why use parameterized queries?

Using parameterized queries offers several key advantages:

* **Enhanced Performance:** Parameterized queries significantly boost performance when executing multiple similar queries that differ only in input parameters. This is achieved through the use of [prepared statements](https://en.wikipedia.org/wiki/Prepared_statement). The query is compiled once and then cached on the server. Subsequent requests with the same query text bypass the compilation phase, allowing for immediate execution.

* **Protection Against SQL Injection:** Another critical benefit of using parameterized queries is the protection they offer against [SQL injection](https://en.wikipedia.org/wiki/SQL_injection) attacks. This security feature ensures that the input parameters are appropriately handled, mitigating the risk of malicious code execution.

## Executing a single query {#one-request}

To provide parameters for a single query execution, you can use the command-line arguments, JSON files, or `stdin`, using the following {{ ydb-short-name }} CLI options:

| Name | Description |
| --- | --- |
| `-p, --param` | The value of a single query parameter in the `name=value` or `$name=value` format, where `name` is the parameter name and `value` is its value (a valid [JSON value](https://www.json.org/json-en.html)). This option can be specified multiple times.<br/><br/>All specified parameters must be declared in the query using the [DECLARE operator](../../yql/reference/syntax/declare.md). Otherwise, you will receive the "Query does not contain parameter" error. If you specify the same parameter multiple times, you will receive the "Parameter value found in more than one source" error.<br/><br/>Depending on your operating system, you might need to escape the `$` character or enclose your expression in single quotes (`'`). |
| `--input-file` | The name of a file in [JSON](https://en.wikipedia.org/wiki/JSON) format and [UTF-8](https://en.wikipedia.org/wiki/UTF-8) encoding that contains parameter values matched against the query parameters by key names. Only one input file can be used.<br/><br/>If values for the same parameter are found in multiple files or set by the `--param` command-line option, you will receive the "Parameter value found in more than one source" error.<br/><br/>Keys that are present in the file but not declared in the query will be ignored without an error message. |
| `--input-format` | The format of parameter values applied to all sources of parameters (command line, file, or `stdin`).<br/>Available options:<ul><li>`json` (default): JSON format.</li><li>`csv`: [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) format.</li><li>`tsv`: [TSV](https://en.wikipedia.org/wiki/Tab-separated_values) format.</li><li>`raw`: Input is read as parameter values with no transformation or parsing. The parameter name should be set with the `--input-param-name` option.</li></ul> |
| `--input-binary-strings` | The input binary string encoding format. Defines how binary strings in the input should be interpreted.<br/>Available options:<ul><li>`unicode`: Every byte in binary strings that is not a printable ASCII symbol (codes 32-126) should be encoded as UTF-8.</li><li>`base64`: Binary strings should be fully encoded with base64.</li></ul> |

If values are specified for all non-optional (i.e., NOT NULL) parameters [in the `DECLARE` clause](../../yql/reference/syntax/declare.md), the query will be executed on the server. If a value is absent for even one such parameter, the command fails with the error message "Missing value for parameter".

### More specific options for input parameters {#specific-param-options}

The following options are not described in the `--help` output. To see their descriptions, use the `-hh` option instead.

| Name | Description |
| --- | --- |
| `--input-framing` | The input framing format. Defines how parameter sets are delimited in the input.<br/>Available options:<br/><ul><li>`no-framing` (default): Data from the input is taken as a single set of parameters.</li><li>`newline-delimited`: A newline character delimits parameter sets in the input and triggers processing according to the `--input-batch` option.</li></ul> |
| `--input-param-name` | The parameter name in the input stream, required when the input format contains only values (that is, when `--input-format raw` is used). |
| `--input-columns` | A string with column names that replaces the CSV/TSV header. Relevant only when passing parameters in CSV/TSV format. It is assumed that the file does not contain a header. |
| `--input-skip-rows` | The number of CSV/TSV header rows to skip in the input data (excluding the row of column names if the `--header` option is used). Relevant only when passing parameters in CSV/TSV format. |
| `--input-batch` | The batch mode applied to parameter sets from `stdin` or `--input-file`.<br/>Available options:<br/><ul><li>`iterative` (default): Executes the query for each parameter set (exactly one execution when `no-framing` is specified for `--input-framing`).</li><li>`full`: A simplified batch mode where the query runs only once and all the parameter sets received from the input (`stdin` or `--input-file`) are wrapped into a `List<...>`.</li><li>`adaptive`: Executes the query with a JSON list of parameter sets when either the number of sets reaches `--input-batch-max-rows` or the waiting time reaches `--input-batch-max-delay`.</li></ul> |
| `--input-batch-max-rows` | The maximum size of the list for the input adaptive batching mode (default: 1000). |
| `--input-batch-max-delay` | The maximum delay before submitting a received parameter set for processing in the `adaptive` batch mode. The value is specified as a number with a time unit: `s` (seconds), `ms` (milliseconds), `m` (minutes), etc. Default value: `1s` (1 second).<br/><br/>The {{ ydb-short-name }} CLI starts a timer when it receives the first set of parameters for the batch from the input and sends the accumulated batch for execution once the timer expires. This parameter enables efficient batching when the arrival rate of new parameter sets is unpredictable. |

### Examples {#examples-one-request}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

#### Passing the value of a single parameter {#example-simple}

From the command line using `--param` option:

```bash
{{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --param '$a=10'
```

Using a file in JSON format (which is used by default):

```bash
echo '{"a":10}' > p1.json
{{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --input-file p1.json
```

Via `stdin` passing a JSON string as a set of one parameter:

```bash
echo '{"a":10}' | {{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a'
```

Via `stdin` passing only a parameter value and setting a parameter name via the `--input-param-name` option:

```bash
echo '10' | {{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --input-param-name a
```

#### Passing the values of parameters of different types from multiple sources {#example-multisource}

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

#### Passing Base64-encoded binary strings {#example-base64}

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

#### Passing raw binary content directly {#example-raw}

```bash
curl -Ls http://ydb.tech/docs/en | {{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS String;
      SELECT LEN($a)' \
  --input-format raw \
  --input-param-name a
```

Command output (the exact number of bytes may vary):

```text
┌─────────┐
| column0 |
├─────────┤
| 66426   |
└─────────┘
```

#### Passing CSV data {#example-csv}

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

{{ ydb-short-name }} CLI supports executing a query multiple times with different sets of parameter values provided via `stdin` **or** an input file (but not both). In this case, the database connection is established once, and the query execution plan is cached. This approach significantly improves performance compared to making separate CLI calls.

To use this feature, stream different sets of values for the same parameters to the command input (`stdin` or `--input-file`) one after another, specifying a rule for the {{ ydb-short-name }} CLI to separate the sets.

The query is executed as many times as there are parameter value sets received from the input. Each set is combined with the parameter values defined using the `--param` options. The command completes once the input stream is closed. Each query is executed within a dedicated transaction.

A rule for separating parameter sets (framing) complements the `--input-format` option:

| Name | Description |
| --- | --- |
| `--input-framing` | Input framing format. Defines how parameter sets are delimited on the input.<br/>Available options:<br/><ul><li>`no-framing` (default): Data from the input is taken as a single set of parameters.</li><li>`newline-delimited`: A newline character delimits parameter sets in the input and triggers processing according to the `--input-batch` option.</li></ul> |

{% note warning %}

When using a newline character as a separator between parameter sets, ensure that newline characters are not used inside the parameter sets. Quoting a text value does not allow newlines within the text. Multiline JSON documents are also not allowed.

{% endnote %}

### Example {#example-streaming-iterate}

#### Streaming processing of multiple parameter sets {#example-iterate}

{% list tabs %}

- JSON

  Suppose you need to run your query three times with the following sets of values for the `a` and `b` parameters:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file that contains lines with JSON representations of these sets:

  ```bash
  echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' | tee par1.txt
  ```

  Command output:

  ```text
  {"a":10,"b":20}
  {"a":15,"b":25}
  {"a":35,"b":48}
  ```

  Let's execute the query by passing the content of this file to `stdin`, formatting the output as JSON:

  ```bash
  cat par1.txt | \
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
    --input-framing newline-delimited \
    --format json-unicode
  ```

  Command output:

  ```text
  {"column0":30}
  {"column0":40}
  {"column0":83}
  ```

  Or just by passing the input file name to the `--input-file` option:

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

  This output can be passed as input to the next query command if it has a `column0` parameter.

- CSV

  Suppose you need to run your query three times with the following sets of values for the `a` and `b` parameters:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file that contains lines with CSV representations of these sets:

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

  Let's execute the query by passing the content of this file to `stdin`, formatting the output as CSV:

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

  Or just by passing the input file name to the `--input-file` option:

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

  This output can be passed as input to another command running a different parameterized query.

- TSV

  Suppose you need to run your query three times, with the following sets of values for the `a` and `b` parameters:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file that includes lines with TSV representations of these sets:

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

  Let's execute the query by passing the content of this file to `stdin`, formatting the output as TSV:

  ```bash
  cat par1.txt | \
  {{ ydb-cli }} -p quickstart sql \
    -s 'DECLARE $a AS Int64;
        DECLARE $b AS Int64;
        SELECT $a + $b' \
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

  Or just by passing the input file name to the `--input-file` option:

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

  This output can be passed as input to the next query command.

{% endlist %}

#### Streaming processing with joining parameter values from different sources {#example-iterate-union}

For example, you need to run your query three times with the following sets of values for the `a` and `b` parameters:

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

## Batched streaming processing {#streaming-batch}

The {{ ydb-short-name }} CLI supports automatic conversion of multiple consecutive parameter sets to a `List<...>`, enabling you to process them in a single request and transaction. As a result, you can achieve a substantial performance gain compared to one-by-one query processing.

Two batch modes are supported:

- Full
- Adaptive

### Full batch mode {#batch-full}

The `full` mode is a simplified batch mode where the query runs only once, and all the parameter sets received from the input (`stdin` or `--input-file`) are wrapped into a `List<...>`. If the request is too large, you will receive an error.

Use this batch mode when you want to ensure transaction atomicity by applying all the parameters within a single transaction.

### Adaptive batch mode {#batch-adaptive}

In the `adaptive` mode, the input stream is split into multiple transactions, with the batch size automatically determined for each of them.

In this mode, you can process a broad range of dynamic workloads with unpredictable or infinite amounts of data, as well as workloads with an unpredictable or significantly varying rate of new sets appearing in the input. For example, this scenario is common when sending the output of another command to `stdin` using the `|` operator.

The adaptive mode solves two key issues of dynamic stream processing:

1. Limiting the maximum batch size.
2. Limiting the maximum data processing delay.

### Syntax {#batch-syntax}

To use the batching capabilities, define the `List<...>` or `List<Struct<...>>` parameter in the query's `DECLARE` clause, and use the following options:

| Name | Description |
| --- | --- |
| `--input-batch` | The batch mode applied to parameter sets on `stdin` or `--input-file`.<br/>Available options:<br/><ul><li>`iterative` (default): Executes the query for each parameter set (exactly one execution when `no-framing` is specified for `--input-framing`).</li><li>`full`: A simplified batch mode where the query runs only once and all the parameter sets received from the input (`stdin` or `--input-file`) are wrapped into a `List<...>`.</li><li>`adaptive`: Executes the query with a JSON list of parameter sets whenever the number of sets reaches `--input-batch-max-rows` or the waiting time reaches `--input-batch-max-delay`.</li></ul> |

In the adaptive batch mode, you can use the following additional parameters:

| Name | Description |
| --- | --- |
| `--input-batch-max-rows` | The maximum number of parameter sets per batch in the `adaptive` batch mode. The next batch will be sent with the query if the number of parameter sets reaches the specified limit. When set to `0`, there is no limit.<br/><br/>Default value: `1000`.<br/><br/>Parameter values are sent to each query execution without streaming, so the total size per gRPC request that includes the parameter values has an upper limit of about 5 MB. |
| `--input-batch-max-delay` | The maximum delay before submitting a received parameter set for processing in the `adaptive` batch mode. The value is specified as a number with a time unit: `s` (seconds), `ms` (milliseconds), `m` (minutes), etc. Default value: `1s` (1 second).<br/><br/>The {{ ydb-short-name }} CLI starts a timer when it receives the first set of parameters for the batch from the input and sends the accumulated batch for execution once the timer expires. This parameter enables efficient batching when the arrival rate of new parameter sets is unpredictable. |

### Examples: Full batch processing {#example-batch-full}

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

### Examples: Adaptive batch processing {#example-batch-adaptive}

#### Limiting the maximum data processing delay {#example-adaptive-delay}

This example demonstrates adaptive batching triggered by a processing delay. In the first line of the command below, we generate 1,000 rows with a delay of 0.2 seconds on `stdout` and pipe them to `stdin` for the `ydb sql` query execution command. The query execution command displays the parameter batches in each subsequent query call.

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

Command output (the actual values may differ):

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

The first batch includes all the rows accumulated at the input while the database connection was being established, which is why it is larger than the subsequent ones.

You can terminate the command by pressing Ctrl+C or wait 200 seconds until the input generation is finished.

#### Limit on the number of records {#example-adaptive-limit}

This example demonstrates adaptive batching triggered by the number of parameter sets. In the first line of the command below, we generate 200 rows. The command displays parameter batches in each subsequent query call, applying the specified limit `--input-batch-max-rows` of 20 (the default limit is 1,000).

This example also demonstrates the option to join parameters from different sources and generate JSON as output.

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

#### Deleting multiple records from a {{ ydb-short-name }} table based on primary keys {#example-adaptive-delete-pk}

{% include [not_allow_for_olap_note](../../_includes/not_allow_for_olap_note.md) %}

If you attempt to delete a large number of rows from a substantial table using a simple `DELETE FROM large_table WHERE id > 10;` statement, you may encounter an error due to exceeding the transaction record limit. This example shows how to delete an unlimited number of records from {{ ydb-short-name }} tables without breaching this limitation.

Let's create a test table:

```bash
{{ ydb-cli }} -p quickstart sql -s 'CREATE TABLE test_delete_1(id UInt64 NOT NULL, PRIMARY KEY (id))'
```

Add 100,000 records to it:

```bash
for i in $(seq 1 100000); do echo "$i"; done | \
{{ ydb-cli }} -p quickstart import file csv -p test_delete_1
```

Delete all records with `id` greater than 10:

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

Examples of processing messages read from a topic are provided in [{#T}](topic-pipeline.md#example-read-to-yql-param).

## See also {#see-also}

* [Parameterized queries in {{ ydb-short-name }} SDK](../ydb-sdk/parameterized_queries.md)
