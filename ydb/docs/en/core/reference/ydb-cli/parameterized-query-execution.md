# Executing parameterized queries

## Overview

{{ ydb-short-name }} CLI supports executing parameterized queries. To work with parameters, their definitions must be present in the query text using [the YQL `DECLARE` command](../../yql/reference/syntax/declare.md)

The main tool for executing parameterized queries in {{ ydb-short-name }} CLI is the [{{ ydb-cli }} sql](sql.md) command.

## Why use parameterized queries?

Using parameterized queries provides several important benefits:

* **Improved performance:** Parameterized queries significantly improve performance when executing large numbers of similar operations that differ only in input values. This is due to query plan caching. After the first compilation of a query, its plan is saved in [LRU cache](https://en.wikipedia.org/wiki/Cache_replacement_policies) [{{ ydb-short-name }} node](../../concepts/glossary.md#node). All subsequent calls to the same query with new parameters skip the compilation stage and start executing immediately, saving time and resources.

* **Protection against SQL injections:** Another important advantage of using parameterized queries is protection against [SQL injection](https://en.wikipedia.org/wiki/SQL_injection). This security feature ensures proper handling of input data, reducing the risk of executing malicious code.

## Single query execution {#one-request}

This command supports passing parameters via command-line options, a file, and `stdin`. When passing parameters via `stdin` or a file, you can execute the query multiple times with different parameter values and support batching. The [{{ ydb-cli }} sql](sql.md) command provides the following parameters for this:

| Name | Description |
| --- | --- |
| `-p, --param` | The value of a query parameter in the `$name=value` or `name=value` format, where `name` is the parameter name and `value` is its value (valid [JSON value](https://www.json.org/json-ru.html)). |
| `--input-file` | The name of a file in the [JSON](https://en.wikipedia.org/wiki/JSON) format in the [UTF-8](https://en.wikipedia.org/wiki/UTF-8) encoding, which specifies parameter values mapped to query parameters by key names. A maximum of one parameter file can be used. |
| `--input-format` | The format for representing parameter values. Applies to all methods of passing them (via a command parameter, file, or `stdin`).<br/>Possible values:<ul><li>`json` (default): [JSON](https://en.wikipedia.org/wiki/JSON) format.</li><li>`csv` — [CSV](https://en.wikipedia.org/wiki/CSV) format. By default, parameter names should be in the CSV file header. When executing a query once, only one line in the file is allowed, excluding the header.</li><li>`tsv` — [TSV](https://en.wikipedia.org/wiki/TSV) format.</li><li>`raw`: The input stream from `stdin` or `--input-file` contains only the parameter value as binary data. The parameter name must be specified with the `--input-param-name` option.</li></ul> |
| `--input-binary-strings` | The encoding format for parameters of the «binary string» type (`DECLARE $par AS String`). Indicates how binary strings from the input stream should be interpreted.<br/>Possible values:<ul><li>`unicode`: Each byte in the binary string that is not a printable ASCII character (codes 32-126) must be encoded in [UTF-8](https://en.wikipedia.org/wiki/UTF-8).</li><li>`base64`: Binary strings are represented in [Base64](https://en.wikipedia.org/wiki/Base64) encoding. This option allows you to pass binary data, which will be decoded from Base64 by the {{ ydb-short-name }} CLI.</li></ul> |

If values are specified for all parameters that do not allow `NULL` (i.e., with the NOT NULL type), [in the `DECLARE` clause](../../yql/reference/syntax/declare.md), the query will be executed on the server. If a value is missing for at least one such parameter, the command will fail with the message "No value specified for the parameter."

### More specific options for using input parameters {#specific-param-options}

The following options are not displayed in the output `--help`. Their description can only be seen in the output `-hh`.

| Name | Description |
| --- | --- |
| `--input-framing` | Sets the framing for the input stream (`stdin` or `--input-file`). Determines how the input stream will be divided into separate sets of parameters.<br/>Possible values:<br/><ul><li>`no-framing` (default): The input stream is expected to contain one set of parameters, and the query is executed once.</li><li>`newline-delimited`: A newline character marks the end of one set of parameters in the input stream, separating it from the next. A set of parameters is considered collected each time a newline character is received in the input stream.</li></ul> |
| `--input-param-name` | The name of the parameter whose value is passed in the input stream. Specified without the `$` symbol. Required when using the `raw` format in `--input-format`.<br/><br/>When used with JSON format, the input stream is interpreted not as a JSON document, but as a JSON value, with the value passed to the parameter with the specified name. |
| `--input-columns` | A string with column names that replace the header of the CSV/TSV document read from the input stream. If this option is specified, the header is considered absent. The option is only valid with CSV and TSV formats. |
| `--input-skip-rows` | The number of lines from the beginning of the data read from stdin that should be skipped, not including the header line if present. The option is only valid with CSV and TSV stdin formats. |
| `--input-batch` | The mode for batching parameter sets obtained from the input stream (`stdin` or `--input-file`).<br/>Possible values:<br/><ul><li>`iterative` (default): Batching [disabled](#streaming-iterate)<br/>Possible values:<br/><ul><li>`iterative` (default): Batching [disabled](#streaming-iterate)<br/>Possible values:<br/><ul><li>`iterative` (default): Batching [disabled](#streaming-iterate)<br/>Possible values:<br/><ul><li>`iterative` (default): Batching [disabled](#streaming-iterate)
| `0` | The maximum number of parameter sets in a batch for adaptive batching mode. The next batch will be sent for execution with the query once the number of data sets in it reaches the specified value. Setting it to <br/><br/> removes the limit.`1000`The default value is <br/><br/>.`--input-batch-max-delay`Parameters are passed to the query without streaming, and the total size of a single GRPC request, which includes parameter values, has an upper limit of about 5 MB. |
| `s` | The maximum delay for sending a received set of parameters for processing in adaptive batching mode. Specified as a number with a time dimension — `ms` (seconds), `m` (milliseconds), `1s` (minutes), etc. The default value is <br/><br/>{{ ydb-short-name }} (1 second).`stdin`⟦VAR:4��| Name | Description |
| --- | --- |
| 

### Examples {#examples-one-request}[ydb-cli-profile](../../_includes/ydb-cli-profile.md) {#examples-one-request}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

#### Passing a single parameter value {#example-simple}

On the command line, using the option `--param`:

```
{{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --param '$a=10'
```

Via a JSON file (which is used by default):

```bash
echo '{"a":10}' > p1.json
{{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a' --input-file p1.json
```

Through `stdin`, passing a JSON string as a set of one parameter:

```
echo '{"a":10}' | {{ ydb-cli }} -p quickstart sql -s 'DECLARE $a AS Int64; SELECT $a'
```

Using `stdin` by passing only the parameter value and specifying the parameter name using the `--input-param-name` option:

```
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

#### Direct transmission of binary content {#example-raw}

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

#### CSV file upload {#example-csv}

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

## Iterative stream processing {#streaming-iterate}

{{ ydb-short-name }} CLI supports the ability to execute a query multiple times with different sets of parameter values, either by passing them through `stdin` **or** an input file (not both at the same time). In this case, the connection to the database is established once, and the query execution plan is cached, which significantly improves the performance of this approach compared to separate CLI calls.

To use this feature, you need to pass different sets of values for the same parameters to the command input one after another, telling {{ ydb-short-name }} CLI the rule that will allow you to separate these sets from each other.

The query is executed as many times as there are sets of parameter values received. Each set received through the input stream (`stdin` or `--input-file`) is combined with the parameter values defined through the `--param` options. The command execution will be completed after the input stream ends. Each query is executed in its own transaction.

The rule for separating parameter sets from each other (framing) complements the description of the parameter representation format in the input stream, specified by the `--input--framing` parameter:

| Name | Description |
| --- | --- |
| `--input-framing` | Sets the framing for the input stream (file or `stdin`). <br/>Possible values:<ul><li>`no-framing` (default) — the input is expected to contain a single set of parameters, and the query is executed once after the stream is read.</li><li>`newline-delimited` — a newline character marks the end of a parameter set in the input, separating it from the next one. The query is executed each time a newline character is received.</li></ul> |

{% note warning %}

When using a newline character as a delimiter for parameter sets, you must ensure that it does not appear within the parameter sets. Enclosing text in quotes does not make it acceptable to include a newline within that text. Multiline JSON documents are not allowed.

{% endnote %}

### Example {#example-streaming-iterate}

#### Streaming processing of multiple parameter sets {#example-iterate}

{% list tabs %}

- JSON

  Suppose we need to execute a query three times, with the following sets of parameter values `a` and `b`:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file that will contain lines with the JSON representation of these sets:

  ```
  echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' | tee par1.txt
  ```

Command output:

  ```text
  {"a":10,"b":20}
  {"a":15,"b":25}
  {"a":35,"b":48}
  ```

Run the query by passing the contents of this file to `stdin`, with the output formatted as JSON:

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

Or simply by passing the file name to the option `--input-file`:

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

The result obtained in this format can be used as input for the next query execution command.

- CSV

Suppose we need to execute the query three times with the following sets of parameter values `a` and `b`:

1. `a` = 10, `b` = 20
2. `a` = 15, `b` = 25
3. `a` = 35, `b` = 48

Let's create a file with parameter value sets in CSV format:

  ```
  echo -e 'a,b\n10,20\n15,25\n35,48' | tee par1.txt
  ```

Command output:

  ```text
  a,b
  10,20
  15,25
  35,48
  ```

Run the query by passing the contents of this file to `stdin`, with the output formatted as CSV:

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

Or simply by passing the file name to the option `--input-file`:

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

The result obtained in this format can be used as input for the next query execution command by specifying the data in CSV format with the `--input-columns` option.

- TSV

Suppose we need to execute the query three times with the following sets of parameter values `a` and `b`:

1. `a` = 10, `b` = row1
2. `a` = 15, ��The result obtained in this format can be used as input for the next query execution command by specifying the data in CSV format with the `b` option.

- TSV

Suppose we need to execute the query three times with the following sets of parameter values `a` and `b`:

1. 

  ```bash
  echo -e 'a\tb\n10\t20\n15\t25\n35\t48' | tee par1.txt
  ```

Command output:

  ```
  a  b
  10 20
  15 25
  35 48
  ```

Run the query by passing the contents of this file to `stdin`, with the output formatted as TSV:

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

Or simply by passing the file name to the option `--input-file`:

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

The result obtained in this format can be used as input for the next query execution command by specifying the data in TSV format with the `--input-columns` option.

{% endlist %}

#### Streaming processing with merging parameter values from different sources {#example-iterate-union}

Suppose we need to execute a query three times with the following sets of parameter values `a` and `b`:

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

## Batch streaming processing {#streaming-batch}

{{ ydb-short-name }} CLI supports automatic conversion of parameter sets into `List<...>`, allowing you to process multiple parameter sets in a single transaction with a single request to the server, which can significantly improve performance compared to executing requests one by one.

Two batching modes are supported:

- Full (`full`);
- Adaptive (`adaptive`).

### Full batching mode {#batch-full}

The full (`full`) mode is a simplified batching option where the query is executed once, wrapping all parameter sets received from the input stream in `List<...>`. If the query size is too large, an error will be returned.

This batching option is necessary when you need to guarantee atomicity by applying all parameters in a single transaction.

### Adaptive batching mode {#batch-adaptive}

In adaptive (`adaptive`) mode, the input stream processing is split into multiple transactions, with the package size automatically adjusted for each of them.

This mode allows you to efficiently handle a wide range of input loads with unpredictable or infinite amounts of data, as well as unpredictable or rapidly changing rates of data arrival. In particular, this profile is typical when the output of another command is fed to `stdin` via the `|` operator.

The adaptive mode solves two main problems of dynamic stream processing:

1. Limiting the maximum package size.
2. Limiting the maximum data processing delay.

### Syntax {#batch-syntax}

To use batching, you need to describe a parameter of type `List<...>` or `List<Struct<...>>` in the DECLARE section of the query and select the mode with the next parameter:

| Name | Description |
| --- | --- |
| `--input-batch` | The batch mode applied to parameter sets on `stdin` or `--input-file`.<br/>Available options:<br/><ul><li>`iterative` (default): Executes the query for each parameter set (exactly one execution when `no-framing` is specified for `--input-framing`).</li><li>`full`: A simplified batch mode where the query runs only once and all the parameter sets received from the input (`stdin` or `--input-file`) are wrapped into a `List<...>`.</li><li>`adaptive`: Executes the query with a JSON list of parameter sets whenever the number of sets reaches `--input-batch-max-rows` or the waiting time reaches `--input-batch-max-delay`.</li></ul> |

In adaptive batching mode, additional parameters are available:

| Name | Description |
| --- | --- |
| `--input-batch-max-rows` | The maximum number of parameter sets per batch in the `adaptive` batch mode. The next batch will be sent with the query if the number of parameter sets reaches the specified limit. When set to `0`, there is no limit.<br/><br/>Default value: `1000`.<br/><br/>Parameter values are sent to each query execution without streaming, so the total size per gRPC request that includes the parameter values has an upper limit of about 5 MB. |
| `--input-batch-max-delay` | The maximum delay before submitting a received parameter set for processing in the `adaptive` batch mode. The value is specified as a number with a time unit: `s` (seconds), `ms` (milliseconds), `m` (minutes), etc. Default value: `1s` (1 second).<br/><br/>The {{ ydb-short-name }} CLI starts a timer when it receives the first set of parameters for the batch from the input and sends the accumulated batch for execution once the timer expires. This parameter enables efficient batching when the arrival rate of new parameter sets is unpredictable. |

### Examples — full batch processing {#example-batch-full}

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

### Examples — adaptive batch processing {#example-batch-adaptive}

#### Limiting the maximum processing delay {#example-adaptive-delay}

To demonstrate the operation of adaptive batching with triggering a processing delay limit, the first line of the command below generates 1000 rows with a 0.2-second delay in `stdout`, which are passed to the `stdin` query execution command. The query execution command, in turn, displays parameter batches in each subsequent query call.

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

Command output (exact values may vary):

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

The first batch includes all the rows that accumulated at the input during the database connection opening, which is why it is larger than the subsequent ones.

You can interrupt the command execution by pressing Ctrl+C, or wait until 200 seconds pass, during which the input is generated.

#### Record limit {#example-adaptive-limit}

To demonstrate adaptive batching with a trigger based on the number of parameter sets, the command below generates 200 rows in the first line. The command will display parameter batches in each subsequent query call, taking into account the specified limit `--input-batch-max-rows` of 20 (default is 1000).

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

#### Deleting multiple records from a string table {{ ydb-short-name }} by primary keys {#example-adaptive-delete-pk}

If you try to delete a large number of rows from a large table using a simple query `DELETE FROM large_table WHERE id > 10;`, you may encounter an error due to exceeding the limit on the number of records in a transaction. This example shows how you can delete an unlimited number of records from {{ ydb-short-name }} tables without violating this limit.
Let's create a test string table:

```bash
{{ ydb-cli }} -p quickstart sql -s 'CREATE TABLE test_delete_1(id UInt64 NOT NULL, PRIMARY KEY (id))'
```

Let's insert 100,000 records into it:

```bash
for i in $(seq 1 100000); do echo "$i";done | \
{{ ydb-cli }} -p quickstart import file csv -p test_delete_1
```

Delete all records with values `id` greater than 10:

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
  --input-batch-max-rows 10000[{#T}](topic-pipeline.md#example-read-to-yql-param)
```

#### Processing messages read from a topic {#example-adaptive-pipeline-from-topic}

Examples of processing messages read from a topic are provided in the article [{#T}](topic-pipeline.md#example-read-to-yql-param).

## See also {#see-also}

* [Parameterized queries in {{ ydb-short-name }} SDK](../ydb-sdk/parameterized_queries.md)
