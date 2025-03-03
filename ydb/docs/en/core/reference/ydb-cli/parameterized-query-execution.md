# Running parametrized queries

## Overview

{{ ydb-short-name }} CLI can execute [parameterized queries](https://en.wikipedia.org/wiki/Prepared_statement). To use parameters you need to declare them using [the YQL `DECLARE` command](../../yql/reference/syntax/declare.md) in your query text.

The preferred way to run parameterized queries in {{ ydb-short-name }} CLI is to use the [`ydb sql`](sql.md) command.

Parameter values can be set on the command line, uploaded from [JSON](https://en.wikipedia.org/wiki/JSON) files, and read from `stdin` in binary or [JSON](https://en.wikipedia.org/wiki/JSON) format. While reading from `stdin` or a file, you can stream multiple parameter values triggering multiple query executions with batching options.

## Executing a single query {#one-request}

To provide parameters for a single query execution, you can use command line, JSON files, and `stdin`, using the following {{ ydb-short-name }} CLI options:

| Name | Description |
---|---
| `-p, --param` | The value of a single parameter of the query, in the format: `name=value` or `$name=value`, where `name` is the parameter name and `value` is its value (a valid [JSON value](https://www.json.org/json-ru.html)). This option can be specified repeatedly.<br/><br/>All the specified parameters must be declared in the query by the [DECLARE operator](../../yql/reference/syntax/declare.md); otherwise, you will get an error "Query does not contain parameter". If you specify the same parameter several times, you will get an error "Parameter value found in more than one source".<br/><br/>Depending on your operating system, you might need to escape the `$` character or enclose your expression in single quotes (`'`). |
| `--input-file` | Name of a file in [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} format and in [UTF-8]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/UTF-8){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/UTF-8){% endif %} encoding that contains parameter values matched against the query parameters by key names. Only one input file can be used.<br/><br/>If values of the same parameter are found in multiple files or set by the `--param` command line option, you'll get an error "Parameter value found in more than one source".<br/><br/>Keys that are present in the file but aren't declared in the query will be ignored without an error message. |
| `--input-format` | Format of parameter values, applied to all sources of parameters (command line, file, or `stdin`).<br/>Available options:<ul><li>`json` (default): [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} format.</li><li>`csv`:[CSV]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/CSV){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Comma-separated_values){% endif %} format.</li><li>`tsv`: [TSV]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/TSV){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Tab-separated_values){% endif %} format.</li><li>`raw`: Input is read as parameter value[s] with no transformation or parsing. Parameter name should be set with `--input-param-name` option.</li></ul> |
| `--input-binary-strings` | Input binary strings encoding format. Sets how binary strings in the input should be interterpreted.<br/>Available options:<ul><li>`unicode`: Every byte in binary strings that is not a printable ASCII symbol (codes 32-126) should be encoded as [UTF-8]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/UTF-8){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/UTF-8){% endif %}.</li><li>`base64`: Binary strings should be fully encoded with base64.</li></ul> |

If values are specified for all the parameters [in the `DECLARE` clause](../../yql/reference/syntax/declare.md), the query will be executed on the server. If a value is absent for at least one parameter, the command fails with the "Missing value for parameter" message.

#### More specific options for input parameters {#specific-param-options}

Following options are not described in `--help` output. To see its descriptions use `-hh` option instead.

| Name | Description |
---|---
| `--input-framing` | Input framing format. Defines how parameter sets are delimited on the input.

Available options:

* `no-framing` (default): Data from the input is taken as a single set of parameters.
* `newline-delimited`: Newline character delimits parameter sets on the input and triggers processing in accordance to `--input-batch` option. |
| `--input-param-name` | Parameter name on the input stream, required/applicable when input format implies values only (i.e. `--input-format raw`). |
| `--input-columns` | String with column names that replaces CSV/TSV header. Relevant when passing parameters in CSV/TSV format only. It is assumed that there is no header in the file. |
| `--input-skip-rows` | Number of CSV/TSV header rows to skip in the input data (not including the row of column names, if `--header` option is used). Relevant when passing parameters in CSV/TSV format only. |
| `--input-batch` | The batch mode applied to parameter sets on `stdin` or `--input-file`.

Available options:

* `iterative` (default): Executes the query for each parameter set (exactly one execution when `no-framing` is specified for `--input-framing`)
* `full`: Executes the query with all parameter sets wrapped in json list every time EOF is reached on stdin |
* `adaptive`: Executes the query with a json list of parameter sets every time when its number reaches `--input-batch-max-rows`, or the waiting time reaches `--input-batch-max-delay` |
| `--input-batch-max-rows` | Maximum size of list for input adaptive batching mode (default: 1000) |
| `--input-batch-max-delay` | Maximum delay to process first item in the list for `adaptive` batching mode (default: 1s) |

### Examples {#examples-one-request}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

#### Passing the value of a single parameter {#example-simple}

From the command line:

```bash
{{ ydb-cli }} -p quickstart sql -s 'declare $a as Int64;select $a' --param '$a=10'
```

Using a file:

```bash
echo '{"a":10}' > p1.json
{{ ydb-cli }} -p quickstart sql -s 'declare $a as Int64;select $a' --input-file p1.json
```

Through `stdin`:

```bash
echo '{"a":10}' | {{ ydb-cli }} -p quickstart sql -s 'declare $a as Int64;select $a'
```

```bash
echo '10' | {{ ydb-cli }} -p quickstart sql -s 'declare $a as Int64;select $a' --input-param-name a
```

#### Passing the values of parameters of different types from multiple sources {#example-multisource}

```bash
echo '{ "a":10, "b":"Some text", "x":"Ignore me" }' > p1.json
{{ ydb-cli }} -p quickstart sql \
  -s 'declare $a as Int64;
      declare $b as Utf8;
      declare $c as Int64;

      select $a, $b, $c' \
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

#### Passing binary content directly {#example-raw}

```bash
curl -Ls http://ydb.tech/docs/en | {{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $a AS String;
      SELECT LEN($a)' \
  --input-format raw \
  --input-param-name a
```

Command output (exact number of bytes may vary):

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
  -s 'declare $a as Int32;
      declare $b as String;
      select $a, $b' \
  --input-format csv \
  --input-columns 'a,b'
```

Вывод команды:

```text
┌─────────┬─────────────┐
| column0 | column1     |
├─────────┼─────────────┤
| 10      | "Some text" |
└─────────┴─────────────┘
```

## Iterative streaming processing {#streaming-iterate}

{{ ydb-short-name }} CLI supports execution of a query multiple times with different sets of parameter values provided on `stdin` **or** input file (not both). In this case, the database connection is established once and the query execution plan is cached. This substantially increases the performance of such an approach compared to separate CLI calls.

To use this feature, you need to stream different sets of the same parameters to command input (`stdin` or `--input-file`) one after another, specifying a rule for the {{ ydb-short-name }} CLI on how to separate the sets from each other.

The query is executed as many times as many parameter value sets received from input. Each set from input is joined with the parameter values defined with `--param` options. The command will complete once the input stream is closed. Each query is executed within a dedicated transaction.

A rule for separating parameter sets from one another (framing) complements the `--input-format` option:

| Name | Description |
---|---
| `--input-framing` | Input framing format. Defines how parameter sets are delimited on the input.

Available options:

* `no-framing` (default): Data from the input is taken as a single set of parameters.
* `newline-delimited`: Newline character delimits parameter sets on the input and triggers processing in accordance to `--input-batch` option. |

{% note warning %}

When using a newline character as a separator between the parameter sets, make sure that it isn't used inside the parameter sets. Putting some text value in quotes does not enable newlines within the text. Multiline JSON documents are not allowed.

{% endnote %}

### Example {#example-streaming-iterate}

#### Streaming processing of multiple parameter sets {#example-iterate}

{% list tabs %}

- JSON

  Suppose you need to run your query three times, with the following sets of values for the `a` and `b` parameters:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file that includes lines with JSON representations of these sets:

  ```bash
  echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' > par1.txt
  cat par1.txt
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

  Or just by passing input file name to `--input-file` option:

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

  This output can be passed as input to the next query command.

- CSV

  Suppose you need to run your query three times, with the following sets of values for the `a` and `b` parameters:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file that includes lines with CSV representations of these sets:

  ```bash
  echo -e 'a,b\n10,20\n15,25\n35,48' > par1.txt
  cat par1.txt
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

  Or just by passing input file name to `--input-file` option:

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

  This output can be passed as input to the next query command with .

- TSV

  Suppose you need to run your query three times, with the following sets of values for the `a` and `b` parameters:

  1. `a` = 10, `b` = 20
  2. `a` = 15, `b` = 25
  3. `a` = 35, `b` = 48

  Let's create a file that includes lines with TSV representations of these sets:

  ```bash
  echo -e 'a\tb\n10\t20\n15\t25\n35\t48' > par1.txt
  cat par1.txt
  ```

  Command output:

  ```text
  a	b
  10	20
  15	25
  35	48
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

  Or just by passing input file name to `--input-file` option:

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

For example, you need to run your query thrice, with the following sets of values for the `a` and `b` parameters:

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

The {{ ydb-short-name }} CLI supports automatic conversion of multiple consecutive parameter sets to a `List<>`, enabling you to process them in a single request and transaction. As a result, you can have a substantial performance gain compared to one-by-one query processing.

Two batch modes are supported:

- Full
- Adaptive

### Full batch mode {#batch-full}

The `full` mode is a simplified batch mode where the query runs only once, and all the parameter sets received from the input (`stdin` or `--input-file`) are wrapped into a `List<>`. If the request is too large, you will get an error.

Use this batch mode when you want to ensure transaction atomicity by applying all the parameters within a single transaction.

### Adaptive batch mode {#batch-adaptive}

In the `adaptive` mode, the input stream is split into multiple transactions, with the batch size automatically determined for each of them.

In this mode, you can process a broad range of dynamic workloads with unpredictable or infinite amounts of data, as well as with unpredictable or significantly varying rate of new sets appearance at the input. For example, such a profile is typical when sending the output of another command to `stdin` using the `|` operator.

The adaptive mode solves two basic issues of dynamic stream processing:

1. Limiting the maximum batch size.
2. Limiting the maximum data processing delay.

### Syntax {#batch-syntax}

To use the batching capbilities, define the `List<...>` or `List<Struct<...>>` parameter in the query's DECLARE clause, and use the following options:

| Name | Description |
---|---
| `--input-batch` | The batch mode applied to parameter sets on `stdin` or `--input-file`.

Available options:

* `iterative` (default): Executes the query for each parameter set (exactly one execution when `no-framing` is specified for `--input-framing`)
* `full`: Executes the query with all parameter sets wrapped in json list every time EOF is reached on stdin |
* `adaptive`: Executes the query with a json list of parameter sets every time when its number reaches `--input-batch-max-rows`, or the waiting time reaches `--input-batch-max-delay` |

In the adaptive batch mode, you can use the following additional parameters:

| Name | Description |
---|---
| `--input-batch-max-rows` | The maximum number of sets of parameters per batch in the `adaptive` batch mode. The next batch will be sent with the query if the number of parameter sets in it reaches the specified limit. When it's `0`, there's no limit.<br/><br/>Default value: `1000`.<br/><br/>Parameter values are sent to each query execution without streaming, so the total size per GRPC request that includes the parameter values has the upper limit of about 5 MB. |
| `--input-batch-max-delay` | The maximum delay to submit a received parameter set for processing in the `adaptive` batch mode. It's set as a number with a time unit - `s`, `ms`, `m`.<br/><br/>Default value: `1s` (1 second).<br/><br/>The {{ ydb-short-name }} CLI starts a timer when it receives a first set of parameters for the batch from the input, and sends the whole accumulated batch for execution once the timer expires. With this parameter, you can batch efficiently when new parameter sets arrival rate on the input is unpredictable. |

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

This example demonstrates the adaptive batching triggered by a processing delay. In the first line of the command below, we generate 1,000 rows at a delay of 0.2 seconds on `stdout` and pipe them to `stdin` to the `ydb sql` query execution command. The  query execution command shows the parameter batches in each subsequent query call.

```bash
for i in $(seq 1 1000);do echo "Line$i";sleep 0.2;done | \
{{ ydb-cli }} -p quickstart sql \
  -s 'DECLARE $x AS List<Utf8>;
      SELECT ListLength($x), $x' \
  --input-framing newline-delimited \
  --input-format raw \
  --input-param-name x \
  --input-batch adaptive
```

Command output (actual values may differ):

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

The first batch includes all the rows accumulated at the input while the database connection has had been establishing, that's why it's larger than the next ones.

You can terminate the command by Ctrl+C or wait 200 seconds until the input generation is finished.

#### Limit on the number of records {#example-adaptive-limit}

This example demonstrates the adaptive batching triggered by a number of parameter sets. In the first line of the command below, we generate 200 rows. The command will show parameter batches in each subsequent query call, applying the given limit `--input-batch-max-rows` of 20 (the default limit is 1,000).

In this example, we also demonstrate the option to join parameters from different sources and generate JSON at the output.

```bash
for i in $(seq 1 200);do echo "Line$i";done | \
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

This example shows how you can delete an unlimited number of records from {{ ydb-short-name }} tables without risking exceeding the limit on the number of records per transaction.

Let's create a test table:

```bash
{{ ydb-cli }} -p quickstart sql -s 'create table test_delete_1( id UInt64 not null, primary key (id))'
```

Add 100,000 records to it:

```bash
for i in $(seq 1 100000);do echo "$i";done | \
{{ ydb-cli }} -p quickstart import file csv -p test_delete_1
```

Delete all records with ID > 10:

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

#### Processing of messages read from a topic {#example-adaptive-pipeline-from-topic}

Examples of processing messages read from a topic are given in [{#T}](topic-pipeline.md#example-read-to-yql-param).

## See also {#see-also}

* [Parameterized queries in {{ ydb-short-name }} SDK](../ydb-sdk/parameterized_queries.md)
