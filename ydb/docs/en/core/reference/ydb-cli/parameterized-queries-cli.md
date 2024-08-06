# Running parametrized YQL queries and scripts

## Overview

{{ ydb-short-name }} CLI can execute [parameterized YQL queries](https://en.wikipedia.org/wiki/Prepared_statement). To use parameters you need to declare them using [the YQL `DECLARE`](../../yql/reference/syntax/declare.md) command in your YQL query text.

To run parameterized YQL queries you can use the following {{ ydb-short-name }} CLI commands:

* [ydb yql](yql.md).
* [ydb scripting yql](scripting-yql.md).
* [ydb table query execute](table-query-execute.md).

These commands support the same query parametrization options. Parameter values can be set on the command line, uploaded from [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} files, and read from `stdin` in binary or [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} format. On `stdin` you can stream multiple parameter values triggering multiple YQL query executions with batching options.

{% note warning %}

Among the above commands, only the `table query execute` applies retry policies. Such policies ensure reliable query execution and continuity when certain data ranges are unavailable for a short time because of partition changes or other regular processes in a distributed database.

{% endnote %}

## Executing a single YQL query {#one-request}

To provide parameters for a YQL query execution, you can use command line, JSON files, and `stdin`, using the following {{ ydb-short-name }} CLI options:

| Name | Description |
---|---
| `-p, --param` | An expression in the format `$name=value`, where `$name` is the name of the YQL query parameter and `value` is its value (a correct [JSON value](https://www.json.org/json-ru.html)). The option can be specified repeatedly.<br/><br/>All the specified parameters must be declared in the YQL query by the [DECLARE operator](../../yql/reference/syntax/declare.md); otherwise, you will get an error "Query does not contain parameter". If you specify the same parameter several times, you will get an error "Parameter value found in more than one source".<br/><br/>Depending on your operating system, you might need to escape the `$` character or enclose your expression in single quotes (`'`). |
| `--param-file` | Name of a file in [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} format in [UTF-8]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/UTF-8){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/UTF-8){% endif %} encoding that contains parameter values matched against the YQL query parameters by key names. The option can be specified repeatedly.<br/><br/>If values of the same parameter are found in multiple files or set by the `--param` command line option, you'll get an error "Parameter value found in more than one source".<br/><br/>Names of keys in the JSON file are expected without the leading `$` sign. Keys that are present in the file but aren't declared in the YQL query will be ignored without an error message. |
| `--input-format` | Format of parameter values, applied to all sources of parameters (command line, file, or `stdin`).<br/>Available options:<ul><li>`json-unicode` (default):[JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}.</li><li>`json-base64`: [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} with values of binary string parameters (`DECLARE $par AS String`) are [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}-encoded. This feature enables you to process binary data, being decoded from Base64 by the {{ ydb-short-name }} CLI.</li></ul> |
| `--stdin-format` | Format of parameter values for `stdin`.<br/>The {{ ydb-short-name }} CLI automatically detects that a file or an output of another shell command has been redirected to the standard input device `stdin`. In this case, the CLI interprets the incoming data based on the following available options:<ul><li>`json-unicode`: [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %}.</li><li>`json-base64`: [JSON]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/JSON){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/JSON){% endif %} with values of binary string parameters (`DECLARE $par AS String`) are [Base64]{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Base64){% endif %}{% if lang == "en" %}(https://en.wikipedia.org/wiki/Base64){% endif %}-encoded.</li><li>`raw`: Binary data.</li></ul>If format of parameter values for `stdin` isn't specified, the `--input-format` is used. |
| `--stdin-par` | Name of a parameter whose value is provided on `stdin`, without a `$` sign. This name is required when you use the `raw` format in `--stdin-format`.<br/><br/>When used with JSON formats, `stdin` is interpreted not as a JSON document but as a JSON value passed to the parameter with the specified name. |

The query will be executed on the server once, provided that values are specified for all the parameters [in the `DECLARE` clause](../../yql/reference/syntax/declare.md). If a value is absent for at least one parameter, the command fails with the "Missing value for parameter" message.

### Examples {#examples-one-request}

In our examples, we use the `table query execute` command, but you can also run them using the `yql` and `scripting yql` commands.

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

#### Passing the value of a single parameter {#example-simple}

From the command line:

```bash
{{ ydb-cli }} -p quickstart table query execute -q 'declare $a as Int64;select $a' --param '$a=10'
```

Using a file:

```bash
echo '{"a":10}' > p1.json
{{ ydb-cli }} -p quickstart table query execute -q 'declare $a as Int64;select $a' --param-file p1.json
```

Through `stdin`:

```bash
echo '{"a":10}' | {{ ydb-cli }} -p quickstart table query execute -q 'declare $a as Int64;select $a'
```

```bash
echo '10' | {{ ydb-cli }} -p quickstart table query execute -q 'declare $a as Int64;select $a' --stdin-par a
```

#### Passing the values of parameters of different types from multiple sources {#example-multisource}

```bash
echo '{ "a":10, "b":"Some text", "x":"Ignore me" }' > p1.json
echo '{ "c":"2012-04-23T18:25:43.511Z" }' | {{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $a as Int64;
      declare $b as Utf8;
      declare $c as DateTime;
      declare $d as Int64;

      select $a, $b, $c, $d' \
  --param-file p1.json \
  --param '$d=30'
```

Command output:

```text
┌─────────┬─────────────┬────────────────────────┬─────────┐
| column0 | column1     | column2                | column3 |
├─────────┼─────────────┼────────────────────────┼─────────┤
| 10      | "Some text" | "2012-04-23T18:25:43Z" | 30      |
└─────────┴─────────────┴────────────────────────┴─────────┘
```

#### Passing Base64-encoded binary strings {#example-base64}

```bash
{{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $a as String;
      select $a' \
  --input-format json-base64 \
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
curl -Ls http://ydb.tech/docs | {{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $a as String;
      select LEN($a)' \
  --stdin-format raw \
  --stdin-par a
```

Command output (exact number of bytes may vary):

```text
┌─────────┐
| column0 |
├─────────┤
| 66426   |
└─────────┘
```

## Iterative streaming processing {#streaming-iterate}

{{ ydb-short-name }} CLI supports execution of a YQL query multiple times with different sets of parameter values provided on `stdin`. In this case, the database connection is established once and the query execution plan is cached. This substantially increases the performance of such an approach compared to separate CLI calls.

To use this feature, you need to stream different sets of the same parameters to `stdin` one after another, specifying a rule for the {{ ydb-short-name }} CLI on how to separate the sets from each other.

The YQL query runs as many times as many parameter value sets received on `stdin`. Each set received on `stdin` is joined with the parameter values defined on other sources (`--param`, `--param-file`). The command will complete once the `stdin` stream is closed. Each query is executed within a dedicated transaction.

A rule for separating parameter sets from one another (framing) complements the `stdin` format specified by the `--stdin-format` option:

| Name | Description |
---|---
| `--stdin-format` | Defines the `stdin` framing. <br/>Available options:<ul><li>`no-framing` (default): No framing, `stdin` expects a single set of parameters, and the YQL query is executed only once when the `stdin` stream is closed.</li><li>`newline-delimited`: A newline character marks the end of one set of parameter values on `stdin`, separating it from the next one. The YQL query is executed each time a newline character is read from `stdin`.</li></ul> |

{% note warning %}

When using a newline character as a separator between the parameter sets, make sure that it isn't used inside the parameter sets. Putting some text value in quotes does not enable newlines within the text. Multiline JSON documents are not allowed.

{% endnote %}

### Example {#example-streaming-iterate}

#### Streaming processing of multiple parameter sets {#example-iterate}

Suppose you need to run your query thrice, with the following sets of values for the `a` and `b` parameters:

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
{{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $a as Int64;
      declare $b as Int64;
      select $a+$b' \
  --stdin-format newline-delimited \
  --format json-unicode
```

Command output:

```text
{"column0":30}
{"column0":40}
{"column0":83}
```

This output can be passed as input to the next YQL query command.

#### Streaming processing with joining parameter values from different sources {#example-iterate-union}

For example, you need to run your query thrice, with the following sets of values for the `a` and `b` parameters:

1. `a` = 10, `b` = 100
2. `a` = 15, `b` = 100
3. `a` = 35, `b` = 100

```bash
echo -e '10\n15\n35' | \
{{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $a as Int64;
      declare $b as Int64;
      select $a+$b as sum1' \
  --param '$b=100' \
  --stdin-format newline-delimited \
  --stdin-par a \
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

The `full` mode is a simplified batch mode where the query runs only once, and all the parameter sets received through `stdin` are wrapped into a `List<>`. If the request is too large, you will get an error.

Use this batch mode when you want to ensure transaction atomicity by applying all the parameters within a single transaction.

### Adaptive batch mode {#batch-adaptive}

In the `adaptive` mode, the input stream is split into multiple transactions, with the batch size automatically determined for each of them.

In this mode, you can process a broad range of dynamic workloads with unpredictable or infinite amounts of data, as well as with unpredictable or significantly varying rate of new sets appearance at the input. For example, such a profile is typical when sending the output of another command to `stdin` using the `|` operator.

The adaptive mode solves two basic issues of dynamic stream processing:

1. Limiting the maximum batch size.
2. Limiting the maximum data processing delay.

### Syntax {#batch-syntax}

To use the batching capbilities, define the `List<...>` or `List<Struct<...>>` parameter in the YQL query's DECLARE clause, and use the following options:

| Name | Description |
---|---
| `--batch` | The batch mode applied to parameter sets on `stdin`.<br/>Available options:<ul><li>`iterative` (default): Batching is [disabled](#streaming-iterate).</li><li>`full`: Full batch mode. The YQL query runs only once when `stdin` is closed, with all the received sets of parameters wrapped into `List<>`, the parameter name is set by the `--stdin-par` option.</li><li>`adaptive`: Adaptive batch mode. The YQL query runs every time when limits are exceeded either on the number of parameter sets per query (`--batch-limit`) or on the batch processing delay (`--batch-max-delay`). All the sets of parameters received by that moment are wrapped into a `List<>`, the parameter name is set by the `--stdin-par` option. |

In the adaptive batch mode, you can use the following additional parameters:

| Name | Description |
---|---
| `--batch-limit` | The maximum number of sets of parameters per batch in the adaptive batch mode. The next batch will be sent to the YQL query if the number of parameter sets in it reaches the specified limit. When it's `0`, there's no limit.<br/><br/>Default value: `1000`.<br/><br/>Parameter values are sent to each YQL execution without streaming, so the total size per GRPC request that includes the parameter values has the upper limit of about 5 MB. |
| `--batch-max-delay` | The maximum delay to submit a received parameter set for processing in the adaptive batch mode. It's set as a number with a time unit - `s`, `ms`, `m`.<br/><br/>Default value: `1s` (1 second).<br/><br/>The {{ ydb-short-name }} CLI starts a timer when it receives a first set of parameters for the batch on `stdin`, and sends the whole accumulated batch for execution once the timer expires. With this parameter, you can batch efficiently when new parameter sets arrival rate on `stdin` is unpredictable. |

### Examples: Full batch processing {#example-batch-full}

```bash
echo -e '{"a":10,"b":20}\n{"a":15,"b":25}\n{"a":35,"b":48}' | \
{{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $x as List<Struct<a:Int64,b:Int64>>;
      select ListLength($x), $x' \
  --stdin-format newline-delimited \
  --stdin-par x \
  --batch full
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

This example demonstrates the adaptive batching triggered by a processing delay. In the first line of the command below, we generate 1,000 rows at a delay of 0.2 seconds on `stdout` and pipe them to `stdin` to the YQL query execution command. The YQL query execution command shows the parameter batches in each subsequent YQL query call.

```bash
for i in $(seq 1 1000);do echo "Line$i";sleep 0.2;done | \
{{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $x as List<Utf8>;
      select ListLength($x), $x' \
  --stdin-format newline-delimited \
  --stdin-format raw \
  --stdin-par x \
  --batch adaptive
```

Command output (actual values may differ):

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

The first batch includes all the rows accumulated at the input while the database connection has had been establishing, that's why it's larger than the next ones.

You can terminate the command by Ctrl+C or wait 200 seconds until the input generation is finished.

#### Limit on the number of records {#example-adaptive-limit}

This example demonstrates the adaptive batching triggered by a number of parameter sets. In the first line of the command below, we generate 200 rows. The command will show parameter batches in each subsequent YQL query call, applying the given limit `--batch-limit` of 20 (the default limit is 1,000).

In this example, we also demonstrate the option to join parameters from different sources and generate JSON at the output.

```bash
for i in $(seq 1 200);do echo "Line$i";done | \
{{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $x as List<Utf8>;
      declare $p2 as Int64;
      select ListLength($x) as count, $p2 as p2, $x as items' \
  --stdin-format newline-delimited \
  --stdin-format raw \
  --stdin-par x \
  --batch adaptive \
  --batch-limit 20 \
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

#### Deleting multiple records from a YDB table based on primary keys {#example-adaptive-delete-pk}

This example shows how you can delete an unlimited number of records from YDB tables without risking exceeding the limit on the number of records per transaction.

Let's create a test table:

```bash
{{ ydb-cli }} -p quickstart yql -s 'create table test_delete_1( id UInt64 not null, primary key (id))'
```

Add 100,000 records to it:

```bash
for i in $(seq 1 100000);do echo "$i";done | \
{{ ydb-cli }} -p quickstart import file csv -p test_delete_1
```

Delete all records with ID > 10:

```bash
{{ ydb-cli }} -p quickstart table query execute -t scan \
  -q 'select t.id from test_delete_1 as t where t.id > 10' \
  --format json-unicode | \
{{ ydb-cli }} -p quickstart table query execute \
  -q 'declare $lines as List<Struct<id:UInt64>>;
      delete from test_delete_1 where id in (select tl.id from AS_TABLE($lines) as tl)' \
  --stdin-format newline-delimited \
  --stdin-par lines \
  --batch adaptive \
  --batch-limit 10000
```

#### Processing of messages read from a topic {#example-adaptive-pipeline-from-topic}

Examples of processing messages read from a topic are given in [{#T}](topic-pipeline.md#example-read-to-yql-param).

## See also {#see-also}

* [Parameterized YQL queries in {{ ydb-short-name }} SDK](../ydb-sdk/parameterized_queries.md)
