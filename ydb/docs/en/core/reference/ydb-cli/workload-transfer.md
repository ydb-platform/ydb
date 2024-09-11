# Transfer load

Starts the load in the form of transactions {{ ydb-short-name }} involving topics and tables simultaneously. The data is read from the topic and written to the table. To simulate a real load, you can set various input parameters: the number of messages, the size of messages, the target write speed, the number of consumers and producers, the number of partitions. During operation, the console displays the results: the number of written messages, the speed of writing messages, etc.

{% include [ydb-cli-profile.md](../../_includes/ydb-cli-profile.md) %}

## Initializing the test environment {#init}

Before starting the load, it is necessary to initialize the test environment. You can use the command `{{ ydb-cli }} workload transfer topic-to-table init` to do this. It will create a topic and a table with the necessary parameters.

Command syntax:
```bash
{{ ydb-cli }} [global options...] workload transfer topic-to-table init [options...]
```
* `global options` — [global parameters](commands/global-options.md).
* `options` - parameters of the subcommand.

View the command description:

```bash
{{ ydb-cli }} workload transfer topic-to-table init --help
```

Parameters of the subcommand:
Parameter name | Parameter description | Default value
---|---|---
`--topic` | Topic name | `transfer-topic`
`--consumer-prefix` | Prefix of the consumers name | `workload-consumer`
`--table` | Table name | `transfer-table`
`--consumers` | Number of topic consumers | `1`
`--topic-partitions` | Number of topic partitions | `128`
`--table-partitions` | Number of table partitions | `128`

After executing the `init` subcommand, a table, topic and consumers will be created. Reader names are created by the rule `${CONSUMER_PREFIX}-${INDEX}`. The value of `${INDEX}` is an integer from 0 to the value of the parameter `--consumers` minus 1.

For example, the command `{{ ydb-cli }} --profile quickstart workload transfer topic-to-table init --consumers 2 --topic-partitions 143 --table-partitions 237` will create a topic `transfer-topic` with 2 consumers, 143 partitions, and a table `transfer-table` with 237 partitions. The consumer names are `workload-consumer-0` and `workload-consumer-1`.

## Running a load test {#run}

The test simulates the load from an application that receives messages from a topic, processes them and writes the processing results to a database table.

During the operation of the program, two types of work streams are simulated:
* Input stream: messages are written to the topic in the non-transaction mode. The user can control the writing speed, the message size, the number of producers.
* Processing flow: messages are read from the topic and written to the table using the {{ ydb-short-name }} transaction.

The following actions are performed in the processing flow within a single transaction:
* messages from the topic are being read until the `--commit-period` period has expired;
* one `UPSERT` command and a `COMMIT` command are executed on the table to commit the transaction after the period expires.

Command syntax:
```bash
{{ ydb-cli }} [global options...] workload transfer topic-to-table run [options...]
```
* `global options` — [global parameters](commands/global-options.md).
* `options` - parameters of the subcommand.

View the command description:

```bash
{{ ydb-cli }} workload transfer topic-to-table run --help
```

Parameters of the subcommand:
Parameter name | Parameter Description | Default value
---|---|---
`--seconds`, `-s` | Duration of the test in seconds | `60`
`--window`, `-w` | Duration of the statistics collection window in seconds | `1`
`--quiet`, `-q` | Output only the final test result | `0`
`--print-timestamp` | Print the time together with the statistics of each time window | `0`
`--percentile` | Percentile in statistics output | `50`
`--warmup` | The warm-up time of the test in seconds. No statistics are calculated during this time | `5`
`--topic` | Topic name | `transfer-topic`
`--consumer-prefix` | Prefix of the consumers name | `workload-consumer`
`--table` | Table name | `transfer-table`
`--producer-threads`, `-p` | Number of producer threads | `1`
`--consumer-threads`, `-t` | Number of consumer threads | `1`
`--consumers`, `-c` | Number of consumers | `1`
`--message-size`, `-m` | Message size in bytes. It is possible to specify in KB, MB, GB by adding suffixes `K`, `M`, `G` respectively | `10240`
`--message-rate` | Target total write speed. In messages per second. Excludes the use of the `--byte-rate` parameter | `0`
`--byte-rate` | Target total write speed. In bytes per second. Excludes the use of the `--message-rate` parameter. It is possible to specify in KB/s, MB/s, GB/s by adding suffixes `K`, `M`, `G` respectively | `0`
`--commit-period` | The period between `COMMIT` calls. In seconds | `10`
`--commit-messages` | The period between `COMMIT` calls. In number of messages | `1000000`
`--only-topic-in-tx` | Only topic partitions are forced to participate in transactions. Excludes the use of the `--only-table-in-tx` parameter | `0`
`--only-table-in-tx` | Only table shards are forced to participate in transactions. Excludes the use of the `--only-topic-in-tx` parameter | `0`

For example, the command `{{ ydb-cli }} --profile quickstart workload transfer topic-to-table run` will run a test lasting 60 seconds. The data for the first 5 seconds will not be taken into account in the work statistics. Example of console output:

```text
Window  Write speed     Write time      Inflight        Read speed      Topic time      Select time     Upsert time     Commit time
#       msg/s   MB/s    percentile,ms   percentile,msg  msg/s   MB/s    percentile,ms   percentile,ms   percentile,ms   percentile,ms
1       0       0       0               0               0       0       0               0               0               0
2       0       0       0               0               0       0       0               0               0               0
3       0       0       0               0               0       0       0               0               0               0
4       0       0       0               0               0       0       0               0               0               0
5       0       0       0               0               0       0       0               0               0               0
6       103     1       1023            83              103     1       1025            0               0               0
7       103     1       999             78              103     1       1001            0               0               0
8       103     1       1003            93              103     1       1002            0               0               0
9       103     1       1003            88              103     1       1003            0               0               0
10      103     1       999             79              103     1       999             0               0               0
11      103     1       1119            89              0       0       0               0               0               0
12      103     1       1023            90              206     2       1028            90              223             695
13      103     1       975             84              103     1       976             0               0               0
14      103     1       1003            91              103     1       1006            0               0               0
15      103     1       1003            93              103     1       1005            0               0               0
16      103     1       1103            89              103     1       1100            0               0               0
17      103     1       1063            89              103     1       1061            0               0               0
...
```

* `Window` — the serial number of the time window for collecting statistics.
* `Write speed` — the speed of writing messages by producers. In messages per second and in megabytes per second.
* `Write time` — the specified percentile of the message writing time in ms.
* `Inflight` — the maximum number of messages waiting for confirmation for all batches.
* `Lag` — the specified percentile of maximum number of messages waiting to be read in the statistics collection window. Messages for all batches are taken into account.
* `Lag time` — the specified percentile of message delay time in ms.
* `Read speed` — the speed of reading messages by consumers. In messages per second and in megabytes per second.
* `Select time`, `Upsert time`, `Commit time` — the specified percentile of the execution time of Select, Insert, Commit operations in ms.
<!-- * `Full time` — the specified percentile of the time of complete processing of the message, from writing by the producer to reading by the consumer in ms. -->

## Removing the test environment {#clean}

After the test is completed, you can delete the test environment.

Command syntax:
```bash
{{ ydb-cli }} [global options...] workload transfer topic-to-table clean [options...]
```
* `global options` — [global parameters](commands/global-options.md).
* `options` - parameters of the subcommand.

View the command description:

```bash
{{ ydb-cli }} workload transfer topic-to-table clean --help
```

Parameters of the subcommand:
Parameter Name | Parameter Description | Default value
---|---|---
`--topic` | Topic name | `transfer-topic`
`--table` | Table name | `transfer-table`

For example, the command `{{ ydb-cli }} --profile quickstart workload transfer topic-to-table clean` will delete the topic `transfer-topic`, its consumers and the table `transfer-table`.
