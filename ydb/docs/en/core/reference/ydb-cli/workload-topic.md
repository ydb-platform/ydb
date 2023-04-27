# Topic workload

The workload simulates the publish-subscribe architectural pattern using [YDB topics](../../concepts/topic.md).

The workload allow us to generate and consume high volumes of data through your YDB cluster in order to measure it's performance characteristics such as throughout and latency.

The tests can be configured to match your real workloads. The number of messages, message sizes, target throughput, the number of producers and consumers can be adjusted.

Test outputs include the number of messages and the amount of data transferred including latencies in order to understand the real world performance characteristics of your cluster.

## Types of load{#workload-types}

The load test runs 3 types of load:

* [write](#run-write) — generate messages and write them to a topic asynchronously;
* [read](#run-read) — read messages from a topic asynchronously;
* [full](#run-full) — simultaneously read and write messages.

## Load initialization {#init}

To get started, create a topic:

```bash
{{ ydb-cli }} workload topic init [init options...]
```

* `init options` — [Initialization options](#init-options).

See the description of the command to init the data load:

```bash
{{ ydb-cli }} workload topic init --help
```

### Available parameters {#init-options}

Parameter name | Short name | Parameter description
---|---|---
`--partitions <value>` | `-p <value>` | Number of partitions in the topic. Default: 128.
`--consumers <value>` | `-c <value>` | Number of consumers in the topic. Default: 1.

The `workload-topic` topic will be created with the specified numbers of partitions and consumers.

### Load initialization example{#init-topic-examples}

Creating a topic with 256 partitions and 2 consumers:

```bash
{{ ydb-cli }} workload topic init --partitions 256 --consumers 2
```

## Clean {#clean}

When the workload is complete, you can delete the `workload-topic` topic:

```bash
{{ ydb-cli }} workload topic clean
```

### Clean example {#clean-topic-examples}

```bash
{{ ydb-cli }} workload topic clean
```

## Running a load test {#run}

To run the load, execute the command:

```bash
{{ ydb-cli }} workload topic run [workload type...] [specific workload options...]
```

* `workload type` — [The types of workload](#workload-types).
* `global workload options` — [The global options for all types of load.](#global-workload-options).
* `specific workload options` — Options of a specific load type.

See the description of the command to run the workload:

```bash
{{ ydb-cli }} workload topic run --help
```

### The global options for all types of load {#global-workload-options}

Parameter name | Short name | Parameter description
---|---|---
`--seconds <value>` | `-s <value>` | Duration of the test (seconds). Default: 10.
`--window <value>` | `-w <value>` | Statistics collection window (seconds). Default: 1.
`--quiet` | `-q` | Outputs only the total result.
`--print-timestamp` | - | Print the time together with the statistics of each time window.

## Write workload {#run-write}

This load type generate messages and send them into a topic asynchronously.

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload topic run write [global workload options...] [specific workload options...]
```

See the description of the command to run the write workload:

```bash
{{ ydb-cli }} workload topic run write --help
```

### Write workload options {#run-write-options}

Parameter name | Short name | Parameter description
---|---|---
`--threads <value>` | `-t <value>` | Number of producer threads. Default: `1`.
`--message-size <value>` | `-m <value>` | Message size (bytes). It can be specified in KB, MB, GB using one of suffixes: `K`, `M`, `G`. Default: `10K`.
`--message-rate <value>` | - | Total message rate for all producer threads (messages per second). 0 - no limit. Default: `0`.
`--byte-rate <value>` | - | Total message rate for all producer threads (bytes per second). 0 - no limit. It can be specified in KB/s, MB/s, GB/s using one of suffixes: `K`, `M`, `G`. Default: `0`.
`--codec <value>` | - | Codec used for message compression on the client before sending them to the server. Possible values: `RAW` (no compression), `GZIP`, and `ZSTD`. Compression causes higher CPU utilization on the client when reading and writing messages, but usually lets you reduce the volume of data transferred over the network and stored. When consumers read messages, they're automatically decompressed with the codec used when writing them, without specifying any special options. Default: `RAW`.

Note: The options `--byte-rate` и `--message-rate` are mutually exclusive.

### Write workload example{#run-write-examples}

Example of a command to create 100 producer threads with a target speed of 80 MB/s and a duration of 300 seconds:

```bash
{{ ydb-cli }} workload topic run write --threads 100 --seconds 300 --byte-rate 80M
```

### Write workload output {#run-write-output}

During the process of work, both intermediate and total statistics are printed. Example output:

```text
Window  Write speed     Write time      Inflight
#       msg/s   MB/s    P99(ms)         max msg
1       20      0       1079            72
2       8025    78      1415            78
3       7987    78      1431            79
4       7888    77      1471            101
5       8126    79      1815            116
6       7018    68      1447            79
7       8938    87      2511            159
8       7055    68      1463            78
9       7062    69      1455            79
10      9912    96      3679            250
Window  Write speed     Write time      Inflight
#       msg/s   MB/s    P99(ms)         max msg
Total   7203    70      3023            250
```

Column name | Column description
---|---
`Window`|The time window counter.
`Write speed`|Write speed (messages/s and Mb/s).
`Write time`|99 percentile of message write time (ms).
`Inflight`|The maximum count of inflight messages.

## Read workload {#run-read}

This load type read messages from a topic asynchronously.

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload topic run read [global workload options...] [specific workload options...]
```

See the description of the command to run the read workload:

```bash
{{ ydb-cli }} workload topic run read --help
```

### Read workload options {#run-read-options}

Parameter name | Short name | Parameter description
---|---|---
`--consumers <value>` | `-c <value>` | Number of consumers in the topic. Default: `1`.
`--threads <value>` | `-t <value>` | Number of consumer threads. Default: `1`.

### Read workload example {#run-read-examples}

Example of a command to create 2 consumers with 100 threads each:

```bash
{{ ydb-cli }} workload topic run read --consumers 2 --threads 100
```

### Read workload output {#run-read-output}

During the process of work, both intermediate and total statistics are printed. Example output:

```text
Window  Lag     Lag time        Read speed      Full time
#       max msg P99(ms)         msg/s   MB/s    P99(ms)
1       0       0               48193   471     0
2       30176   0               66578   650     0
3       30176   0               68999   674     0
4       30176   0               66907   653     0
5       27835   0               67628   661     0
6       30176   0               67938   664     0
7       30176   0               71628   700     0
8       20338   0               61367   599     0
9       30176   0               61770   603     0
10      30176   0               58291   569     0
Window  Lag     Lag time        Read speed      Full time
#       max msg P99(ms)         msg/s   MB/s    P99(ms)
Total   30176   0               80267   784     0
```

Column name | Column description
---|---
`Window`|The time window counter.
`Lag`|The maximum lag between producers and consumers across all the partitions (messages).
`Lag time`|99 percentile of message delay time (ms).
`Read`|Read speed (messages/s and MB/s).
`Full time`|99 percentile of the end-to-end message time, from writing by the producer to reading by the reader.

## Full workload {#run-full}

This load type both write and read messages asynchronously.

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload topic run full [global workload options...] [specific workload options...]
```

This command is equivalent to running both read and write load workloads simultaneously.

See the description of the command to run the full workload:

```bash
{{ ydb-cli }} workload topic run full --help
```

### Full workload options {#run-full-options}

Parameter name | Short name | Parameter description
---|---|---
`--producer-threads <value>` | `-p <value>` | Number of producer threads. Default: `1`.
`--message-size <value>` | `-m <value>` | Message size (bytes). It can be specified in KB, MB, GB using one of suffixes: `K`, `M`, `G`. Default: `10K`.
`--message-rate <value>` | - | Total message rate for all producer threads (messages per second). 0 - no limit. Default: `0`.
`--byte-rate <value>` | - | Total message rate for all producer threads (bytes per second). 0 - no limit. It can be specified in KB/s, MB/s, GB/s using one of suffixes: `K`, `M`, `G`. Default: `0`.
`--codec <value>` | - | Codec used for message compression on the client before sending them to the server. Possible values: `RAW` (no compression), `GZIP`, and `ZSTD`. Compression causes higher CPU utilization on the client when reading and writing messages, but usually lets you reduce the volume of data transferred over the network and stored. When consumers read messages, they're automatically decompressed with the codec used when writing them, without specifying any special options. Default: `RAW`.
`--consumers <value>` | `-c <value>` | Number of consumers in the topic. Default: `1`.
`--threads <value>` | `-t <value>` | Number of consumer threads. Default: `1`.

Note: The options `--byte-rate` и `--message-rate` are mutually exclusive.

### Full workload example {#run-full-examples}

Example of a command to create 100 producer threads, 2 consumers the 50 consumer thread,a target speed of 80 MB/s and a duration of 300 seconds:

```bash
{{ ydb-cli }}  workload topic run full --producer-threads 100 --consumers 2 --consumer-threads 50 --byte-rate 80M --seconds 300
```

### Ful workload output {#run-full-output}

During the process of work, both intermediate and total statistics are printed. Example output:

```text
Window  Write speed     Write time      Inflight        Lag     Lag time        Read speed      Full time
#       msg/s   MB/s    P99(ms)         max msg         max msg P99(ms)         msg/s   MB/s    P99(ms)
1       39      0       1215            4               0       0               30703   300     29716
2       1091    10      2143            8               2076    20607           40156   392     30941
3       1552    15      2991            12              7224    21887           41040   401     31886
4       1733    16      3711            15              10036   22783           38488   376     32577
5       1900    18      4319            15              10668   23551           34784   340     33372
6       2793    27      5247            21              9461    24575           33267   325     34893
7       2904    28      6015            22              12150   25727           34423   336     35507
8       2191    21      5087            21              12150   26623           29393   287     36407
9       1952    19      2543            10              7627    27391           33284   325     37814
10      1992    19      2655            9               10104   28671           29101   284     38797
Window  Write speed     Write time      Inflight        Lag     Lag time        Read speed      Full time
#       msg/s   MB/s    P99(ms)         max msg         max msg P99(ms)         msg/s   MB/s    P99(ms)
Total   1814    17      5247            22              12150   28671           44827   438     40252
```

Column name | Column description
---|---
`Window`|The time window counter.
`Write speed`|Write speed (messages/s and Mb/s).
`Write time`|99 percentile of message write time (ms).
`Inflight`|The maximum count of inflight messages.
`Lag`|The maximum lag between producers and consumers across all the partitions (messages).
`Lag time`|99 percentile of message delay time (ms).
`Read`|Read speed (messages/s and MB/s).
`Full time`|99 percentile of the end-to-end message time, from writing by the producer to reading by the reader.
