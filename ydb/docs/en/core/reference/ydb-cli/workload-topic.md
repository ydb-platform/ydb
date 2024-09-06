# Topic load

Applies load to your {{ ydb-short-name }} [topics](../../concepts/topic.md), using them as message queues. You can use a variety of input parameters to simulate production load: message number, message size, target write rate, and number of consumers and producers.

As you apply load to your topic, the console displays the results (the number of written messages, message write rate, and others).

To generate load against your topic:

1. [Initialize the load](#init).
1. Run one of the available load types:
   * [write](#run-write): Generate messages and write them to the topic asynchronously.
   * [read](#run-read): Read messages from the topic asynchronously.
   * [full](#run-full): Read and write messages asynchronously in parallel.

{% include [ydb-cli-profile.md](../../_includes/ydb-cli-profile.md) %}

## Initializing a load test {#init}

Before executing the load, you need to initialize it. During initialization, you will create a topic named `workload-topic` with the specified options. To initialize the load, run the following command:

```bash
{{ ydb-cli }} [global options...] workload topic init [options...]
```

* `global options`: [Global options](commands/global-options.md).
* `options`: Subcommand options.

Subcommand options:

| Option name | Option description |
---|---
| `--topic` | Topic name.<br/>Default value: `workload-topic`. |
| `--partitions`, `-p` | Number of topic partitions.<br/>Default value: `128`. |
| `--consumers`, `-c` | Number of topic consumers.<br/>Default value: `1`. |
| `--consumer-prefix` | Consumer name prefix.<br/>Default value: `workload-consumer`.<br/>For example, if the number of consumers `--consumers` is `2` and the prefix `--consumer-prefix` is `workload-consumer`, then the following consumer names will be used: `workload-consumer-0`, `workload-consumer-1`. |

> To create a topic with `256` partitions and `2` consumers, run this command:
>
> ```bash
> {{ ydb-cli }} --profile quickstart workload topic init --partitions 256 --consumers 2
> ```

## Write load {#run-write}

This load type generates and writes messages to the topic asynchronously.

General format of the command that generates the write load:

```bash
{{ ydb-cli }} [global options...] workload topic run write [options...]
```

* `global options`: [Global options](commands/global-options.md).
* `options`: Subcommand options.

View the description of the command that generates the write load:

```bash
{{ ydb-cli }} workload topic run write --help
```

Subcommand options:

| Option name | Option description |
---|---
| `--seconds`, `-s` | Test duration in seconds.<br/>Default value: `60`. |
| `--window`, `-w` | Statistics window in seconds.<br/>Default value: `1`. |
| `--quiet`, `-q` | Output only the final test result. |
| `--print-timestamp` | Print the time together with the statistics of each time window. |
| `--warmup` | Test warm-up period (in seconds).<br/>Within the period, no statistics are calculated. It's needed to eliminate the effect of transition processes at startup.<br/>Default value: `5`. |
| `--percentile` | Percentile that is output in statistics.<br/>Default value: `50`. |
| `--topic` | Topic name.<br/>Default value: `workload-topic`. |
| `--threads`, `-t` | Number of producer threads.<br/>Default value: `1`. |
| `--message-size`, `-m` | Message size in bytes. Use the `K`, `M`, or `G` suffix to set the size in KB, MB, or GB, respectively.<br/>Default value: `10K`. |
| `--message-rate` | Total target write rate in messages per second. Can't be used together with the `--byte-rate` option.<br/>Default value: `0` (no limit). |
| `--byte-rate` | Total target write rate in bytes per second. Can't be used together with the `--message-rate` option. Use the `K`, `M`, or `G` suffix to set the rate in KB/s, MB/s, or GB/s, respectively.<br/>Default value: `0` (no limit). |
| `--codec` | Codec used to compress messages on the client before sending them to the server.<br/>Compression increases CPU usage on the client when reading and writing messages, but usually enables you to reduce the amounts of data stored and transmitted over the network. When consumers read messages, they decompress them by the codec that was used to write the messages, with no special options needed.<br/>Acceptable values: `RAW` - no compression (default), `GZIP`, `ZSTD`. |

> To write data to `100` producer threads at the target rate of `80` MB/s for `10` seconds, run this command:
>
> ```bash
> {{ ydb-cli }} --profile quickstart workload topic run write --threads 100 --byte-rate 80M
> ```
>
> You will see statistics for in-progress time windows and final statistics when the test is complete:
>
> ```text
> Window  Write speed     Write time      Inflight
> #       msg/s   MB/s    percentile,ms   percentile,msg
> 1       20      0       1079            72
> 2       8025    78      1415            78
> 3       7987    78      1431            79
> 4       7888    77      1471            101
> 5       8126    79      1815            116
> 6       7018    68      1447            79
> 7       8938    87      2511            159
> 8       7055    68      1463            78
> 9       7062    69      1455            79
> 10      9912    96      3679            250
> Window  Write speed     Write time      Inflight
> #       msg/s   MB/s    percentile,ms   percentile,msg
> Total   7203    70      3023            250
> ```
>
> * `Window`: Sequence number of the statistics window.
> * `Write speed`: Message write rate in messages per second and MB/s.
> * `Write time`: Percentile of the message write time, in milliseconds.
> * `Inflight`: Maximum number of messages awaiting commit across all partitions.

## Read load {#run-read}

This type of load reads messages from the topic asynchronously. To make sure that the topic includes messages, run the [write load](#run-write) before you start reading.

General format of the command to generate the read load:

```bash
{{ ydb-cli }} [global options...] workload topic run read [options...]
```

* `global options`: [Global options](commands/global-options.md).
* `options`: Subcommand options.

View the description of the command to generate the read load:

```bash
{{ ydb-cli }} workload topic run read --help
```

Subcommand options:

| Option name | Option description |
---|---
| `--seconds`, `-s` | Test duration in seconds.<br/>Default value: `60`. |
| `--window`, `-w` | Statistics window in seconds.<br/>Default value: `1`. |
| `--quiet`, `-q` | Output only the final test result. |
| `--print-timestamp` | Print the time together with the statistics of each time window. |
| `--warmup` | Test warm-up period (in seconds).<br/>Within the period, no statistics are calculated. It's needed to eliminate the effect of transition processes at startup.<br/>Default value: `5`. |
| `--percentile` | Percentile that is output in statistics.<br/>Default value: `50`. |
| `--topic` | Topic name.<br/>Default value: `workload-topic`. |
| `--consumers`, `-c` | Number of consumers.<br/>Default value: `1`. |
| `--consumer-prefix` | Consumer name prefix.<br/>Default value: `workload-consumer`.<br/>For example, if the number of consumers `--consumers` is `2` and the prefix `--consumer-prefix` is `workload-consumer`, then the following consumer names will be used: `workload-consumer-0`, `workload-consumer-1`. |
| `--threads`, `-t` | Number of consumer threads.<br/>Default value: `1`. |

> To use `2` consumers to read data from the topic, with `100` threads per consumer, run the following command:
>
> ```bash
> {{ ydb-cli }} --profile quickstart workload topic run read --consumers 2 --threads 100
> ```
>
> You will see statistics for in-progress time windows and final statistics when the test is complete:
>
> ```text
> Window  Lag             Lag time        Read speed      Full time
> #       percentile,msg  percentile,ms   msg/s   MB/s    percentile,ms
> 1       0               0               0       0       0
> 2       30176           0               66578   650     0
> 3       30176           0               68999   674     0
> 4       30176           0               66907   653     0
> 5       27835           0               67628   661     0
> 6       30176           0               67938   664     0
> 7       30176           0               71628   700     0
> 8       20338           0               61367   599     0
> 9       30176           0               61770   603     0
> 10      30176           0               58291   569     0
> Window  Lag             Lag time        Read speed      Full time
> #       percentile,msg  percentile,ms   msg/s   MB/s    percentile,ms
> Total   30176           0               80267   784     0
> ```
>
> * `Window`: Sequence number of the statistics window.
> * `Lag`: Maximum consumer lag in the statistics window. Messages across all partitions are included.
> * `Lag time`: Percentile of the message lag time in milliseconds.
> * `Read`: Message read rate for the consumer (in messages per second and MB/s).
> * `Full time`: Percentile of the full message processing time (from writing by the producer to reading by the consumer), in milliseconds.

## Read and write load {#run-full}

This type of load both reads messages from the topic and writes them to the topic asynchronously. This command is equivalent to running both read and write loads in parallel.

General format of the command to generate the read and write load:

```bash
{{ ydb-cli }} [global options...] workload topic run full [options...]
```

* `global options`: [Global options](commands/global-options.md).
* `options`: Subcommand options.

View the description of the command to run the read and write load:

```bash
{{ ydb-cli }} workload topic run full --help
```

Subcommand options:

| Option name | Option description |
---|---
| `--seconds`, `-s` | Test duration in seconds.<br/>Default value: `60`. |
| `--window`, `-w` | Statistics window in seconds.<br/>Default value: `1`. |
| `--quiet`, `-q` | Output only the final test result. |
| `--print-timestamp` | Print the time together with the statistics of each time window. |
| `--warmup` | Test warm-up period (in seconds).<br/>Within the period, no statistics are calculated. It's needed to eliminate the effect of transition processes at startup.<br/>Default value: `5`. |
| `--percentile` | Percentile that is output in statistics.<br/>Default value: `50`. |
| `--topic` | Topic name.<br/>Default value: `workload-topic`. |
| `--producer-threads`, `-p` | Number of producer threads.<br/>Default value: `1`. |
| `--message-size`, `-m` | Message size in bytes. Use the `K`, `M`, or `G` suffix to set the size in KB, MB, or GB, respectively.<br/>Default value: `10K`. |
| `--message-rate` | Total target write rate in messages per second. Can't be used together with the `--message-rate` option.<br/>Default value: `0` (no limit). |
| `--byte-rate` | Total target write rate in bytes per second. Can't be used together with the `--byte-rate` option. Use the `K`, `M`, or `G` suffix to set the rate in KB/s, MB/s, or GB/s, respectively.<br/>Default value: `0` (no limit). |
| `--codec` | Codec used to compress messages on the client before sending them to the server.<br/>Compression increases CPU usage on the client when reading and writing messages, but usually enables you to reduce the amounts of data stored and transmitted over the network. When consumers read messages, they decompress them by the codec that was used to write the messages, with no special options needed.<br/>Acceptable values: `RAW` - no compression (default), `GZIP`, `ZSTD`. |
| `--consumers`, `-c` | Number of consumers.<br/>Default value: `1`. |
| `--consumer-prefix` | Consumer name prefix.<br/>Default value: `workload-consumer`.<br/>For example, if the number of consumers `--consumers` is `2` and the prefix `--consumer-prefix` is `workload-consumer`, then the following consumer names will be used: `workload-consumer-0`, `workload-consumer-1`. |
| `--threads`, `-t` | Number of consumer threads.<br/>Default value: `1`. |

> Example of a command that reads `50` threads by `2` consumers and writes data to `100` producer threads at the target rate of `80` MB/s and duration of `10` seconds:
>
> ```bash
> {{ ydb-cli }} --profile quickstart workload topic run full --producer-threads 100 --consumers 2 --consumer-threads 50 --byte-rate 80M
> ```
>
> You will see statistics for in-progress time windows and final statistics when the test is complete:
>
> ```text
> Window  Write speed     Write time      Inflight        Lag             Lag time        Read speed      Full time
> #       msg/s   MB/s    percentile,ms   percentile,msg  percentile,msg  percentile,ms   msg/s   MB/s    percentile,ms
> 1       0       0       0               0               0               0               0       0       0
> 2       1091    10      2143            8               2076            20607           40156   392     30941
> 3       1552    15      2991            12              7224            21887           41040   401     31886
> 4       1733    16      3711            15              10036           22783           38488   376     32577
> 5       1900    18      4319            15              10668           23551           34784   340     33372
> 6       2793    27      5247            21              9461            24575           33267   325     34893
> 7       2904    28      6015            22              12150           25727           34423   336     35507
> 8       2191    21      5087            21              12150           26623           29393   287     36407
> 9       1952    19      2543            10              7627            27391           33284   325     37814
> 10      1992    19      2655            9               10104           28671           29101   284     38797
> Window  Write speed     Write time      Inflight        Lag             Lag time        Read speed      Full time
> #       msg/s   MB/s    percentile,ms   percentile,msg  percentile,msg  percentile,ms   msg/s   MB/s    percentile,ms
> Total   1814    17      5247            22              12150           28671           44827   438     40252
> ```
>
> * `Window`: Sequence number of the statistics window.
> * `Write speed`: Message write rate in messages per second and MB/s.
> * `Write time`: Percentile of the message write time, in milliseconds.
> * `Inflight`: Maximum number of messages awaiting commit across all partitions.
> * `Lag`: Maximum number of messages awaiting reading, in the statistics window. Messages across all partitions are included.
> * `Lag time`: Percentile of the message lag time in milliseconds.
> * `Read`: Message read rate for the consumer (in messages per second and MB/s).
> * `Full time`: Percentile of the full message processing time, from writing by the producer to reading by the consumer, in milliseconds.

## Deleting a topic {#clean}

When the work is complete, you can delete the test topic: General format of the topic deletion command:

```bash
{{ ydb-cli }} [global options...] workload topic clean [options...]
```

* `global options`: [Global options](commands/global-options.md).
* `options`: Subcommand options.

Subcommand options:

| Option name | Option description |
---|---
| `--topic` | Topic name.<br/>Default value: `workload-topic`. |

> To delete the `workload-topic` test topic, run the following command:
>
> ```bash
> {{ ydb-cli }} --profile quickstart workload topic clean
> ```
