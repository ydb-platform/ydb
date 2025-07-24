# Load testing

You can use the `workload` command to run different types of workload against your DB.

General format of the command:

```bash
{{ ydb-cli }} [global options...] workload [subcommands...]
```

* `global options`: [Global options](../../../commands/global-options.md).
* `subcommands`: The [subcommands](#subcommands).

See the description of the command to run the data load:

```bash
{{ ydb-cli }} workload --help
```

## Available subcommands {#subcommands}

The following types of load tests are supported at the moment:

* [Stock](../stock.md): An online store warehouse simulator.
* [Key-value](../../../workload-kv.md): Key-Value load.
* [ClickBench](../../../workload-click-bench.md): [ClickBench analytical benchmark](https://github.com/ClickHouse/ClickBench).
* [TPC-H](../../../workload-tpch.md): [TPC-H benchmark](https://www.tpc.org/tpch/).
* [TPC-DS](../../../workload-tpcds.md): [TPC-DS benchmark](https://www.tpc.org/tpcds/).
* [Topic](../../../workload-topic.md): Topic load.
* [Transfer](../../../workload-transfer.md): Transfer load.
* [Vector](../../../workload-vector.md): Vector search workload.

### Global parameters for all workloads {#global_workload_options}

| Parameter name | Short name | Parameter description |
---|---|---
| `--seconds <value>` | `-s <value>` | Duration of the test, in seconds. Default: 10. |
| `--threads <value>` | `-t <value>` | The number of parallel threads creating the load. Default: 10. |
| `--rate <value>` | - | Total rate for all threads, in transactions per second. Default: 0 (no rate limit). |
| `--quiet` | - | Outputs only the total result. |
| `--print-timestamp` | - | Prints the time together with the statistics of each time window. |
| `--client-timeout` | - | [Transport timeout in milliseconds](../../../../../dev/timeouts.md). |
| `--operation-timeout` | - | [Operation timeout in milliseconds](../../../../../dev/timeouts.md). |
| `--cancel-after` | - | [Timeout for canceling an operation in milliseconds](../../../../../dev/timeouts.md). |
| `--window` | - | Statistics collection window, in seconds. Default: 1. |
