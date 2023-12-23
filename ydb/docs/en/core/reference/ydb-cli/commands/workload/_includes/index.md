# Load testing

You can use the `workload` command to run different types of workload against your DB.

General format of the command:

```bash
{{ ydb-cli }} [global options...] workload [subcommands...]
```

* `global options`: [Global options](../../../commands/global-options.md).
* `subcommands`: The [subcommands](#subcomands).

See the description of the command to run the data load:

```bash
{{ ydb-cli }} workload --help
```

## Available subcommands {#subcommands}

The following types of load tests are supported at the moment:

* [Stock](../stock.md): An online store warehouse simulator.
* [Key-value](../../../workload-kv.md): Key-Value load.
* [ClickBench](../../../workload-click-bench.md): ClickBench analytical benchmark (https://github.com/ClickHouse/ClickBench).
* [TPC-H](../../../workload-tpch.md): TPC-H benchmark (https://www.tpc.org/tpch/).
* [Topic](../../../workload-topic.md): Topic load.
* [Transfer](../../../workload-transfer.md): Transfer load.
