# Conducting load testing

{{ ydb-short-name }} has a built-in toolkit for conducting load testing with several standard benchmarks:

* [TPC-H](https://tpc.org/tpch/)
* [TPC-DS](https://tpc.org/tpcds/)
* [ClickBench](https://benchmark.clickhouse.com/)

They work in a similar way, for a detailed description of each, see the relevant sections, links below.
All commands for working with benchmarks are collected in the corresponding groups, and the path to the database is specified in the same way for all commands:

```bash
{{ ydb-cli }} workload clickbench --path path/in/database ...
{{ ydb-cli }} workload tpch --path path/in/database ...
{{ ydb-cli }} workload tpcds --path path/in/database ...
```

Load testing can be divided into 3 stages:

1. Data preparation
1. Testing
1. Cleaning

## Data preparation

Consists of two stages, initializing tables and filling them with data.

### Initialization

Initialization is performed by the `init` command:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits init --store=row
{{ ydb-cli }} workload tpch --path tpch/s1 init --store=column
{{ ydb-cli }} workload tpcds --path tpcds/s1 init --store=external-s3
```

At this stage, you can configure the tables to be created:

* Select the type of tables to be used: row, column, external, etc. (parameter `--store`);
* Select the types of columns to be used: strings (parameter `--string`), dates and times (`--datetime`), and the type of real numbers (`--float-mode`).

You can also specify that tables should be deleted before creation if they have already been created. `--clear` parameter

For more details, see the description of the commands for each benchmark:
[clickbench init](../../reference/ydb-cli/workload-click-bench.md#init)
[tpch init](../../reference/ydb-cli/workload-tpch.md#init)
[tpcds init](../../reference/ydb-cli/workload-tpcds.md#init)

### Filling with data

Filling with data is performed using the `import` command. This command is specific to each benchmark and its behavior depends on the subcommands. However, there are also parameters common to all.

For a detailed description, see the relevant sections:
[clickbench import](../../reference/ydb-cli/workload-click-bench.md#load)
[tpch import](../../reference/ydb-cli/workload-tpch.md#load)
[tpcds import](../../reference/ydb-cli/workload-tpcds.md#load)

Examples:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits import files --input hits.csv.gz
{{ ydb-cli }} workload tpch --path tpch/s1 import generator --scale 1
{{ ydb-cli }} workload tpcds --path tpcds/s1 import generator --scale 1
```

## Testing

The testing itself is performed by the `run` command. Its behavior is almost the same for different benchmarks, although some differences are still present.

Examples:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits run --include 1-5,8
{{ ydb-cli }} workload tpch --path tpch/s1 run --exсlude 3,4 --iterations 3
{{ ydb-cli }} workload tpcds --path tpcds/s1 run --plan ~/query_plan --include 2 --iterations 5
```

The command allows you to select queries for execution, generate several types of reports, collect execution statistics, etc.

For a detailed description, see the relevant sections:
[clickbench run](../../reference/ydb-cli/workload-click-bench.md#run)
[tpch run](../../reference/ydb-cli/workload-tpch.md#run)
[tpcds run](../../reference/ydb-cli/workload-tpcds.md#run)

## Cleanup

After all necessary testing has been performed, the data can be removed from the database.
This can be done using the `clean` command:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits clean
{{ ydb-cli }} workload tpch --path tpch/s1 clean
{{ ydb-cli }} workload tpcds --path tpcds/s1 clean
```

For a detailed description, see the corresponding sections:
[clickbench clean](../../reference/ydb-cli/workload-click-bench.md#clean)
[tpch clean](../../reference/ydb-cli/workload-tpch.md#clean)
[tpcds clean](../../reference/ydb-cli/workload-tpcds.md#clean)
