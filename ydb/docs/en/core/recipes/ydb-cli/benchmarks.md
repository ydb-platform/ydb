# Conducting load testing

{{ ydb-short-name }} CLI has a built-in toolkit for performing load testing using standard benchmarks:

| Benchmark                            | Reference                                                |
|--------------------------------------|----------------------------------------------------------|
| [TPC-C](https://tpc.org/tpcc/)       | [tpcc](../../reference/ydb-cli/workload-tpcc.md) |
| [TPC-H](https://tpc.org/tpch/)       | [tpch](../../reference/ydb-cli/workload-tpch.md) |
| [TPC-DS](https://tpc.org/tpcds/)     | [tpcds](../../reference/ydb-cli/workload-tpcds.md) |
| [ClickBench](https://benchmark.clickhouse.com/) | [clickbench](../../reference/ydb-cli/workload-click-bench.md) |

Also, there is a possibility to run user-defined testing scenarios, which are initiated via the `ydb workload query` command, see the [description](../../reference/ydb-cli/workload-query.md). The details are provided in the corresponding section.

All mentioned methods emulate the user's workload under the given scenarios. The detailed description of each method is provided in the corresponding sections, the links to which are provided above.

All commands for working with benchmarks are organized into corresponding groups, and the database path is specified in the same way for all commands:

```bash
{{ ydb-cli }} workload tpcc --path path/in/database ...
{{ ydb-cli }} workload tpch --path path/in/database ...
{{ ydb-cli }} workload tpcds --path path/in/database ...
{{ ydb-cli }} workload clickbench --path path/in/database ...
{{ ydb-cli }} workload query --path path/in/database ...
```

Load testing consists of three stages:

1. [Data preparation](#data-preparation)
1. [Testing](#testing)
1. [Cleanup](#cleanup)

## Data preparation {#data-preparation}

It consists of two steps: initializing tables and filling them with data.

### Initialization

Initialization is performed by the `init` command:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh init
{{ ydb-cli }} workload clickbench --path clickbench/hits init --store=row
{{ ydb-cli }} workload tpch --path tpch/s1 init --store=column
{{ ydb-cli }} workload tpcds --path tpcds/s1 init --store=external-s3
{{ ydb-cli }} workload query --path user/suite1 init --suite-path /home/user/user_suite
```

At the stage of creating tables, for `tpch`, `tpcds`, and `clickbench`, you can configure the tables to be created:

* Select the type of tables to be used: row, column, external, etc. (parameter `--store`);
* Select the types of columns to be used: some data types from the original benchmarks can be represented by multiple {{ ydb-short-name }} data types. In such cases, it is possible to select a specific one with `--string`, `--datetime`, and `--float-mode` parameters.

You can also specify that tables should be deleted before creation if they already exist using the `--clear` parameter.

For more details, see the relevant reference sections:

* [tpcc init](../../reference/ydb-cli/workload-tpcc.md#init)
* [clickbench init](../../reference/ydb-cli/workload-click-bench.md#init)
* [tpch init](../../reference/ydb-cli/workload-tpch.md#init)
* [tpcds init](../../reference/ydb-cli/workload-tpcds.md#init)
* [query init](../../reference/ydb-cli/workload-query.md#init)

### Loading data into the tables

The `import` command is used to load data into the benchmark tables. This command is specific to each benchmark, and its behavior depends on the subcommands. However, there are also parameters common to all benchmarks.

For more details, see the relevant reference sections:

* [tpcc import](../../reference/ydb-cli/workload-tpcc.md#load)
* [tpch import](../../reference/ydb-cli/workload-tpch.md#load)
* [tpcds import](../../reference/ydb-cli/workload-tpcds.md#load)
* [clickbench import](../../reference/ydb-cli/workload-click-bench.md#load)

Examples:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh import
{{ ydb-cli }} workload clickbench --path clickbench/hits import files --input hits.csv.gz
{{ ydb-cli }} workload tpch --path tpch/s1 import generator --scale 1
{{ ydb-cli }} workload tpcds --path tpcds/s1 import generator --scale 1
{{ ydb-cli }} workload query --path user/suite1 import --suite-path /home/user/user_suite
```

## Testing {#testing}

The performance testing is performed using the `run` command. Its behavior is mostly the same across different benchmarks, though some differences do exist.

Examples:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh run
{{ ydb-cli }} workload clickbench --path clickbench/hits run --include 1-5,8
{{ ydb-cli }} workload tpch --path tpch/s1 run --exсlude 3,4 --iterations 3
{{ ydb-cli }} workload tpcds --path tpcds/s1 run --plan ~/query_plan --include 2 --iterations 5
{{ ydb-cli }} workload query --path user/suite1 run --plan ~/query_plan --include first_query_set.1.sql,second_query_set.2.sql --iterations 5
```

The command 'run' for each benchmark has a number of additional parameters for configuring the types of reports generated, collecting statistics, and other results of load testing.

For more details, see the relevant reference sections:

* [tpcc run](../../reference/ydb-cli/workload-tpcc.md#run)
* [clickbench run](../../reference/ydb-cli/workload-click-bench.md#run)
* [tpch run](../../reference/ydb-cli/workload-tpch.md#run)
* [tpcds run](../../reference/ydb-cli/workload-tpcds.md#run)
* [query run](../../reference/ydb-cli/workload-query.md#run)

## Cleanup {#cleanup}

After all necessary testing has been completed, the benchmark's data can be removed from the database using the `clean` command:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh clean
{{ ydb-cli }} workload clickbench --path clickbench/hits clean
{{ ydb-cli }} workload tpch --path tpch/s1 clean
{{ ydb-cli }} workload tpcds --path tpcds/s1 clean
{{ ydb-cli }} workload query --path user/suite1 clean
```

For more details, see the relevant reference sections:

* [tpcc clean](../../reference/ydb-cli/workload-tpcc.md#cleanup)
* [clickbench clean](../../reference/ydb-cli/workload-click-bench.md#cleanup)
* [tpch clean](../../reference/ydb-cli/workload-tpch.md#cleanup)
* [tpcds clean](../../reference/ydb-cli/workload-tpcds.md#cleanup)
* [query clean](../../reference/ydb-cli/workload-query.md#cleanup)
