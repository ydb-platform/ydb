# TPC-DS workload

The workload is based on the TPC-DS [documentation](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v3.2.0.pdf), and the queries and table schemas are adapted for {{ ydb-short-name }}.

The test generates a typical workload in the decision support area.

## Common command options

All commands support the common option `--path`, which specifies the path to the directory with tables in the database:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 ...
```

### Available options { #common_options }

Option name | Option description
---|---
`--path` or `-p` | Path to the directory with tables. Default value is `/`

## Initializing the load test {#init}

Before running the benchmark, create a table:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 init
```

See the description of the command to run the load:

```bash
{{ ydb-cli }} workload tpcds init --help
```

{% include [init_options](./_includes/workload/init_options_tpc.md) %}

## Loading data into the table { #load }

Load data into the table. The data will be generated directly by {{ ydb-cli }}:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 import generator --scale 1
```

See the description of the command to load the data:

```bash
{{ ydb-cli }} workload tpcds import --help
```

### Available options {#load_files_options}

Option name | Option description
---|---
`--scale <value>` | Data scale. Powers of ten are usually used.
`--tables <value>` | Comma-separated list of tables to generate. Available tables: `customer`, `nation`, `order_line`, `part_psupp`, `region`, `supplier`. All tables are created by default.
`--proccess-count <value>` or `-C <value>` | Data generation can be split into several processes, this parameter specifies the number of processes. Default value: 1.
`--proccess-index <value>` or `-i <value>` | Data generation can be split into several processes, this parameter specifies the process number. Default value: 0.
`--state <path>` | Path to the download state file. If the download was interrupted for some reason, the download will be continued from the same place when it is started again.
`--clear-state` | Relevant if the `--state` parameter is specified. Clear the state file and start the download from the beginning.

{% include [load_options](./_includes/workload/load_options.md) %}

## Run the load test { #run }

Run the load:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 run
```

During the benchmark, load statistics are displayed for each request.

See the description of the command to run the load:

```bash
{{ ydb-cli }} workload tpcds run --help
```

{% include [run_options](./_includes/workload/run_options.md) %}

### TPC-DS-specific options { #run_tpcds_options }

Option name | Option description
---|---
`--ext-query-dir <name>` | Directory with external queries for load execution. Queries should be in files named `q[1-99].sql`. No default value.

## Test data cleaning { #clean }

Run cleaning:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits clean
```

The command has no parameters.
