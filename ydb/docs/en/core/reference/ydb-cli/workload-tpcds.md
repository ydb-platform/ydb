# TPC-DS workload

The workload is based on the TPC-DS [specification](https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-DS_v3.2.0.pdf), with the queries and table schemas adapted for {{ ydb-short-name }}.

This benchmark generates a workload typical for decision support systems.

## Common command options

All commands support the common option `--path`, which specifies the path to the directory containing benchmark tables in the database:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 ...
```

### Available options { #common_options }

| Name              | Description                         | Default value |
|-------------------|-------------------------------------|---------------|
| `--path` or `-p` | Path to the directory with tables.   | `/`           |

## Initializing the load test {#init}

Before running the benchmark, create a table:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 init
```

See the command description to run the load:

```bash
{{ ydb-cli }} workload tpcds init --help
```

{% include [init_options](./_includes/workload/init_options_tpc.md) %}

## Loading data into the table { #load }

The data will be generated and loaded into the table directly by {{ ydb-short-name }} CLI:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 import generator --scale 1
```

See the command description:

```bash
{{ ydb-cli }} workload tpcds import --help
```

### Available options {#load_files_options}

| Name                                  | Description                                                                                                                                       | Default value |
|---------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `--scale <value>`                     | Data scale. Typically, powers of ten are used.                                                                                                    |               |
| `--tables <value>`                    | Comma-separated list of tables to generate. Available tables: `customer`, `nation`, `order_line`, `part_psupp`, `region`, `supplier`.             | All tables    |
| `--process-count <value>` or `-C <value>` | Specifies the number of processes for parallel data generation.                                                                               | `1`           |
| `--process-index <value>` or `-i <value>` | Specifies the process number when data generation is split into multiple processes.                                                           | `0`           |
| `--state <path>`                      | Path to the state file for resuming generation. If the generation is interrupted, it will resume from the same point when restarted.              |               |
| `--clear-state`                       | Relevant if the `--state` parameter is specified. Clears the state file and restarts the download from the beginning.                             |               |

{% include [load_options](./_includes/workload/load_options.md) %}

## Run the load test { #run }

Run the load:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 run
```

During the benchmark, load statistics are displayed for each request.

See the command description:

```bash
{{ ydb-cli }} workload tpcds run --help
```

{% include [run_options](./_includes/workload/run_options.md) %}

### TPC-DS-specific options { #run_tpcds_options }

| Name                       | Description                                                                                         | Default value |
|----------------------------|-----------------------------------------------------------------------------------------------------|---------------|
| `--ext-query-dir <name>`   | Directory with external queries for load execution. Queries should be in files named `q[1-99].sql`. |               |

## Test data cleanup { #cleanup }

Run cleanup:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 clean
```

The command has no parameters.
