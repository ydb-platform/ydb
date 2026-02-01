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
| `--scale <value>`                     | Data scale. Typically, powers of ten are used. Also supports fractional scale, which is not described in the TPC-DS specification. It can be useful for quickly testing small YDB databases. Examples: `0.1`, `0.3`.                                                                                                   |               |
| `--tables <value>`                    | Comma-separated list of tables to generate. Available tables: `customer`, `nation`, `order_line`, `part_psupp`, `region`, `supplier`.             | All tables    |
| `--process-count <value>` or `-C <value>` | Specifies the number of processes for parallel data generation.                                                                               | `1`           |
| `--process-index <value>` or `-i <value>` | Specifies the process number when data generation is split into multiple processes.                                                           | `0`           |
| `--state <path>`                      | Path to the state file for resuming generation. If the generation is interrupted, it will resume from the same point when restarted.              |               |
| `--clear-state`                       | Relevant if the `--state` parameter is specified. Clears the state file and restarts the download from the beginning.                             |               |
| `--dry-run`                           | Do not execute loading queries, but only display their text.                                                                                     |               |

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
| `--syntax <value>`         | Syntax of the queries to use. Available values: `yql`, `pg` (abbreviation of `PostgreSQL`). For more information about working with YQL syntax, see [here](../../yql/reference/index.md), and for PostgreSQL [here](../../postgresql/intro.md). | `yql` |
| `--float-mode <value>`     | Float mode. Can be `float`, `decimal` or `decimal_ydb`. If the value is `float` - float will be used, `decimal` means that decimal with canonical size specified in the TPC-DS specification (`Decimal(12, 2)`) will be used, and `decimal_ydb` means that all float will be converted to `Decimal(22, 9)`. For more information about the Decimal type, see [documentation](../../yql/reference/types/primitive.md#numeric). | `float` |
| `--scale <value>`          | Scale factor. See the TPC-DS specification, chapter 3. Used in TPC-DS queries. Also supports fractional scale, which is not described in the TPC-DS specification. It can be useful for quickly testing small YDB databases. Examples: `0.1`, `0.3`. For scale factors `1`, `10`, `100`, `1000` canonical answers are specified (see the `--check-canonical` option description). | 1 |

## Test data cleanup { #cleanup }

Run cleanup:

```bash
{{ ydb-cli }} workload tpcds --path tpcds/s1 clean
```

The command has no parameters.
