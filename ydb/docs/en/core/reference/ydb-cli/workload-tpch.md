# TPC-H workload

The workload is based on the TPC-H [specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf), with the queries and table schemas adapted for {{ ydb-short-name }}.

The benchmark generates a workload typical for decision support systems.

## Common command options

All commands support the common `--path` option, which specifies the path to the directory containing tables in the database:

```bash
{{ ydb-cli }} workload tpch --path tpch/s1 ...
```

### Available options {#common_options}

| Name             | Description                        | Default value |
|------------------|------------------------------------|---------------|
| `--path` or `-p` | Path to the directory with tables. | `/`           |

## Initializing a load test { #init }

Before running the benchmark, create a table:

```bash
{{ ydb-cli }} workload tpch --path tpch/s1 init
```

See the command description:

```bash
{{ ydb-cli }} workload tpch init --help
```

{% include [init_options](./_includes/workload/init_options_tpc.md) %}

## Loading data into a table { #load }

The data will be generated and loaded into a table directly by {{ ydb-cli }}:

```bash
{{ ydb-cli }} workload tpch --path tpch/s1 import generator --scale 1
```

See the command description:

```bash
{{ ydb-cli }} workload tpch import --help
```

### Available options { #load_files_options }

| Name                                        | Description                                                                                                                                                        | Default value |
|---------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `--scale <value>`                           | Data scale. Powers of ten are usually used. Also supports fractional scale, which is not described in the TPC-H specification. It can be useful for quickly testing small YDB databases. Examples: `0.1`, `0.3`.                                                                                                    |               |
| `--tables <value>`                          | Comma-separated list of tables to generate. Available tables: `customer`, `nation`, `order_line`, `part_psupp`, `region`, `supplier`.                              | All tables    |
| `--proccess-count <value>` or `-C <value>`  | Data generation can be split into several processes, this parameter specifies the number of processes.                                                             | 1             |
| `--proccess-index <value>` or `-i <value>`  | Data generation can be split into several processes, this parameter specifies the process number.                                                                  | 0             |
| `--state <path>`                            | Path to the generation state file. If the generation was interrupted for some reason, the download will be continued from the same place when it is started again. |               |
| `--clear-state`                             | Relevant if the `--state` parameter is specified. Clear the state file and start the download from the beginning.                                                  |               |
| `--dry-run`                                 | Do not execute loading queries, but only display their text.                                                                                     |               |

{% include [load_options](./_includes/workload/load_options.md) %}

## Run the load test { #run }

Run the load:

```bash
{{ ydb-cli }} workload tpch --path tpch/s1 run
```

During the test, load statistics are displayed for each request.

See the command description:


```bash
{{ ydb-cli }} workload tpch run --help
```

{% include [run_options](./_includes/workload/run_options.md) %}

### TPC-H-specific options { #run_tpch_options }

| Name                       | Description                                                                                         | Default value |
|----------------------------|-----------------------------------------------------------------------------------------------------|---------------|
| `--syntax <value>`         | Syntax of the queries to use. Available values: `yql`, `pg` (abbreviation of `PostgreSQL`). For more information about working with YQL syntax, see [here](../../yql/reference/index.md), and for PostgreSQL [here](../../postgresql/intro.md). | `yql` |
| `--float-mode <value>`     | Float mode. Can be `float`, `decimal` or `decimal_ydb`. If the value is `float` - float will be used, `decimal` means that decimal with canonical size specified in the TPC-H specification (`Decimal(12, 2)`) will be used, and `decimal_ydb` means that all float will be converted to `Decimal(22, 9)`. For more information about the Decimal type, see [documentation](../../yql/reference/types/primitive.md#numeric). | `float` |
| `--scale <value>`          | Scale factor. See the TPC-H specification, chapter 3. Used in TPC-H queries. Also supports fractional scale, which is not described in the TPC-H specification. It can be useful for quickly testing small YDB databases. Examples: `0.1`, `0.3`. For scale factors `1`, `10`, `100`, `1000` canonical answers are specified (see the `--check-canonical` option description). | 1 |

## Test data cleaning { #cleanup }

Run cleaning:

```bash
{{ ydb-cli }} workload tpch --path tpch/s1 clean
```

The command has no parameters.
