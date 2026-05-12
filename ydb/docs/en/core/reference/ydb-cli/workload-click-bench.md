# ClickBench load

The load is based on data and queries from the [https://github.com/ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench) repository, and the queries and table layout are adapted to {{ ydb-short-name }}.

The benchmark generates typical workload in the following areas: clickstream and traffic analysis, web analytics, machine-generated data, structured logs, and event data. It covers typical queries in analytics and real-time dashboards.

The dataset for this benchmark was obtained from an actual traffic recording of one of the world's largest web analytics platforms. It has been anonymized while keeping all the essential data distributions. The query set was improvised to reflect realistic workloads, while the queries are not directly from production.

## Common command options

All commands support the common option `--path`, which specifies the path to a table in the database:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits ...
```

### Available options {#common_options}

| Name          | Description                                                       | Default value             |
|----------------------|---------------------------------------------------------------------------|---------------------------|
| `--path` or `-p`     | Specifies the table path.                                           | `clickbench/hits`         |

## Initializing a load test { #init }

Before running the benchmark, create a table:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits init
```

See the description of the command to init the data load:

```bash
{{ ydb-cli }} workload clickbench init --help
```

### Available parameters { #init_options }

| Name          | Description                                                       | Default value             |
|--------------------------------------------------|---------------------------------------------------------------------------|----------------------------|
| `--store <value>`                                | Table storage type. Possible values: `row`, `column`, `external-s3`.      | `column`.                     |
| `--external-s3-prefix <value>`                   | Only relevant for external tables. Root path to the dataset in S3 storage.|                            |
| `--external-s3-endpoint <value>` or `-e <value>` | Only relevant for external tables. Link to S3 Bucket with data.           |                            |
| `--string`                                       | Use `String` type for text fields. `Utf8` is used by default.             |                            |
| `--datetime`                                     | Use `Date`, `Datetime` and `Timestamp` type for time-related fields.      |`Date32`, `Datetime64` and `Timestamp64`|
|  `--partition-size` | Maximum partition size in megabytes (AUTO_PARTITIONING_PARTITION_SIZE_MB) for row tables. | 2000 |
| `--clear`                                        | If the table at the specified path has already been created, it will be deleted.|                      |
| `--dry-run`                                      | Do not execute initialization queries, but only display their text. |                      |

## Loading data into a table { #load }

Download the data archive, then load the data into the table:

```bash
wget https://datasets.clickhouse.com/hits_compatible/hits.csv.gz
{{ ydb-cli }} workload clickbench --path clickbench/hits import files --input hits.csv.gz
```

For source files, you can use CSV and TSV files, as well as directories containing such files. They can be either compressed or not.

### Available parameters { #load_files_options }

| Name | Description | Default value |
|---|---|---|
| `--input <path>` or `-i <path>` | Path to the source data files. Both unpacked and packed CSV and TSV files, as well as directories containing such files, are supported. Data can be downloaded from the official ClickBench website: [csv.gz](https://datasets.clickhouse.com/hits_compatible/hits.csv.gz), [tsv.gz](https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz). To speed up the process, these files can be split into smaller parts, allowing parallel downloads. | |
| `--state <path>`                | Path to the download state file. If the download is interrupted, it will resume from the same point when restarted.                                                                                                                                                                                                                                                                                                                                           | |
| `--clear-state`                 | Relevant if the `--state` parameter is specified. Clears the state file and restarts the download from the beginning.                                                                                                                                                                                                                                                                                                                                          | |
| `--dry-run`                     | Do not execute loading queries, but only display their text. | |

{% include [load_options](./_includes/workload/load_options.md) %}

## Run a load test { #run }

Run the load:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits run
```

During the test, load statistics are displayed for each request.

See the command description to run the load:

```bash
{{ ydb-cli }} workload clickbench run --help
```

{% include [run_options](./_includes/workload/run_options.md) %}

### ClickBench-specific options { #run_clickbench_options }

| Name                                        | Description                                                                                                 | Default value |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------|---------------|
| `--syntax <value>` | Syntax of the queries to use. Available values: `yql`, `pg` (abbreviation of `PostgreSQL`). For more information about working with YQL syntax, see [here](../../yql/reference/index.md), and for PostgreSQL [here](../../postgresql/intro.md). | `yql` |

## Cleanup test data { #cleanup }

Run cleanup:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits clean
```

The command has no parameters.
