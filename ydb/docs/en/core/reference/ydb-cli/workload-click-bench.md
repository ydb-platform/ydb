# ClickBench load

The load is based on data and queries from the [https://github.com/ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench) repository, and the queries and table layout are adapted to {{ ydb-short-name }}.

The benchmark generates typical workload in the following areas: clickstream and traffic analysis, web analytics, machine-generated data, structured logs, and event data. It covers typical queries in analytics and real-time dashboards.

The dataset for this benchmark was obtained from an actual traffic recording of one of the world's largest web analytics platforms. It has been anonymized while keeping all the essential data distributions. The query set was improvised to reflect realistic workloads, while the queries are not directly from production.

## Common command options

All commands support a common option `--path`, which specifies the path to a table in the database:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits ...
```

### Available options { #common_options }

Option name | Option description
---|---
`--path` or `-p` | Path to the table. Default value is `clickbench/hits`

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

Option name | Option description
---|---
`--store <value>` | Table storage type. Possible values: `row`, `column`, `external-s3`. Default value is `row`.
`--external-s3-prefix <value>` | Only relevant for external tables. Root path to the dataset in S3 storage.
`--external-s3-endpoint <value>` or `-e <value>` | Only relevant for external tables. Link to S3 Bucket with data.

`--string` | Use `String` type for text fields. `Utf8` is used by default.

`--datetime` | Use `Date`, `Datetime` and `Timestamp` type for time-related fields. `Date32`, `Datetime64` and `Timestamp64` are used by default.

`--clear` | If the table at the specified path has already been created, it will be deleted.

## Loading data into a table { #load }

Load data into a table. To do this, download the archive with the data, then load the data into the table:

```bash
wget https://datasets.clickhouse.com/hits_compatible/hits.csv.gz
{{ ydb-cli }} workload clickbench --path clickbench/hits import files --input hits.csv.gz
```

You can use both unpacked and packed csv and tsv files, as well as directories with such files, as source files.

### Available parameters { #load_files_options }

Option name | Option description
---|---
`--input <path>` or `-i <path>` | Path to source data files. Both unpacked and packed csv and tsv files, as well as directories with such files, are supported. The data can be downloaded from the official ClickBench website: [csv.gz](https://datasets.clickhouse.com/hits_compatible/hits.csv.gz), [tsv.gz](https://datasets.clickhouse.com/hits_compatible/hits.tsv.gz). To speed up the download, you can split these files into smaller parts, in which case the parts will be downloaded in parallel.
`--state <path>` | Path to the download state file. If the download was interrupted for some reason, the download will continue from the same place when restarted.
`--clear-state` | Relevant if the `--state` parameter is specified. Clear the state file and start the download from the beginning.

{% include [load_options](./_includes/workload/load_options.md) %}

## Run a load test { #run }

Run the load:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits run
```

During the test, load statistics are displayed for each request.

See the description of the command to run the load:

```bash
{{ ydb-cli }} workload clickbench run --help
```

{% include [run_options](./_includes/workload/run_options.md) %}

### ClickBench-specific options { #run_clickbench_options }

Option name | Option description
---|---
`--ext-queries <queries>` or `-q <queries>` | External queries to perform the load, separated by semicolons. Not required by default.
`--ext-queries-file <name>` | The name of the file in which external queries to perform the load can be specified, separated by semicolons. Not required by default.
`--ext-query-dir <name>` | Directory with external queries to perform the load. Queries must be located in files named `q[0-42].sql`. No default value.
`--ext-results-dir <name>` | Directory with external query results for comparison. Results must be located in files named `q[0-42].sql`. No default value.
`--check-cannonical` or `-c` | Use special deterministic inner queries and check the results against canonical ones.

## Clean test data { #clean }

Run cleanup:

```bash
{{ ydb-cli }} workload clickbench --path clickbench/hits clean
```

The command has no parameters.
