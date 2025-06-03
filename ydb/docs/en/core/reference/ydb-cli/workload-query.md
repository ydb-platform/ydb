# Query load

Executes a user-defined workload consisting of multiple stages.
The user provides a directory path, referred to as a suite, which contains subdirectories for each stage. This path is specified using the "--suite-path" parameter in each command.

There is example of suite directory: https://github.com/ydb-platform/ydb/tree/main/ydb/tests/functional/tpc/data/e1.

The suite can contain up to four stages:
1. init
Initialization of tables and their configurations, typically involving DDL queries from files with `sql` and `yql` extensions. These queries can also be directly specified from the command line using the `--query` parameter of the `init` command.

2. import
Populating tables with data. The `import` directory should contain subfolders named after each table, with files in supported data formats such as `csv`, `tsv`, `csv.gz`, `tsv.gz` or `parquet`.

3. run
Executing load testing using queries from files in the "run" directory or directly from the command line via the `--query` parameter.

Files with the `sql` and `yql` extensions will be used to generate queries. For each one, a canonical result can be set using a file with the same name and the additional `.result` extension. These are CSV files with headers and some additional syntax:
    * If a query has more than one result set, the result file should contain the same number of data sets, separated by empty lines.
    * The last line may be set to `...`, which means the query result can have more rows, but only the first ones will be checked.
    * By default, floating-point numbers are compared with a relative accuracy of 1e-3 percent, but you can specify any absolute or relative accuracy like: `1.5+-0.01`, `2.4e+10+-1%`.

The canonical result will not be used unless the `--check-canonical` flag is set.

4. clean
Cleaning up by removing tables used for load testing.
This step only requires the database path.

Details can be found in the description of the commands, using the "--help" option.
## Common command options

All commands support the common option `--path`, which specifies the path to a table in the database:

```bash
{{ ydb-cli }} workload query --path user ...
```

### Available options {#common_options}

| Name             | Description                | Default value |
|------------------|----------------------------|---------------|
| `--path` or `-p` | Specifies the tables path. | Root of db    |

## Initializing a load test { #init }

Before running the benchmark, create and configure tables:

```bash
{{ ydb-cli }} workload query --path user init --suite-path ~/user-suite
```

See the description of the command to init the data load:

```bash
{{ ydb-cli }} workload query init --help
```

### Available parameters { #init_options }

| Name                              | Description                                                                      | Default value |
|-----------------------------------|----------------------------------------------------------------------------------|---------------|
| `--suite-path <path>`             | Path to suite direcory. See [description](./workload-query.md)                   |               |
| `--query <query>` or `-q <query>` | DDL query to execute. Can be used multiple times.                                |               |
| `--clear`                         | If the table at the specified path has already been created, it will be deleted. |               |
| `--dry-run`                       | Do not realy perfom DDL queries but only print they                              |               |

## Loading data into a table { #load }

 Populating tables with data:

```bash
{{ ydb-cli }} workload query --path user import --suite-path ~/user-suite
```

For source files, you can use CSV, TSV, PSV and parquet files. They can be either compressed or not.

### Available parameters { #load_files_options }

| Name | Description | Default value |
|---|---|---|
| `--suite-path <path>`           | Path to suite direcory. See [description](./workload-query.md) |               |
| `--state <path>`                | Path to the download state file. If the download is interrupted, it will resume from the same point when restarted.                                                                                                                                                                                                                                                                                                                                           | |
| `--clear-state`                 | Relevant if the `--state` parameter is specified. Clears the state file and restarts the download from the beginning.                                                                                                                                                                                                                                                                                                                                          | |
| `--dry-run`                     | Do not realy perfom import | |

{% include [load_options](./_includes/workload/load_options.md) %}

## Run a load test { #run }

Run the load:

```bash
{{ ydb-cli }} workload query --path user run --suite-path ~/user-suite
```

During the test, load statistics are displayed for each request.

See the command description to run the load:

```bash
{{ ydb-cli }} workload query run --help
```

{% include [run_options](./_includes/workload/run_options.md) %}

### Query-specific options { #run_query_options }

| Name                              | Description                                                    | Default value |
|-----------------------------------|----------------------------------------------------------------|---------------|
| `--suite-path <path>`             | Path to suite direcory. See [description](./workload-query.md) |               |
| `--query <query>` or `-q <query>` | Query to execute. Can be used multiple times.                  |               |

## Cleanup test data { #cleanup }

Run cleanup:

```bash
{{ ydb-cli }} workload query --path user clean
```

The command has no parameters.
