# Query Load

Executes a user-defined workload consisting of multiple stages.  The user provides a directory path, referred to as a suite, which contains subdirectories for each stage. This path is specified using the `--suite-path` parameter in each command.

An example of a suite directory can be found on [GitHub](https://github.com/ydb-platform/ydb/tree/main/ydb/tests/functional/tpc/data/e1).

A suite can contain up to four stages:

1. **init**
    Initializes tables and their configurations, typically involving DDL queries from files with `sql` and `yql` extensions. These queries can also be specified directly from the command line using the `--query` parameter of the `init` command.

2. **import**
    Populates tables with data. The `import` directory should contain subfolders named after each table, with files in supported data formats such as `csv`, `tsv`, `csv.gz`, `tsv.gz`, or `parquet`.

3. **run**
    Executes load testing using queries from files in the `run` directory or directly from the command line via the `--query` parameter.

    Files with the `sql` and `yql` extensions are used to generate queries. For each one, a canonical result can be set using a file with the same name and an additional `.result` extension. These are CSV files with headers and some additional syntax:
 
    - If a query has more than one result set, the result file should contain the same number of data sets, separated by empty lines.
    - The last line may be `...`, which means the query result can have more rows, but only the first ones will be checked.
    - By default, floating-point numbers are compared with a relative accuracy of `1e-3` percent, but you can specify any absolute or relative accuracy, such as `1.5+-0.01` or `2.4e+10+-1%`.

    The canonical result will not be used unless the `--check-canonical` flag is set.

4. **clean**
    Cleans up by removing tables used for load testing.  This step only requires the database path.

    Details can be found in the description of the commands using the `--help` option.

## Common Command Options

All commands support the common option `--path`, which specifies the path to a table in the database:

```bash
{{ ydb-cli }} workload query --path user ...
```

### Available Options {#common_options}

| Name              | Description                 | Default value |
|-------------------|-----------------------------|---------------|
| `--path` or `-p`  | Specifies the table path.   | Root of db    |

## Initializing a Load Test {#init}

Before running the benchmark, create and configure tables:

```bash
{{ ydb-cli }} workload query --path user init --suite-path ~/user-suite
```

See the description of the command to initialize the data load:

```bash
{{ ydb-cli }} workload query init --help
```

### Available Parameters {#init_options}

| Name                              | Description                                                                      | Default value |
|-----------------------------------|----------------------------------------------------------------------------------|---------------|
| `--suite-path <path>`             | Path to the suite directory. See [description](./workload-query.md).             |               |
| `--query <query>` or `-q <query>` | DDL query to execute. Can be used multiple times.                                |               |
| `--clear`                         | If the table at the specified path has already been created, it will be deleted. |               |
| `--dry-run`                       | Do not actually perform DDL queries; only print them.                            |               |

## Loading Data into a Table {#load}

Populating tables with data:

```bash
{{ ydb-cli }} workload query --path user import --suite-path ~/user-suite
```

For source files, you can use CSV, TSV, PSV, and Parquet files. They can be either compressed or uncompressed.

### Available Parameters {#load_files_options}

| Name                         | Description                                                                                                                                                                             | Default value |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------|
| `--suite-path <path>`        | Path to the suite directory. See [description](./workload-query.md).                                                                             |               |
| `--state <path>`             | Path to the download state file. If the download is interrupted, it will resume from the same point when restarted.                             |               |
| `--clear-state`              | Relevant if the `--state` parameter is specified. Clears the state file and restarts the download from the beginning.                            |               |
| `--dry-run`                  | Do not actually perform the import.                                                                                                               |               |

{% include [load_options](./_includes/workload/load_options.md) %}

## Run a Load Test {#run}

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

### Query-load Specific Options {#run_query_options}

| Name                              | Description                                                     | Default value |
|-----------------------------------|------------------------------------------------------------------|---------------|
| `--suite-path <path>`             | Path to the suite directory. See [description](./workload-query.md). |               |
| `--query <query>` or `-q <query>` | Query to execute. Can be used multiple times.                   |               |

## Cleanup Test Data {#cleanup}

Run cleanup:

```bash
{{ ydb-cli }} workload query --path user clean
```

The command has no parameters.
