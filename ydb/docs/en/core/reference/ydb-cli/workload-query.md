# User workload

Allows you to perform load testing according to a specified user scenario.

A test scenario is a directory called `suite`, which contains a set of subdirectories: `init`, `import`, and `run`, corresponding to the `{{ ydb-cli }} workload query` commands.

* The `init` directory can have an arbitrary structure and contains SQL queries for creating and configuring objects in the database, such as tables or indexes.
* The `import` directory contains data to be loaded into tables. Each table corresponds to a subdirectory in the `import` directory.
* The `run` directory can have an arbitrary structure and contains SQL queries for load testing.

Example of a `suite` directory on [GitHub](https://github.com/ydb-platform/ydb/tree/main/ydb/tests/functional/tpc/data/e1).

The user specifies the path to the `suite` directory using the `--suite-path` option in each command.

## General syntax

General syntax for calling user workload commands:

```bash
{{ ydb-cli }} [global options...] workload query [--path <path/in/db>] <command> [options...]
```

* `global options` — [global parameters](commands/global-options.md).
* `--path` — the path in the database for load testing objects.
* `<command>` — one of the commands: `init`, `import`, `run`, `clean`.
* `options` — parameters for a specific command.

## Common command parameters

All commands support the common `--path` parameter, which sets the path in the database for load testing objects. All created objects will be created at this path. The path will also be taken into account during test execution and cleanup.

### Available parameters {#common_options}

| Name | Description | Default value |
|--------------------|---------------------------------------------------------------|-----------------------|
| `--path` or `-p` | Path in the database for load testing objects. | `/` |

## Load test initialization {#init}

Before starting the test, create tables and other database objects:

```bash
{{ ydb-cli }} workload query --path user_path init --suite-path ~/user-suite
```

The `init` command works as follows:

* If a `suite` is specified and it contains an `init` directory, the `init` directory is scanned, including its nested directories, using a depth-first search with lexicographical ordering of files and subdirectories at each level.
* Files with `.sql` and `.yql` extensions are selected.
* The content of each file is read and executed sequentially as an SQL query.
* After that, queries specified directly from the command line using the `--query` parameter are executed.
* If any of the queries result in an error, the command execution stops.

It is assumed that relatively "lightweight" queries, such as creating tables and indexes, are performed at this stage. It is not recommended to include scripts for filling tables with data in the files in the `init` directory. These operations should be moved to the `import` phase.

Macros can be used in queries:

* `{db}` — The absolute path in the database to the testing directory. It is a combination of the `--database` and `--path` option values.

View the command description for initializing tables:

```bash
{{ ydb-cli }} workload query init --help
```

### Available parameters {#init_options}

| Name | Description | Default value |
|-------------------------------------|----------------------------------------------------------------------------------|-----------------------|
| `--suite-path <path>` | Path to the `suite` directory. | |
| `--query <query>` or `-q <query>` | DDL query to execute. Can be used multiple times. | |
| `--clear` | If a table being created already exists at the specified path, it will be deleted first. | |
| `--dry-run` | Do not execute initialization queries, only print them. | |

## Loading data into a table {#load}

Load data into tables:

```bash
{{ ydb-cli }} workload query --path user_path import --suite-path ~/user-suite
```

The `import` command works as follows:

* If a `suite` is specified and it contains an `import` directory, it performs a sequential traversal of its subdirectories.
* It is assumed that each subdirectory corresponds to a table in the database.
* Files in supported data formats are used in the subdirectory, namely `csv`, `tsv`, `csv.gz`, `tsv.gz`, and `parquet`.
* Data is loaded from files into the corresponding tables. Loading is multi-threaded, with both the loading of different files and parts of a single file being parallelized. The order of loading is not guaranteed.

The loading process can take a long time and may be interrupted for some reason. To be able to continue loading from the point of interruption, a state mechanism can be applied. Using the `--state` option, you can specify the path to a state file. If the state file exists, it will be used to continue loading. If there is no state file, it will be created during the loading process. It can also be cleared using the `--clear-state` option.

View the command description for loading data:

```bash
{{ ydb-cli }} workload query import --help
```

### Available parameters {#load_files_options}

| Name | Description | Default value |
|---------------------|----------------------------------------------------------------------------------|-----------------------|
| `--suite-path <path>` | Path to the `suite` directory. | |
| `--state <path>` | Path to the loading state file. If the loading was interrupted for some reason, it will be continued from the same place upon a new start. | |
| `--clear-state` | Relevant if the `--state` parameter is specified. Clear the state file and start loading from the beginning. | |
| `--dry-run` | Do not load data into the database, only prepare the data and output a message about it. | |

{% include [load_options](./_includes/workload/load_options.md) %}

## Running a load test {#run}

Start the load:

```bash
{{ ydb-cli }} workload query --path user_path run --suite-path ~/user-suite
```

The `run` command works as follows:

* If the `suite` contains a `run` directory, the command traverses it and all its nested directories using a depth-first search, ordering files and subdirectories alphabetically at each level. If the `run` directory is missing, execution stops.
* It selects files with `.sql` and `.yql` extensions and includes them in the list of SQL queries.
* After that, it adds queries specified directly from the command line using the `--query` parameter to the list.
* Depending on the options, it either executes all queries sequentially, each query a specified number of times, or in parallel in multiple threads with query shuffling.
* Errors in queries will be recorded in the statistics but will not cause the testing to stop.

During the test, load statistics for each query and aggregated statistics for all queries are displayed on the screen.

View the command description for starting the load:

```bash
{{ ydb-cli }} workload query run --help
```

{% include [run_options](./_includes/workload/run_options.md) %}

### Query-specific options {#run_query_options}

| Name | Description | Default value |
|-------------------------------------|-----------------------------------------------------------------|-----------------------|
| `--suite-path <path>` | Path to the `suite` directory. | |
| `--query <query>` or `-q <query>` | Query to execute. Can be used multiple times. | |

### Canonical results

A canonical (expected) result can be specified for each query from a file. If the query response does not match the canonical one, this will be reflected in the statistics and command output. Differences will also be provided.

The canonical result is specified using a file with the same name and an additional `.result` extension. These are CSV files with headers and some additional syntax:

* If a query has more than one result set, the result file must contain the corresponding number of datasets separated by empty lines.

    For example, the query

    ```sql
    SELECT COUNT(*) AS count FROM table_1;
    SELECT MIN(col1) AS min, MAX(col1) AS max FROM table_2;
    ```

    Might have the following result file:

    ```csv
    count
    10

    min,max
    4,5
    ```

* The last line can be specified as `...`, which means that the query result can have more lines than in the canonical result, but the first lines of the result must match the canonical ones.
* By default, floating-point numbers are compared with a relative precision of `1e-3` percent, but in the canonical result, you can specify any absolute or relative precision for each value, for example: `1.5+-0.01`, `2.4e+10+-1%`.

For queries specified with the `--query` option, a canonical result cannot be set.

The canonical result will not be used unless the `--check-canonical` flag is set.

## Cleaning up test data {#cleanup}

Start the cleanup:

```bash
{{ ydb-cli }} workload query --path user_path clean
```

Performs recursive deletion of database objects at the path specified in the `--path` parameter.

The command has no parameters.

## Example of running a user test

### Download an example from the repository

```bash
git clone https://github.com/ydb-platform/ydb.git
cd ydb/ydb/tests/functional/tpc/data/e1
```

### Start initialization

```bash
{{ ydb-cli }} workload query --path test_path init --suite-path .
```

Result:

```bash
Init tables ...
Init tables ...Ok
```

### Start data import

```bash
{{ ydb-cli }} workload query --path test_path import --suite-path .
```

Result:

```bash
Fill table test_table_1...
Fill table test_table_1 OK 0 / 0 (0.353816s)
Fill table test_table_2...

Fill table test_table_2 OK 0 / 0 (0.102385s)
```

### Start the load test

```bash
ydb workload query --path user_path run --suite-path e1
```

Result:

```bash
first_query_set.1.sql:
    iteration 0:    ok    0.131573s seconds
first_query_set.2.yql:
    iteration 0:    ok    0.089327s seconds
second_query_set.join.sql:
    iteration 0:    ok    0.145536s seconds

Results for 1 iterations
┌───────────────────────────┬──────────┬─────────┬─────────┬─────────┬─────────┬───────────┬─────────┬─────────┬─────────┬─────────┬────────────────┬────────────────┬────────────────┬───────────┬──────────────┬────────────┬────────────┐
│ Query #                   │ ColdTime │ Min     │ Max     │ Mean    │ Median  │ UnixBench │ Std     │ RttMin  │ RttMax  │ RttAvg  │ CompilationMin │ CompilationMax │ CompilationAvg │ GrossTime │ SuccessCount │ FailsCount │ DiffsCount │
├───────────────────────────┼──────────┼─────────┼─────────┼─────────┼─────────┼───────────┼─────────┼─────────┼─────────┼─────────┼────────────────┼────────────────┼────────────────┼───────────┼──────────────┼────────────┼────────────┤
│ first_query_set.1.sql     │   0.086  │   0.086 │   0.086 │   0.086 │   0.086 │   0.086   │   0.000 │   0.045 │   0.045 │   0.045 │   0.041        │   0.041        │   0.041        │   0.132   │ 1            │            │            │
├───────────────────────────┼──────────┼─────────┼─────────┼─────────┼─────────┼───────────┼─────────┼─────────┼─────────┼─────────┼────────────────┼────────────────┼────────────────┼───────────┼──────────────┼────────────┼────────────┤
│ first_query_set.2.yql     │   0.081  │   0.081 │   0.081 │   0.081 │   0.080 │   0.081   │   0.001 │   0.008 │   0.008 │   0.008 │   0.039        │   0.039        │   0.039        │   0.089   │ 1            │            │            │
├───────────────────────────┼──────────┼─────────┼─────────┼─────────┼─────────┼───────────┼─────────┼─────────┼─────────┼─────────┼────────────────┼────────────────┼────────────────┼───────────┼──────────────┼────────────┼────────────┤
│ second_query_set.join.sql │   0.131  │   0.131 │   0.131 │   0.131 │   0.131 │   0.131   │   0.000 │   0.014 │   0.014 │   0.014 │   0.082        │   0.082        │   0.082        │   0.146   │ 1            │            │            │
├───────────────────────────┼──────────┼─────────┼─────────┼─────────┼─────────┼───────────┼─────────┼─────────┼─────────┼─────────┼────────────────┼────────────────┼────────────────┼───────────┼──────────────┼────────────┼────────────┤
│ Sum                       │   0.299  │   0.299 │   0.299 │   0.299 │   0.297 │   0.299   │   0.000 │   0.068 │   0.068 │   0.068 │   0.161        │   0.161        │   0.162        │   0.367   │ 3            │            │            │
├───────────────────────────┼──────────┼─────────┼─────────┼─────────┼─────────┼───────────┼─────────┼─────────┼─────────┼─────────┼────────────────┼────────────────┼────────────────┼───────────┼──────────────┼────────────┼────────────┤
│ Avg                       │   0.100  │   0.100 │   0.100 │   0.100 │   0.099 │   0.100   │   0.000 │   0.023 │   0.023 │   0.023 │   0.054        │   0.054        │   0.054        │   0.122   │ 3            │            │            │
├───────────────────────────┼──────────┼─────────┼─────────┼─────────┼─────────┼───────────┼─────────┼─────────┼─────────┼─────────┼────────────────┼────────────────┼────────────────┼───────────┼──────────────┼────────────┼────────────┤
│ GAvg                      │   0.097  │   0.097 │   0.097 │   0.097 │   0.097 │   0.097   │   0.000 │   0.018 │   0.018 │   0.001 │   0.051        │   0.051        │   0.001        │   0.000   │ 3            │            │            │
└───────────────────────────┴──────────┴─────────┴─────────┴─────────┴─────────┴───────────┴─────────┴─────────┴─────────┴─────────┴────────────────┴────────────────┴────────────────┴───────────┴──────────────┴────────────┴────────────┘

Results saved to results.out
```

### Clean up tables

```bash
ydb workload query --path user_path clean
```
