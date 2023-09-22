# TPC-H workload

The workload is based on the TPC-H [documentation](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf), and the queries and table schema are adapted for {{ ydb-short-name }}.

The test generates a typical decision support workload.

## Initializing a load test {#init}

Before running the benchmark, create a table:

```bash
{{ ydb-cli }} workload tpch init
```

See the description of the command to run the data load:

```bash
{{ ydb-cli }} workload tpch init --help
```

### Available parameters {#init-options}

| Option name | Option description |
---|---
| `--path <value>` | Directory to create tables in. Default value - empty string. |
| `--store <value>` | Type of table storage. Acceptable values: `row`, `column`. Default value: `row`. |

## Uploading data to the table {#load}

You can download the dataset generator for the TPC-H benchmark by the [link](http://tpc.org/tpc_documents_current_versions/current_specifications5.asp).
Then follow the instructions from README.
In the `dss.h` file, you can specify a field separator: Default: `#define SEPARATOR '|'`.
In the sample data upload script, `'\t'` is used as a separator.

```bash
for table in region nation supplier customer part partsupp orders lineitem; do
    echo "Start data load to $table"
    {{ ydb-cli }} import file tsv --header --path "$table" --input-file $table.tsv --newline-delimited
    echo "Finish data load to $table"
done
```

## Running a load test {#run}

Run the load:

```bash
{{ ydb-cli }} workload tpch run
```

During this test, workload statistics for each query are displayed on the screen.

See the description of the command to run the data load:

```bash
{{ ydb-cli }} workload tpch run --help
```

### Global parameters for all types of load {#run-options}

| Option name | Option description |
---|---
| `--path <value>` | Directory to create tables in. Default value - empty string. |
| `--output <value>` | The name of the file in which the query execution results will be saved. The default value is `results.out`. |
| `--iterations <value>` | The number of executions of each load generating query. The default value is `1`. |
| `--json` | The name of the file in which the query execution statistics will be saved in `json` format. By default, the file is not saved. |
| `--ministat` | The name of the file in which the query execution statistics will be saved in `ministat` format. By default, the file is not saved. |
| `--query-settings` | Query execution settings. By default, not specified. |
| `--ext-queries-dir` | Name of the directory with external queries used to apply the workload. |
| `--include` | The numbers or number sections of the queries to be executed as part of the load. By default, all queries are executed. Separated by commas, for example, `1,2,4-6`. |
| `--exclude` | The numbers or number sections of the queries to be excluded as part of the load. By default, all queries are executed. Separated by commas, for example, `1,2,4-6`. |
