# ClickBench load

The load is based on data and queries from the [https://github.com/ClickHouse/ClickBench](https://github.com/ClickHouse/ClickBench) repository, and the queries and table layout are adapted to {{ ydb-short-name }}.

The benchmark generates typical workload in the following areas: clickstream and traffic analysis, web analytics, machine-generated data, structured logs, and event data. It covers typical queries in analytics and real-time dashboards.

The dataset for this benchmark was obtained from an actual traffic recording of one of the world's largest web analytics platforms. It has been anonymized while keeping all the essential data distributions. The query set was improvised to reflect realistic workloads, while the queries are not directly from production.

## Initializing a load test {#init}

Before running the benchmark, create a table:

```bash
{{ ydb-cli }} workload clickbench init
```

See the description of the command to init the data load:

```bash
{{ ydb-cli }} workload clickbench init --help
```

### Available parameters {#init-options}

| Parameter name | Parameter description |
---|---
| `--table <value>` | The table name. The default value is `clickbench/hits`. |
| `--store <value>` | Type of table storage. Acceptable values: `row`, `column`. Default value: `row`. |

## Uploading data to the table {#load}

Upload data to the table. To do this, download and unzip the data archive, then upload the data to the table:

```bash
wget https://datasets.clickhouse.com/hits_compatible/hits.csv.gz
gzip -d hits.csv.gz
{{ ydb-cli }} import file csv --path clickbench/hits --input-file hits.csv
```

## Running a load test {#run}

Run the load:

```bash
{{ ydb-cli }} workload clickbench run
```

During this test, workload statistics for each query are displayed on the screen.

See the description of the command to run the data load:

```bash
{{ ydb-cli }} workload clickbench run --help
```

### Global parameters for all types of load {#run-options}

| Parameter name | Parameter description |
---|---
| `--output <value>` | The name of the file in which the query execution results will be saved. The default value is `results.out`. |
| `--iterations <value>` | The number of executions of each load generating query. The default value is `1`. |
| `--json` | The name of the file in which the query execution statistics will be saved in `json` format. By default, the file is not saved. |
| `--ministat` | The name of the file in which the query execution statistics will be saved in `ministat` format. By default, the file is not saved. |
| `--query-settings` | Query execution settings. By default, not specified. |
| `--ext-queries-file` | The name of the file in which external queries to run the load can be specified. By default, the file is not required. |
| `--ext-query` | A row with external queries to run the load. There is no default value. |
| `--table` | The table name. The default value is `clickbench/hits`. |
| `--include` | The numbers or number sections of the queries to be executed as part of the load. By default, all queries are executed. Separated by commas, for example, `1,2,4-6`. |
| `--exclude` | The numbers or number sections of the queries to be excluded as part of the load. By default, all queries are executed. Separated by commas, for example, `1,2,4-6`. |
