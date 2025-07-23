# TPC-C Workload

The workload is based on the TPC-C [specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-c_v5.11.0.pdf), with the queries and table schemas adapted for {{ ydb-short-name }}.

TPC-C is an industry-standard [On-Line Transaction Processing (OLTP)](https://en.wikipedia.org/wiki/Online_transaction_processing) benchmark. It simulates a retail company with a configurable number of warehouses, each containing 10 districts and 3,000 customers per district. A corresponding inventory exists for the warehouses. Customers place orders composed of several items. The company tracks payments, deliveries, and order history, and periodically performs inventory checks.

As a result, the benchmark generates a workload of concurrent distributed transactions with varying types and complexities.

Here is a quick start snippet:
```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh init -w 10
{{ ydb-cli }} workload tpcc --path tpcc/10wh import -w 10
{{ ydb-cli }} workload tpcc --path tpcc/10wh run -w 10
```

## Common Command Options

All commands support the common `--path` option, which specifies the path to the directory containing the benchmark tables in the database:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh ...
```

### Available Options {#common_options}

| Name             | Description                                           | Default value |
|------------------|-------------------------------------------------------|----------------|
| `--path` or `-p` | Database path where the benchmark tables are located. | `/`            |

## Initializing a Load Test {#init}

Before running the benchmark, create the tables:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh init --warehouses 10
```

See the command description:

```bash
{{ ydb-cli }} workload tpcc init --help
```

### Available parameters {#init_options}

| Name                         | Description                    | Default value |
|------------------------------|--------------------------------|---------------|
| `--warehouses` or `-w`       | A number of TPC-C warehouses.  | 10            |

## Loading data into a table { #load }

The data will be generated and loaded into the tables directly by {{ ydb-cli }}:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh import --warehouses 10
```

See the command description:

```bash
{{ ydb-cli }} workload tpcc import --help
```

### Available options { #load_options }

| Name                         | Description                                                    | Default value |
|------------------------------|----------------------------------------------------------------|---------------|
| `--warehouses` or `-w`       | A number of TPC-C warehouses.                                  | 10            |
| `--threads`                  | A number of threads loading the TPC-C data to the database.    | 10            |
| `--no-tui`                   | Disable TUI, which is enabled by default in interactive mode.  |               |

## Run the load test { #run }

Run the load:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh run
```

During the benchmark, the CLI displays a preview of the results and various load statistics.

See the command description:

```bash
{{ ydb-cli }} workload tpcc run --help
```

### Available options { #run_options }

| Name                         | Description                                                    | Default value |
|------------------------------|----------------------------------------------------------------|---------------|
| `--warehouses` or `-w`       | A number of TPC-C warehouses.                                  | 10            |
| `--warmup`                   | Warmup time. Example: 10s, 5m, 1h.                             | 30m           |
| `--time` or `-t`             | Execution time. Example: 10s, 5m, 1h.                          | 2h            |
| `--max-sessions` or `-m`     | A soft limit on the number of DB sessions.                         | 100           |
| `--threads`                  | A number of threads executing queries                          | auto          |
| `--format` or `-f`           | Output format: 'Pretty', 'Json'                                | Pretty        |
| `--no-tui`                   | Disable TUI, which is enabled by default in interactive mode.  |               |

### Results

Benchmark results include tpmC, efficiency, and per-transaction type latencies. As stated by the official specification:

“The performance metric reported by TPC-C is a “business throughput” measuring the number of orders processed per minute. Multiple transactions are used to simulate the business activity of processing an order, and each transaction is subject to a response time constraint. The performance metric for this benchmark is expressed in transactions-per-minute-C (tpmC).”

The TPC-C specification limits the number of transactions that can be processed per warehouse. The theoretical maximum is 12.86 tpmC per warehouse. To increase the overall load—and thereby the tpmC—you must scale the number of warehouses.

Efficiency is calculated using the following formula:

```
efficiency = (result_tpmc / (12.86 × warehouse_count)) × 100
```

## Test data cleaning { #cleanup }

Run cleaning:

```bash
{{ ydb-cli }} workload tpcc --path tpcc/10wh clean
```

The command has no parameters.
