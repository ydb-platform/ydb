# Key-Value load

A simple load type using a YDB database as a Key-Value storage.

## Types of load {#workload-types}

This load test runs 3 types of load:
* [upsert](#upsert-kv): Using the UPSERT operation, inserts rows that are tuples (key, value1, value2, ... valueN) into the table created previously with the init command, the N number is specified in the settings.
* [insert](#insert-kv): The function is the same as the upsert load, only the INSERT operation is used for insertion.
* [select](#select-kv): Reads data using the SELECT * WHERE key = $key operation. A query always affects all table columns, but isn't always a point query, and the number of primary key variations can be controlled using parameters.

## Load test initialization {#init}

To get started, you must create tables. When creating them, you can specify how many rows to insert during initialization:

```bash
{{ ydb-cli }} workload kv init [init options...]
```

* `init options`: [Initialization options](#init-options).

View a description of the command to initialize the table:

```bash
{{ ydb-cli }} workload kv init --help
```

### Available parameters {#init-options}

| Parameter name | Parameter description |
---|---
| `--init-upserts <value>` | Number of insertion operations to be performed during initialization. Default: 1000. |
| `--min-partitions` | Minimum number of shards for tables. Default: 40. |
| `--auto-partition` | Enabling/disabling auto-sharding. Possible values: 0 or 1. Default: 1. |
| `--max-first-key` | Maximum value of the primary key of the table. Default: $2^{64} — 1$. |
| `--len` | The size of the rows in bytes that are inserted into the table as values. Default: 8. |
| `--cols` | Number of columns in the table. Default: 2 counting Key. |
| `--rows` | Number of affected rows in one query. Default: 1. |

The following command is used to create a table:

```yql
CREATE TABLE `kv_test`(
    c0 Uint64,
    c1 String,
    c2 String,
    ...
    cN String,
    PRIMARY KEY(c0)) WITH (
        AUTO_PARTITIONING_BY_LOAD = ENABLED,
        AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = partsNum,
        UNIFORM_PARTITIONS = partsNum,
        AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 1000
    )
)
```

### Examples of load initialization {#init-kv-examples}

Example of a command to create a table with 1000 rows:

```bash
{{ ydb-cli }} workload kv init --init-upserts 1000
```

## Deleting a table {#clean}

When the work is complete, you can delete the table:

```bash
{{ ydb-cli }} workload kv clean
```

The following YQL command is executed:

```sql
DROP TABLE `kv_test`
```

### Examples of using clean {#clean-kv-examples}

```bash
{{ ydb-cli }} workload kv clean
```

## Running a load test {#run}

To run the load, execute the command:

```bash
{{ ydb-cli }} workload kv run [workload type...] [global workload options...] [specific workload options...]
```

During this test, workload statistics for each time window are displayed on the screen.

* `workload type`: [The types of workload](#workload-types).
* `global workload options`: [The global options for all types of load](#global-workload-options).
* `specific workload options`: Options of a specific load type.

See the description of the command to run the data load:

```bash
{{ ydb-cli }} workload kv run --help
```

### Global parameters for all types of load {#global-workload-options}

| Parameter name | Short name | Parameter description |
---|---|---
| `--seconds <value>` | `-s <value>` | Duration of the test, in seconds. Default: 10. |
| `--threads <value>` | `-t <value>` | The number of parallel threads creating the load. Default: 10. |
| `--quiet` | - | Outputs only the total result. |
| `--print-timestamp` | - | Print the time together with the statistics of each time window. |
| `--client-timeout` | - | [Transport timeout in milliseconds](../../best_practices/timeouts.md). |
| `--operation-timeout` | - | [Operation timeout in milliseconds](../../best_practices/timeouts.md). |
| `--cancel-after` | - | [Timeout for canceling an operation in milliseconds](../../best_practices/timeouts.md). |
| `--window` | - | Statistics collection window in seconds. Default: 1. |
| `--max-first-key` | - | Maximum value of the primary key of the table. Default: $2^{64} - 1$. |
| `--cols` | - | Number of columns in the table. Default: 2 counting Key. |
| `--rows` | - | Number of affected rows in one query. Default: 1. |

## Upsert load {#upsert-kv}

This load type inserts tuples (key, value1, value2, ..., valueN)

YQL query:

```yql
DECLARE r0 AS Uint64
DECLARE c00 AS String;
DECLARE c01 AS String;
...
DECLARE c0{N - 1} AS String;
DECLARE r1 AS Uint64
DECLARE c10 AS String;
DECLARE c11 AS String;
...
DECLARE c1{N - 1} AS String;
UPSERT INTO `kv_test`(c0, c1, ... cN) VALUES ( (r0, c00, ... c0{N - 1}), (r1, c10, ... c1{N - 1}), ... )
```

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload kv run upsert [global workload options...] [specific workload options...]
```

* `global workload options`: [The global options for all types of load](#global-workload-options).
* `specific workload options`: [Options of a specific load type](#upsert-options).

### Parameters for upsert {#upsert-options}

| Parameter name | Parameter description |
---|---
| `--len` | The size of the rows in bytes that are inserted into the table as values. Default: 8. |

## Insert load {#insert-kv}

This load type inserts tuples (key, value1, value2, ..., valueN)

YQL query:

```yql
DECLARE r0 AS Uint64
DECLARE c00 AS String;
DECLARE c01 AS String;
...
DECLARE c0{N - 1} AS String;
DECLARE r1 AS Uint64
DECLARE c10 AS String;
DECLARE c11 AS String;
...
DECLARE c1{N - 1} AS String;
INSERT INTO `kv_test`(c0, c1, ... cN) VALUES ( (r0, c00, ... c0{N - 1}), (r1, c10, ... c1{N - 1}), ... )
```

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload kv run insert [global workload options...] [specific workload options...]
```

* `global workload options`: [The global options for all types of load](#global-workload-options).
* `specific workload options`: [Options of a specific load type](#insert-options).

### Parameters for insert {#insert-options}

| Parameter name | Parameter description |
---|---
| `--len` | The size of the rows in bytes that are inserted into the table as values. Default: 8. |

## Select load {#select-kv}

This type of load creates SELECT queries that return rows based on an exact match of the primary key.

YQL query:

```yql
DECLARE r0 AS Uint64
DECLARE r1 AS Uint64
...
DECLARE rM AS Uint64
SELECT * FROM `kv_test`(c0, c1, ..., cN) WHERE (
    c0 == r0 OR
    c0 == r1 OR
    ...
    c0 == rM
)
```

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload kv run select [global workload options...]
```

* `global workload options`: [The global options for all types of load](#global-workload-options).
