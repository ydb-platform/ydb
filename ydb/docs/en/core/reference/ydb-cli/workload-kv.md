# Key-Value load

A simple load type using a YDB database as a Key-Value storage.

## Types of load {#workload-types}

This load test runs several types of load:
* [upsert](#upsert-kv): Using the UPSERT operation, inserts rows that are tuples (key1, key2, ... keyK, value1, value2, ... valueN) into the table created previously with the init command, the K and N numbers are specified in the settings.
* [insert](#insert-kv): The function is the same as the upsert load, only the INSERT operation is used for insertion.
* [select](#select-kv): Reads data using the SELECT * WHERE key = $key operation. A query always affects all table columns, but isn't always a point query, and the number of primary key variations can be controlled using parameters.
* [read-rows](#read-rows-kv): Reads data using the ReadRows operation, which performs faster key reading than select operation. A query always affects all table columns, but isn't always a point query, and the number of primary key variations can be controlled using parameters.
* [mixed](#mixed-kv): Simultaneously writes and reads data, additionally checking that all written data is successfully read.

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

Parameter name | Parameter description
---|---
`--init-upserts <value>` | Number of insertion operations to be performed during initialization. Default: 1000.
`--min-partitions` | Minimum number of shards for tables. Default: 40.
`--partition-size` | Maximum size of one shard (the `AUTO_PARTITIONING_PARTITION_SIZE_MB` setting). Default: 2000.
`--auto-partition` | Enabling/disabling auto-sharding. Possible values: 0 or 1. Default: 1.
`--max-first-key` | Maximum value of the primary key of the table. Default: $2^{64} — 1$.
`--len` | The size of the rows in bytes that are inserted into the table as values. Default: 8.
`--cols` | Number of columns in the table. Default: 2 counting Key.
`--int-cols` | Number of first columns in the table that will have the `Uint64` type; subsequent columns will have the `String` type. Default: 1.
`--key-cols` | Number of first columns in the table included in the key. Default: 1.
`--rows` | Number of affected rows in one query. Default: 1.

The following command is used to create a table:

```yql
CREATE TABLE `kv_test`(
    c0 Uint64,
    c1 Uint64,
    ...
    cI Uint64,
    cI+1 String,
    ...
    cN String,
    PRIMARY KEY(c0, c1, ... cK)) WITH (
        AUTO_PARTITIONING_BY_LOAD = ENABLED,
        AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = partsNum,
        UNIFORM_PARTITIONS = partsNum,
        AUTO_PARTITIONING_PARTITION_SIZE_MB = partSize,
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

Parameter name | Short name | Parameter description
---|---|---
`--seconds <value>` | `-s <value>` | Duration of the test, in seconds. Default: 10.
`--threads <value>` | `-t <value>` | The number of parallel threads creating the load. Default: 10.
`--rate <value>` | - | Total rate for all threads, in requests per second. Default: 0 (no rate limit).
`--quiet` | - | Outputs only the total result.
`--print-timestamp` | - | Print the time together with the statistics of each time window.
`--client-timeout` | - | [Transport timeout in milliseconds](../../dev/timeouts.md).
`--operation-timeout` | - | [Operation timeout in milliseconds](../../dev/timeouts.md).
`--cancel-after` | - | [Timeout for canceling an operation in milliseconds](../../dev/timeouts.md).
`--window` | - | Statistics collection window in seconds. Default: 1.
`--max-first-key` | - | Maximum value of the primary key of the table. Default: $2^{64} - 1$.
`--cols` | - | Number of columns in the table. Default: 2 counting Key.
`--int-cols` | - | Number of first columns in the table that will have the `Uint64` type; subsequent columns will have the `String` type. Default: 1.
`--key-cols` | - | Number of first columns in the table included in the key. Default: 1.
`--rows` | - | Number of affected rows in one query. Default: 1.

## Upsert load {#upsert-kv}

This load type inserts tuples (key, value1, value2, ..., valueN)

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload kv run upsert [global workload options...] [specific workload options...]
```

* `global workload options`: [The global options for all types of load](#global-workload-options).
* `specific workload options`: [Options of a specific load type](#upsert-options).

For example, for the parameters `--rows 2 --cols 3 --int-cols 2`, the YQL query will look like this:

```yql
DECLARE $c0_0 AS Uint64;
DECLARE $c0_1 AS Uint64;
DECLARE $c0_2 AS String;
DECLARE $c1_0 AS Uint64;
DECLARE $c1_1 AS Uint64;
DECLARE $c1_2 AS String;
UPSERT INTO `kv_test` (c0, c1, c2) VALUES ($c0_0, $c0_1, $c0_2), ($c1_0, $c1_1, $c1_2)
```

### Parameters for upsert {#upsert-options}

Parameter name | Parameter description
---|---
`--len` | The size of the rows in bytes that are inserted into the table as values. Default: 8.

## Insert load {#insert-kv}

This load type inserts tuples (key, value1, value2, ..., valueN)

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload kv run insert [global workload options...] [specific workload options...]
```

* `global workload options`: [The global options for all types of load](#global-workload-options).
* `specific workload options`: [Options of a specific load type](#insert-options).

For example, for the parameters `--rows 2 --cols 3 --int-cols 2`, the YQL query will look like this:

```yql
DECLARE $c0_0 AS Uint64;
DECLARE $c0_1 AS Uint64;
DECLARE $c0_2 AS String;
DECLARE $c1_0 AS Uint64;
DECLARE $c1_1 AS Uint64;
DECLARE $c1_2 AS String;
INSERT INTO `kv_test` (c0, c1, c2) VALUES ($c0_0, $c0_1, $c0_2), ($c1_0, $c1_1, $c1_2)
```

### Parameters for insert {#insert-options}

Parameter name | Parameter description
---|---
`--len` | The size of the rows in bytes that are inserted into the table as values. Default: 8.

## Select load {#select-kv}

This type of load creates SELECT queries that return rows based on an exact match of the primary key.

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload kv run select [global workload options...]
```

* `global workload options`: [The global options for all types of load](#global-workload-options).

For example, for the parameters `--rows 2 --cols 3 --int-cols 2`, the YQL query will look like this:

```yql
DECLARE $r0_0 AS Uint64;
DECLARE $r0_1 AS Uint64;
DECLARE $r1_0 AS Uint64;
DECLARE $r1_1 AS Uint64;
SELECT c0, c1, c2 FROM `kv_test` WHERE c0 = $r0_0 AND c1 = $r0_1 OR c0 = $r1_0 AND c1 = $r1_1
```

## Read-rows load {#read-rows-kv}

This type of load creates ReadRows queries that return rows based on an exact match of the primary key.

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload kv run read-rows [global workload options...]
```

* `global workload options`: [The global options for all types of load](#global-workload-options).

## Mixed load {#mixed-kv}

This type of load simultaneously writes and reads tuples (key, value1, value2, ..., valueN), additionally checking that all written data is successfully read.

To run this type of load, execute the command:

```bash
{{ ydb-cli }} workload kv run mixed [global workload options...] [specific workload options...]
```

* `global workload options`: [The global options for all types of load](#global-workload-options).
* `specific workload options` - [Options of a specific load type](#mixed-options).

### Parameters for mixed {#mixed-options}

Parameter name | Parameter description
---|---
`--len` | The size of the rows in bytes that are inserted into the table as values. Default: 8.
`--change-partitions-size` | Enabling/disabling random modification of the `AUTO_PARTITIONING_PARTITION_SIZE_MB` setting. Possible values: 0 or 1. Default: 0.
`--do-select` | Enabling/disabling reads using the [select](#select-kv) query. Possible values: 0 or 1. Default: 1.
`--do-read-rows` | Enabling/disabling reads using the [read-rows](#read-rows-kv) query. Possible values: 0 or 1. Default: 1.