# Key-Value load

A simple type of load that uses the YDB database as a Key-Value storage

## Types of load {#workload_types}

This load test contains 3 types of load:
* [upsert](#upsertKv) - using the UPSERT operation, inserts rows into a table created in advance by the init command, which are tuples (key, value1, value2, ... valueN), the number N is set in the parameters
* [insert](#insertKv) - the functionality is the same as that of the upsert load, but the INSERT operation is used for insertion
* [select](#selectKv) - reads data using the SELECT * WHERE key = $key operation. The query always affects all columns of the table, but is not always point-based, the number of variations of the primary key can be controlled using parameters.

## Load test initialization {#init}

To get started, you need to create tables, when creating, you can specify how many rows you need to insert during initialization
```bash
{{ ydb-cli }} workload kv init [init options...]
```

* `init options` — [initialization parameters](#init_options).

See the description of the command to initialize the table:

```bash
{{ ydb-cli }} workload kv init --help
```

### Available parameters {#init_options}

Parameter name | Parameter Description
---|---
`--init-upserts <value>` | The number of insertion operations to be performed during initialization. Default value: 1000.
`--min-partitions` | Minimum number of shards for tables. Default value: 40.
`--auto-partition` | Enabling/disabling autosharding. Possible values: 0 or 1. Default value: 1.
`--max-first-key` | The maximum value of the primary key of the table. Default value: $2^{64} - 1$.
`--len` | The size of the rows in bytes that are inserted into the table as values. Default value: 8.
`--cols` | Number of columns in the table. Default value: 2, counting Key.
`--rows` | The number of affected rows in a single request. Default value: 1.


To create a table, use the following command:
```sql
CREATE TABLE `DbPath/kv_test`(
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

### Load initialization examples {#init-kv-examples}

Example of a command to create a table with 1000 rows:

```bash
{{ ydb-cli }} workload kv init --init-upserts 1000
```

## Deleting a table {#clean}

After completing the work, you can delete the table
```bash
{{ ydb-cli }} workload kv clean
```

The following YQL command is executed:
```sql
DROP TABLE `DbPath/kv_test`
```

### Examples of using clean {#clean-kv-examples}


```bash
{{ ydb-cli }} workload kv clean
```

## Running a load test {#run}

To start the load, run the command:
```bash
{{ ydb-cli }} workload kv run [workload type...] [global workload options...] [specific workload options...]
```
During the test, load statistics for each time window are displayed on the screen.

* `workload type` — [types of load](#workload_types).
* `global workload options` - [general parameters for all types of load](#global_workload_options).
* `specific workload options` - parameters of a specific type of load.

See the description of the command to start the load:

```bash
{{ ydb-cli }} workload kv run --help
```

### Common parameters for all types of load {#global_workload_options}

Parameter name | Short name | Parameter Description
---|---|---
`-- seconds <value>` | `-s <value>` | Duration of the test, sec. Default value: 10.
`--threads <value>` | `-t <value>` | The number of parallel threads creating the load. Default value: 10.
`--quiet` | - | Outputs only the final test result.
`--print-timestamp` | - | Print the time together with the statistics of each time window.
`--client-timeout` | - | [Transport timeout in milliseconds](../../../../../best_practices/timeouts.md).
`--operation-timeout` | - | [Operation timeout in milliseconds](../../../../../best_practices/timeouts.md).
`--cancel-after` | - | [Operation cancellation timeout in milliseconds](../../../../../best_practices/timeouts.md).
`--window` | - | Duration of the statistics collection window in seconds. Default value: 1.
`--max-first-key` | - | The maximum value of the primary key of the table. Default value: $2^{64} - 1$.
`--cols` | - | Number of columns in the table. Default value: 2, counting Key.
`--rows` | - |The number of affected rows in a single request. Default value: 1.


## Load upsert {#upsertKv}

This type of load inserts tuples (key, value1, value2, ..., valueN)

YQL Query:
```sql
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

To start this type of load, you need to run the command:
```bash
{{ ydb-cli }} workload kv run upsert [global workload options...] [specific workload options...]
```

* `global workload options` - [general parameters for all types of load](#global_workload_options).
* `specific workload options` - [parameters of a specific type of load](#upsert_options)

### Parameters for upsert {#upsert_options}
Parameter name | Parameter Description
---|---
`--len` | The size of the rows in bytes that are inserted into the table as values. Default value: 8.

## Load insert {#insertKv}

This type of load inserts tuples (key, value1, value2, ..., valueN)

YQL Query:
```sql
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

To start this type of load, you need to run the command:
```bash
{{ ydb-cli }} workload kv run insert [global workload options...] [specific workload options...]
```

* `global workload options` - [general parameters for all types of load](#global_workload_options).
* `specific workload options` - [parameters of a specific type of load](#insert_options)

### Parameters for insert {#insert_options}
Parameter name | Parameter Description
---|---
`--len` | The size of the rows in bytes that are inserted into the table as values. Default value: 8.

## Load select {#selectKv}

This type of load creates SELECT queries that return rows based on the exact match of the primary key.

YQL Query:
```sql
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

To start this type of load, you need to run the command:
```bash
{{ ydb-cli }} workload kv run select [global workload options...]
```

* `global workload options` - [general parameters for all types of load](#global_workload_options).
