# dqrun - Utility for Local Debugging of Distributed SQL Engine

`dqrun` is a utility designed for local debugging of a distributed SQL engine. It allows you to run all components of a distributed engine in a single process for more convenient debugging.

## Command-Line Options

- `-s`: if specified, SQL is used; if not specified, the query plan execution specified in s-expression is used.
- `-p <file>`: specify a file with an SQL query or a s-expression.
- `--gateways-cfg <file>`: specify a file with the engine configuration. (Example: [examples/gateways.conf](examples/gateways.conf))
- `--fs-cfg <file>`: specify a file with the file cache configuration. (Example: [examples/fs.conf](examples/fs.conf))
- `--bindings-file <file>`: specify a file with the data schema. (Examples: [examples/bindings_tpch.json](examples/bindings_tpch.json), [examples/bindings_tpch_pg.json](examples/bindings_tpch_pg.json))
- `--dq-host <host>`: set the host to connect to the test utilities `service_node` and `worker_node`.
- `--dq-port <port>`: set the port to connect to the test utilities `service_node` and `worker_node`.

## Example of Local Usage

```bash
dqrun -s -p query.sql --gateways-cfg examples/gateways.conf --fs-cfg examples/fs.conf --bindings-file examples/bindings_tpch.json
```

In this example, `dqrun` will use SQL from the file `query.sql`, engine configuration from the file [examples/gateways.conf](examples/gateways.conf), file cache configuration from the file [examples/fs.conf](examples/fs.conf), and data schema from the file [examples/bindings_tpch.json](examples/bindings_tpch.json).

Download `tpc` data using one of `download_files_h_*.sh` scripts from `yql/queries/tpc_benchmark` number in the name corresponds to the size of data. To download minimal working example use `download_files_h_1.sh`. Move the downloaded `tpc` folder to current directory.

To run dq you will also need a query. The simple example of a query:

```sql
SELECT * FROM bindings.customer LIMIT 10;
```

## Example of Usage as a Client to Test Utilities

```bash
dqrun --dq-host localhost --dq-port 8080 -s -p query.sql --gateways-cfg examples/gateways.conf --fs-cfg examples/fs.conf --bindings-file examples/bindings_tpch.json
```

In this example, `dqrun` will use SQL from the file `query.sql`, engine configuration from the file [examples/gateways.conf](examples/gateways.conf), file cache configuration from the file [examples/fs.conf](examples/fs.conf), and data schema from the file [examples/bindings_tpch.json](examples/bindings_tpch.json). Additionally, the utility will act as a client to the test utilities `service_node` and `worker_node`, using the specified host and port.

## Data Schema File Example (bindings_tpch.json)

An example of a data schema file for queries from the TPC-H benchmark.

```json
{
  "customer": {
    "ClusterType": "s3",
    "path": "h/1/customer/",
    "cluster": "yq-tpc-local",
    "format": "parquet",
    "schema": [
      "StructType",
      [
        ["c_acctbal", ["DataType", "Double"]],
        ["c_address", ["DataType", "String"]],
        ["c_comment", ["DataType", "String"]],
        ["c_custkey", ["DataType", "Int32"]],
        ["c_mktsegment", ["DataType", "String"]],
        ["c_name", ["DataType", "String"]],
        ["c_nationkey", ["DataType", "Int32"]],
        ["c_phone", ["DataType", "String"]]
      ]
    ]
  },
  // Other tables from the TPC-H benchmark
}
```

## Data Schema File Example with PostgreSQL Syntax (bindings_tpch_pg.json)

An example of a data schema file for queries from the TPC-H benchmark using PostgreSQL syntax.

```json
{
  "customer": {
    "ClusterType": "s3",
    "path": "h/1/customer/",
    "cluster": "yq-tpc-local",
    "format": "parquet",
    "schema": [
      "StructType",
      [
        ["c_acctbal", ["PgType", "numeric"]],
        ["c_address", ["PgType", "text"]],
        ["c_comment", ["PgType", "text"]],
        ["c_custkey", ["PgType", "int4"]],
        ["c_mktsegment", ["PgType", "text"]],
        ["c_name", ["PgType", "text"]],
        ["c_nationkey", ["PgType", "int4"]],
        ["c_phone", ["PgType", "text"]]
      ]
    ]
  },
  // Other tables from the TPC-H benchmark
}
```

**Example of gateways.conf:**

```conf
Dq {
    DefaultSettings {
        Name: "HashJoinMode"
        Value: "grace"
    }

    DefaultSettings {
        Name: "UseOOBTransport"
        Value: "true"
    }

    DefaultSettings {
        Name: "UseWideChannels"
        Value: "true"
    }
}

S3 {
    ClusterMapping {
        Name: "yq-clickbench-local"
        Url: "file://./clickbench/"
    }
    ClusterMapping {
        Name: "yq-tpc-local"
        Url: "file://./tpc/"
    }
}
```

In the `Dq` section, parameters for the distributed engine are specified. The complete list of parameters can be found [here](../../providers/dq/common/yql_dq_settings.h).

In the `S3` section, either S3 clusters or directories pretending to be S3 clusters are specified. In this example, the "cluster" `yq-tpc-local` points to the `tpc` directory located locally.

