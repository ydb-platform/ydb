# KqpLoad

Runs general performance testing for the {{ ydb-short-name }} cluster by loading all components via the Query Processor layer. The load is similar to that from the [workload](../reference/ydb-cli/commands/workload/index.md) {{ ydb-short-name }} CLI subcommand, but it is generated from within the cluster.

You can run two types of load:

* **Stock**: Simulates a warehouse of an online store: creates multi-product orders, gets a list of orders per customer.
* **Key-value**: Uses the DB as a key-value store.

Before this test, the necessary tables are created. After it's completed, they are deleted.

## Actor parameters {#options}

{% include [load-actors-params](../_includes/load-actors-params.md) %}

| Parameter | Description |
--- | ---
| `DurationSeconds` | Load duration in seconds. |
| `WindowDuration` | Statistics aggregation window duration. |
| `WorkingDir` | Path to the directory to create test tables in. |
| `NumOfSessions` | The number of parallel threads creating the load. Each thread writes data to its own session. |
| `DeleteTableOnFinish` | Set it to `False` if you do not want the created tables deleted after the load stops. This might be helpful when a large table is created upon the actor's first run, and then queries are made to that table. |
| `UniformPartitionsCount` | The number of partitions created in test tables. |
| `WorkloadType` | Type of load.<br/>For Stock:<ul><li>`0`: InsertRandomOrder.</li><li>`1`: SubmitRandomOrder.</li><li>`2`: SubmitSameOrder.</li><li>`3`: GetRandomCustomerHistory.</li><li>`4`: GetCustomerHistory.</li></ul>For Key-Value:<ul><li>`0`: UpsertRandom.</li><li>`1`: InsertRandom.</li><li>`2`: SelectRandom.</li></ul> |
| `Workload` | Kind of load.<br/>`Stock`:<ul><li>`ProductCount`: Number of products.</li><li>`Quantity`: Quantity of each product in stock.</li><li>`OrderCount`: Initial number of orders in the database.</li><li>`Limit`: Minimum number of shards for tables.</li></ul>`Kv`:<ul><li>`InitRowCount`: Before load is generated, the load actor writes the specified number of rows to the table.</li><li>`StringLen`: Length of the `value` string.</li><li>`ColumnsCnt`: Number of columns to use in the table.</li><li>`RowsCnt`: Number of rows to insert or read per SQL query.</li></ul> |

## Examples {#example}

The following actor runs a stock load on the `/slice/db` database by making simple UPSERT queries of `64` threads during `30` seconds.

```proto
KqpLoad: {
    DurationSeconds: 30
    WindowDuration: 1
    WorkingDir: "/slice/db"
    NumOfSessions: 64
    UniformPartitionsCount: 1000
    DeleteTableOnFinish: 1
    WorkloadType: 0
    Stock: {
        ProductCount: 100
        Quantity: 1000
        OrderCount: 100
        Limit: 10
    }
}
```

As a result of the test, the number of successful transactions per second, the number of transaction execution retries, and the number of errors are output.
