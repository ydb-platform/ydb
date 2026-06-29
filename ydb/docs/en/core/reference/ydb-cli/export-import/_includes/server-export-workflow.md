### Server export operation flow {#server-export-workflow}

1. A server asynchronous export operation is created.
2. A service directory `export-{id}` is created in the root directory of the database, where `{id}` is the numeric operation ID.
3. A consistent snapshot of the tables is created in this directory using the `CopyTables` mechanism.
4. The data of each table is written from the snapshot to the storage in the format of the [export file structure](../file-structure.md) with parallel writing from different nodes of the cluster.
5. After successful completion, the service directory `export-{id}` and the table snapshots are deleted.
