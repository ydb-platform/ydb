### Server export operation flow {#server-export-workflow}

1. A server-side asynchronous export operation is created.
2. A service directory `export-{id}` is created in the root directory of the database, where `{id}` is the numeric operation ID.
3. A consistent copy of the tables is created in this directory using the `CopyTables` mechanism.
4. The data of each table is written from the copy to the storage in the [export file structure](../file-structure.md) format, with parallel writing from different nodes of the cluster.
5. After successful completion, the service directory `export-{id}` and the table copies are deleted.
