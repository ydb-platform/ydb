### Server-side export workflow {#server-export-workflow}

1. A server-side asynchronous export operation is created.
2. A service directory `export-{id}` is created in the database root, where `{id}` is the numeric operation identifier.
3. A consistent copy of tables is created in this directory using the `CopyTables` mechanism.
4. Data from each table is written from the copy to the destination storage (S3-compatible object storage or a filesystem mounted in the cluster) in the [export file structure](../file-structure.md) format, with parallel writes from different cluster nodes.
5. After successful completion, the service directory `export-{id}` and table copies are removed.
