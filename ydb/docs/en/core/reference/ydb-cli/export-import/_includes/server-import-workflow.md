### Server-side import workflow {#server-import-workflow}

1. A server-side asynchronous import operation is created.
2. The server reads object metadata from files (`scheme.pb`, `metadata.json`, and so on) in the storage.
3. A new table is created in the database for each object. Target paths **must not exist** — import cannot overwrite existing tables.
4. Data is loaded from files in parallel on all cluster nodes.
