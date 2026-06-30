To connect to a local {{ ydb-short-name }} database in the [Docker](../../../../quickstart.md) scenario, run:


```bash
export YDB_CONNECTION_STRING=grpc://localhost:2136/local
cd ydb-rs-sdk/ydb
cargo run --example basic
```
