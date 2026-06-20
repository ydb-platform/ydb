Для подключения к локальной базе {{ ydb-short-name }} в сценарии [Docker](../../../../quickstart.md) выполните:

```bash
  export YDB_CONNECTION_STRING=grpc://localhost:2136/local
  cd ydb-rs-sdk/ydb
  cargo run --example basic
```
