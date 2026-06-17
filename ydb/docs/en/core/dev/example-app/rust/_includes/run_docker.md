To connect to a locally deployed {{ ydb-short-name }} database according to the [Docker](../../../../quickstart.md) use case, run the following command in the default configuration:

```bash
( export YDB_CONNECTION_STRING=grpc://localhost:2136/local && \
  cd ydb-rs-sdk/ydb && cargo run --example basic )
```
