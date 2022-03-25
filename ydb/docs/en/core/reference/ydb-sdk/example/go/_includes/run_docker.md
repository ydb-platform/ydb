To connect to a locally deployed YDB database according to the [Docker](../../../../../getting_started/self_hosted/ydb_docker.md) use case, run the following command in the default configuration:

```bash
( export YDB_ANONYMOUS_CREDENTIALS=1 && cd ydb-go-examples && \
go run ./basic -ydb="grpc://localhost:2136?database=/local" )
```

