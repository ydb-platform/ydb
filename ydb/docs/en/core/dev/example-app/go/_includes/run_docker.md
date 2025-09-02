To connect to a locally deployed {{ ydb-short-name }} database according to the [Docker](../../../../quickstart.md) use case, run the following command in the default configuration:

``` bash
( export YDB_ANONYMOUS_CREDENTIALS=1 && cd ydb-go-sdk/examples && \
go run ./basic/native/query -ydb="grpc://localhost:2136/local" )
```

