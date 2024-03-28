To connect to a locally deployed YDB database according to the [Docker](../../../../../getting_started/self_hosted/ydb_docker.md) use case, run the following command in the default configuration:

{% list tabs %}

- Test app over YDB Table service

```bash
( export YDB_ANONYMOUS_CREDENTIALS=1 && cd ydb-go-sdk/examples/basic/native/ && \
go run ./table -ydb="grpc://localhost:2136/local" )
```

- Test app over YDB Query service

```bash
( export YDB_ANONYMOUS_CREDENTIALS=1 && cd ydb-go-sdk/examples/basic/native/ && \
go run ./query -ydb="grpc://localhost:2136/local" )
```

{% endlist %}
