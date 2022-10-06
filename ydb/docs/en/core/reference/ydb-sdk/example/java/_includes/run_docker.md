To connect to a locally deployed YDB database according to the [Docker](../../../../../getting_started/self_hosted/ydb_docker.md) use case, run the following command in the default configuration:

```bash
(cd ydb-java-examples/basic_example/target && \
YDB_ANONYMOUS_CREDENTIALS=1 java -jar ydb-basic-example.jar grpc://localhost:2136?database=/local )
```
