To connect to a locally deployed {{ ydb-short-name }} database according to the [Docker](../../../../quickstart.md) use case, run the following command in the default configuration:

```bash
(cd ydb-java-examples/basic_example/target && \
YDB_ANONYMOUS_CREDENTIALS=1 java -jar ydb-basic-example.jar grpc://localhost:2136?database=/local )
```
