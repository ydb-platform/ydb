Для соединения с развернутой локальной базой данных YDB по сценарию [Docker](../../../../quickstart.md) в конфигурации по умолчанию  выполните следующую команду:

```bash
YDB_ANONYMOUS_CREDENTIALS=1 java -jar ydb-java-examples/query-example/target/ydb-query-example.jar grpc://localhost:2136/local
```
