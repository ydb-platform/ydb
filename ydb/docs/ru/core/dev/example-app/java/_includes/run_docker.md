Для соединения с развернутой локальной базой данных {{ ydb-short-name }} по сценарию [Docker](../../../../quickstart.md) в конфигурации по умолчанию  выполните следующую команду:

```bash
YDB_ANONYMOUS_CREDENTIALS=1 java -jar ydb-java-examples/query-example/target/ydb-query-example.jar grpc://localhost:{{ def-ports.grpc }}/local
```
