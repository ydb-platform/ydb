Для соединения с развернутой локальной базой данных YDB по сценарию [Docker](../../../../../getting_started/self_hosted/ydb_docker.md) в конфигурации по умолчанию  выполните следующую команду:

``` bash
YDB_ANONYMOUS_CREDENTIALS=1 java -jar examples/simple_project/target/ydb-simple-project.jar grpc://localhost:2136?database=/local
```
