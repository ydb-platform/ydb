Для соединения с развернутой локальной базой данных YDB по сценарию [Docker](../../../../../getting_started/ydb_docker.md) в конфигурации по-умолчанию выполните следующую команду:

``` bash
( export YDB_ANONYMOUS_CREDENTIALS=1 && cd ydb-go-examples && \
go run ./basic -ydb="grpc://localhost:2136?database=/local" )
```
