Для соединения с развернутой локальной базой данных YDB по сценарию [Docker](../../../../quickstart.md) в конфигурации по умолчанию  выполните следующую команду:

``` bash
( export YDB_ANONYMOUS_CREDENTIALS=1 && cd ydb-go-examples && \
go run ./basic -ydb="grpc://localhost:2136?database=/local" )
```
