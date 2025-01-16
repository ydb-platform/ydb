Для соединения с развернутой локальной базой данных {{ ydb-short-name }} по сценарию [Docker](../../../../quickstart.md) в конфигурации по умолчанию  выполните следующую команду:

<<<<<<< HEAD
``` bash
( export YDB_ANONYMOUS_CREDENTIALS=1 && cd ydb-go-sdk/examples && \
go run ./basic/native/query -ydb="grpc://localhost:2136/local" )
=======
```bash
( export YDB_ANONYMOUS_CREDENTIALS=1 && cd ydb-go-examples && \
go run ./basic -ydb="grpc://localhost:2136?database=/local" )
>>>>>>> main
```
