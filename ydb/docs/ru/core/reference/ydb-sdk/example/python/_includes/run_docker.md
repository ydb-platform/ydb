Для соединения с развернутой локальной базой данных YDB по сценарию [Docker](../../../../../getting_started/self_hosted/ydb_docker.md) в конфигурации по умолчанию  выполните следующую команду:

``` bash
YDB_ANONYMOUS_CREDENTIALS=1 \
python3 ydb-python-sdk/examples/basic_example_v1/ -e grpc://localhost:2136 -d /local
```
