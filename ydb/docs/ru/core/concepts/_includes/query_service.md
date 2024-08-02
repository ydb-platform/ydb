# Query service в {{ ydb-short-name }}

QueryService - это новый сервис для выполнения запросов, обладающий меньшим числом [ограничений](../limits-ydb.md#query) по сравнению с TableService.

Он обладает следующими преимуществами по сравнению с TableService:

* Результатом запроса является [поток grpc](https://grpc.io/docs/what-is-grpc/core-concepts/#server-streaming-rpc), что позволяет обойти лимит на количество строк в результате.
* Поддерживают все доступные в {{ ydb-short-name }} [режимы](../transactions.md#modes) транзакций.
* Добавлен режим **вне транзакции**, что позволяет выполнять DDL запросы.
* Позволяет выполнять запросы к [колоночным таблицам](../datamodel/table.md#column-oriented-tables)

{% note info %}

QueryService не является полной заменой TableService, он предоставляет только альтернативный метод выполнения запросов к {{ ydb-short-name }}. Для использования остального функционала, такого как например [пакетная загрузка данных](../../dev/batch-upload.md), следует использовать TableService.

{% endnote %}


## Как воспользоваться {#how-use}

На данный момент QueryService уже доступен в следующих SDK:

- С# (.NET) [https://github.com/ydb-platform/ydb-dotnet-sdk](https://github.com/ydb-platform/ydb-dotnet-sdk)
- Go [https://github.com/ydb-platform/ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk)
- Java [https://github.com/ydb-platform/ydb-java-sdk](https://github.com/ydb-platform/ydb-java-sdk)

Также вы можете [использовать](https://github.com/ydb-platform/ydb-jdbc-driver#using-queryservice-mode) QueryService при работе через YDB [JDBC драйвер](https://github.com/ydb-platform/ydb-jdbc-driver)

## Пример использования 

Вы можете ознакомиться с примером использования QueryService в соответствующем [разделе](../../dev/query-service-app/index.md) документации.
