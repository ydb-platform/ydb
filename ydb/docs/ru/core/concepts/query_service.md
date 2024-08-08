# Query service в {{ ydb-short-name }}

QueryService - это сервис выполнения запросов, использующий [grpc потоки](https://grpc.io/docs/what-is-grpc/core-concepts/#server-streaming-rpc) для возврата результата исполнения.

Он обладает следующими особенностями:

* Результатом запроса возвращается в виде потока grpc, что позволяет избежать [лимита](./limits-ydb.md#query) на количество строк в результате.
* Поддерживает выполнение интерактивных транзакции для всех доступных в {{ ydb-short-name }} [режимов](./transactions.md#modes).
* Позволяет выполнять DDL запросы в специальном режиме **вне транзакции**.
* Позволяет выполнять запросы к [колоночным таблицам](./datamodel/table.md#column-oriented-tables)

{% note info %}

QueryService не является полной заменой TableService, он предоставляет только альтернативный метод выполнения запросов к {{ ydb-short-name }}. Для использования остального функционала, такого как например [пакетная загрузка данных](../dev/batch-upload.md), следует использовать TableService.

{% endnote %}


## Как воспользоваться {#how-use}

На данный момент QueryService уже доступен в следующих SDK:

- С# (.NET) [https://github.com/ydb-platform/ydb-dotnet-sdk](https://github.com/ydb-platform/ydb-dotnet-sdk)
- Go [https://github.com/ydb-platform/ydb-go-sdk/v3](https://github.com/ydb-platform/ydb-go-sdk)
- Java [https://github.com/ydb-platform/ydb-java-sdk](https://github.com/ydb-platform/ydb-java-sdk)

Также вы можете [использовать](https://github.com/ydb-platform/ydb-jdbc-driver#using-queryservice-mode) QueryService при работе через YDB [JDBC драйвер](https://github.com/ydb-platform/ydb-jdbc-driver)

## Пример использования 

Вы можете ознакомиться с примером использования QueryService в соответствующем [разделе](../dev/query-service-app/index.md) документации.
