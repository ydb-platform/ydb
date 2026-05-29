# Источники данных на основе внешних коннекторов (экспериментальные)

{% note warning %}

Описанные в этом разделе источники данных являются **экспериментальными**. Для их использования необходимо развернуть внешний сервис-коннектор [fq-connector-go](../../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go) и явно включить их в конфигурации кластера {{ ydb-short-name }} через параметр `AvailableExternalDataSources`. Функциональность может измениться и не рекомендуется к использованию в производственной среде без тщательного тестирования.

{% endnote %}

{{ ydb-full-name }} может выполнять запросы к данным во внешних базах данных через архитектуру микросервисов-[коннекторов](../architecture.md#connectors). Для каждого из перечисленных источников данных необходимо развернуть [fq-connector-go](../../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go) рядом с {{ ydb-short-name }}.

Поддерживаемые внешние источники данных, доступные через коннекторы:

- [ClickHouse](clickhouse.md)
- [Greenplum](greenplum.md)
- [Microsoft SQL Server](ms_sql_server.md)
- [MySQL](mysql.md)
- [PostgreSQL](postgresql.md)
- [{{ ydb-short-name }}](ydb.md) (федеративные запросы YDB-to-YDB)
