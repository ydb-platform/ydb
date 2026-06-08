# Источники данных на основе внешних коннекторов (экспериментальные)

{% include [!](../_includes/experimental_connectors_warning.md) %}

{{ ydb-full-name }} может выполнять запросы к данным во внешних базах данных через архитектуру [коннекторов](../architecture.md#connectors). Для каждого из перечисленных источников данных необходимо развернуть [fq-connector-go](../../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go) рядом с {{ ydb-short-name }}.

Поддерживаемые внешние источники данных, доступные через коннекторы:

- [ClickHouse](clickhouse.md)
- [Greenplum](greenplum.md)
- [Microsoft SQL Server](ms_sql_server.md)
- [MySQL](mysql.md)
- [PostgreSQL](postgresql.md)
- [{{ ydb-short-name }}](ydb.md) (федеративные запросы YDB-to-YDB)
