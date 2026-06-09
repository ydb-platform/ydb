# Источники данных на основе внешних коннекторов (экспериментальные)

{% include [!](../_includes/experimental_connectors_warning.md) %}

Внешние коннекторы представляют собой отдельные микросервисы, через которые {{ ydb-full-name }} обращается к сторонним СУБД при обработке федеративных запросов. Подробнее об архитектуре коннекторов см. в разделе [Коннекторы](../architecture.md#connectors).

В этом разделе описаны источники данных, доступ к которым реализуется через коннектор [fq-connector-go](../../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go):

- [ClickHouse](clickhouse.md)
- [Greenplum](greenplum.md)
- [Microsoft SQL Server](ms_sql_server.md)
- [MySQL](mysql.md)
- [PostgreSQL](postgresql.md)
- [{{ ydb-short-name }}](ydb.md) (федеративные запросы YDB-to-YDB)
