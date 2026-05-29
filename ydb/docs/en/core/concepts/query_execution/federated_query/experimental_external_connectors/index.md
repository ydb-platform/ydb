# External Connector-Based Data Sources (Experimental)

{% note warning %}

The data sources described in this section are **experimental**. They require deploying the external [fq-connector-go](../../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go) connector service and explicit enablement in the {{ ydb-short-name }} cluster configuration via the `AvailableExternalDataSources` setting. These features are subject to change and are not recommended for production use without thorough testing.

{% endnote %}

{{ ydb-full-name }} can query data in external databases through the [connector](../architecture.md#connectors) microservice architecture. Each external data source listed here requires the [fq-connector-go](../../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go) connector to be deployed alongside {{ ydb-short-name }}.

Supported external data sources available through connectors:

- [ClickHouse](clickhouse.md)
- [Greenplum](greenplum.md)
- [Microsoft SQL Server](ms_sql_server.md)
- [MySQL](mysql.md)
- [PostgreSQL](postgresql.md)
- [{{ ydb-short-name }}](ydb.md) (YDB-to-YDB federated queries)
