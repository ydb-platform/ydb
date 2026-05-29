# Federated Queries

Federated queries allow retrieving information from various data sources without needing to transfer the data from these sources into {{ ydb-full-name }} storage. Using YQL queries, you can access external databases without the need to duplicate data between systems.

To work with data stored in external DBMSs, it is sufficient to create an [external data source](../../datamodel/external_data_source.md). To work with unstructured data stored in S3 buckets, you additionally need to create an [external table](../../datamodel/external_table.md). In both cases, it is necessary to create [secrets](../../datamodel/secrets.md) objects first that store confidential data required for authentication in external systems.

You can learn about the internals of the federated query processing system in the [architecture](./architecture.md) section. Detailed information on working with various data sources is provided in the corresponding sections:

**Stable data sources** (natively supported, no additional services required):

- [S3-compatible object storage](s3/external_table.md)
- {{ monitoring-full-name }} (metrics)
- YDB Topics (native streaming)

**Experimental data sources** (require deploying the [fq-connector-go](../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go) connector service):

- [ClickHouse](experimental_external_connectors/clickhouse.md)
- [Greenplum](experimental_external_connectors/greenplum.md)
- [Microsoft SQL Server](experimental_external_connectors/ms_sql_server.md)
- [MySQL](experimental_external_connectors/mysql.md)
- [PostgreSQL](experimental_external_connectors/postgresql.md)
- [{{ ydb-short-name }}](experimental_external_connectors/ydb.md) (YDB-to-YDB federated queries)
