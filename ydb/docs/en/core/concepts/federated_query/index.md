# Federated queries

{% note warning %}

This functionality is in "Preview" mode.

{% endnote %}

Federated queries allow retrieving information from various data sources without needing to transfer the data from these sources into {{ ydb-full-name }} storage. Currently, federated queries support interaction with ClickHouse, PostgreSQL, and S3-compatible data stores. Using YQL queries, you can access these databases without the need to duplicate data between systems.

To work with data stored in external DBMSs, it is sufficient to create an [external data source](../datamodel/external_data_source.md). To work with unstructured data stored in S3 buckets, you additionally need to create an [external table](../datamodel/external_table.md). In both cases, it is necessary to  create [secrets](../datamodel/secrets.md) objects first that store confidential data required for authentication in external systems.

You can learn about the internals of the federated query processing system in the [architecture](./architecture.md) section. Detailed information on working with various data sources is provided in the corresponding sections:
- [ClickHouse](clickhouse.md)
- [Greenplum](greenplum.md)
- [MySQL](mysql.md)
- [PostgreSQL](postgresql.md)
- [S3](s3/external_table.md)
- [{{ ydb-short-name }}](ydb.md)
