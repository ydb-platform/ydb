# Федеративные запросы

Федеративные запросы позволяют получать информацию из различных источников данных без необходимости переноса данных этих источников внутрь {{ ydb-full-name }}. С помощью YQL-запросов вы можете обращаться к внешним базам данных без необходимости дублирования данных между системами.

Для работы с данными, хранящимися во внешних СУБД, достаточно создать [внешний источник данных](../../datamodel/external_data_source.md). Для работы с несхематизированными данными, хранящимися в бакетах S3, нужно дополнительно создать [внешнюю таблицу](../../datamodel/external_table.md). В обоих случаях необходимо предварительно создать [секреты](../../datamodel/secrets.md), хранящие конфиденциальные данные, необходимые для аутентификации во внешних системах.

Вы сможете узнать о внутреннем устройстве системы обработки федеративных запросов в разделе об [архитектуре](./architecture.md). Подробная информация про работу с различными источниками данных приведена в соответствующих разделах:

**Стабильные источники данных** (нативная поддержка, не требуют дополнительных сервисов):

- [S3-совместимые объектные хранилища](s3/external_table.md)
- {{ monitoring-full-name }} (метрики)
- YDB Topics (нативная потоковая обработка)

**Экспериментальные источники данных** (требуют развёртывания коннектора [fq-connector-go](../../../devops/deployment-options/manual/federated-queries/connector-deployment.md#fq-connector-go)):

- [ClickHouse](experimental_external_connectors/clickhouse.md)
- [Greenplum](experimental_external_connectors/greenplum.md)
- [Microsoft SQL Server](experimental_external_connectors/ms_sql_server.md)
- [MySQL](experimental_external_connectors/mysql.md)
- [PostgreSQL](experimental_external_connectors/postgresql.md)
- [{{ ydb-short-name }}](experimental_external_connectors/ydb.md) (федеративные запросы YDB-to-YDB)
