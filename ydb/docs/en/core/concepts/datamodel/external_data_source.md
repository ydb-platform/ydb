# External data sources

{% note warning %}

This functionality is in "Experimental" mode.

{% endnote %}

An external data source is an object in {{ ydb-full-name }} that describes the connection parameters to an external data source. For example, in the case of ClickHouse, the external data source describes the network address, login, and password for authentication in the ClickHouse cluster. In the case of S3 ({{ objstorage-name }}), it describes the access credentials and the path to the bucket.

The following example demonstrates creating an external data source pointing to a ClickHouse cluster:

```sql
CREATE EXTERNAL DATA SOURCE test_data_source WITH (
  SOURCE_TYPE="ClickHouse",
  LOCATION="192.168.1.1:8123",
  DATABASE_NAME="default",
  AUTH_METHOD="BASIC",
  USE_TLS="TRUE",
  LOGIN="login",
  PASSWORD_SECRET_NAME="test_password_name"
)
```

After creating an external data source, you can read data from the created `EXTERNAL DATA SOURCE` object. The example below illustrates reading data from the `test_table` table in the `default` database in the ClickHouse cluster:

```sql
SELECT * FROM test_data_source.test_table
```

External data sources allow execution of [federated queries](../federated_query/index.md) for cross-system data analytics tasks.

The following data sources can be used:
- [ClickHouse](../federated_query/clickhouse.md)
- [PostgreSQL](../federated_query/postgresql.md)
- [Connections to S3 ({{ objstorage-name }})](../federated_query/s3/external_data_source.md)