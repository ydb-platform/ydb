# Working with Greenplum databases

This section provides basic information on working with external [Greenplum](https://greenplum.org) databases. Since Greenplum is based on [PostgreSQL](postgresql.md), integrations with them are similar, and some links below may lead to PostgreSQL documentation.

Follow these steps to work with an external Greenplum database:
1. Create a [secret](../datamodel/secrets.md) containing the password for connecting to the database.
    ```sql
    CREATE OBJECT greenplum_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
2. Create an [external data source](../datamodel/external_data_source.md) that describes a specific database within the Greenplum cluster. In the `LOCATION` parameter, pass the network address of the [master node](https://greenplum.org/introduction-to-greenplum-architecture/) of Greenplum. By default, the [namespace](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-system_catalogs-pg_namespace.html) `public` is used for reading, but this value can be changed using the optional `SCHEMA` parameter. The network connection is made using the standard [Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html) over TCP transport (`PROTOCOL="NATIVE"`). You can enable encryption of connections to the external database using the `USE_TLS="TRUE"` parameter.
    ```sql
    CREATE EXTERNAL DATA SOURCE greenplum_datasource WITH (
        SOURCE_TYPE="Greenplum",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="greenplum_datasource_user_password",
        PROTOCOL="NATIVE",
        USE_TLS="TRUE",
        SCHEMA="<schema>"
    );
    ```
1. {% include [!](_includes/connector_deployment.md) %}
1. [Execute a query](#query) to the database.

## Query syntax { #query }
The following SQL query format is used to work with Greenplum:

```sql
SELECT * FROM greenplum_datasource.<table_name>
```

where:
- `greenplum_datasource` - identifier of the external data source;
- `<table_name>` - table name within the external data source.

## Limitations

When working with Greenplum clusters, there are a number of limitations:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

## Supported data types

In the Greenplum database, the optionality of column values (whether a column can contain `NULL` values) is not part of the data type system. The `NOT NULL` constraint for each column is implemented as the `attnotnull` attribute in the system catalog [pg_attribute](https://www.postgresql.org/docs/current/catalog-pg-attribute.html), i.e., at the metadata level of the table. Therefore, all basic Greenplum types can contain `NULL` values by default, and in the {{ ydb-full-name }} type system, they should be mapped to [optional](../yql/reference/yql-core/types/optional.md) types.

Below is a correspondence table between Greenplum and {{ ydb-short-name }} types. All other data types, except those listed, are not supported.

| Greenplum Data Type | {{ ydb-full-name }} Data Type | Notes |
|---|----|------|
| `boolean` | `Optional<Bool>` ||
| `smallint` | `Optional<Int16>` ||
| `int2` | `Optional<Int16>` ||
| `integer` | `Optional<Int32>` ||
| `int` | `Optional<Int32>` ||
| `int4` | `Optional<Int32>` ||
| `serial` | `Optional<Int32>` ||
| `serial4` | `Optional<Int32>` ||
| `bigint` | `Optional<Int64>` ||
| `int8` | `Optional<Int64>` ||
| `bigserial` | `Optional<Int64>` ||
| `serial8` | `Optional<Int64>` ||
| `real` | `Optional<Float>` ||
| `float4` | `Optional<Float>` ||
| `double precision` | `Optional<Double>` ||
| `float8` | `Optional<Double>` ||
| `json` | `Optional<Json>` ||
| `date` | `Optional<Date>` | Valid date range from 1970-01-01 to 2105-12-31. Values outside this range return `NULL`. |
| `timestamp` | `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`. |
| `bytea` | `Optional<String>` ||
| `character` | `Optional<Utf8>` | [Default collation rules](https://www.postgresql.org/docs/current/collation.html), string padded with spaces to the required length. |
| `character varying` | `Optional<Utf8>` | [Default collation rules](https://www.postgresql.org/docs/current/collation.html). |
| `text` | `Optional<Utf8>` | [Default collation rules](https://www.postgresql.org/docs/current/collation.html). |
