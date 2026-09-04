# Working with Greenplum databases

{% include [!](_includes/experimental_connectors_warning.md) %}

This section describes the basic information about working with an external [Greenplum](https://greenplum.org) database. Since Greenplum is based on [PostgreSQL](postgresql.md), integrations with them work similarly, and some links below may lead to PostgreSQL documentation.

To work with an external Greenplum database, you need to perform the following steps:

1. Create a [secret](../../datamodel/secrets.md) containing the password for connecting to the database.


   ```yql
   CREATE SECRET greenplum_datasource_user_password WITH (value = "<password>");
   ```

2. Create an [external data source](../../datamodel/external_data_source.md) that describes a specific database within a Greenplum cluster. Pass the network address of the Greenplum [master node](https://greenplum.org/introduction-to-greenplum-architecture/) in the `LOCATION` parameter. By default, the [namespace](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-system_catalogs-pg_namespace.html) `public` is used for reading, but this value can be changed using the optional `SCHEMA` parameter. You can enable encryption of connections to the external database using the `USE_TLS="TRUE"` parameter.


   ```yql
   CREATE EXTERNAL DATA SOURCE greenplum_datasource WITH (
       SOURCE_TYPE="Greenplum",
       LOCATION="<host>:<port>",
       DATABASE_NAME="<database>",
       AUTH_METHOD="BASIC",
       LOGIN="user",
       PASSWORD_SECRET_PATH="greenplum_datasource_user_password",
       USE_TLS="TRUE",
       SCHEMA="<schema>"
   );
   ```

3. {% include [!](_includes/connector_deployment.md) %}
4. [Execute a query](#query) to the database.

## Query syntax {#query}

The following SQL query form is used to work with Greenplum:


```yql
SELECT * FROM greenplum_datasource.<table_name>
```


where:

- `greenplum_datasource`: external data source identifier
- `<table_name>` is the name of the table inside the external data source.

## Limitations {#limitations}

When working with Greenplum clusters, there are a number of limitations:

1. {% include [!](_includes/supported_requests.md) %}
2. {% include [!](_includes/datetime_limits.md) %}
3. {% include [!](_includes/predicate_pushdown_preamble.md) %}

   {% include [!](_includes/predicate_pushdown_examples.md) %}

   Supported data types for filter pushdown:

   | Data type {{ ydb-short-name }} |
   | --- |
   | `Bool` |
   | `Int8` |
   | `Int16` |
   | `Int32` |
   | `Int64` |
   | `Float` |
   | `Double` |

## Supported data types

In the Greenplum database, the optionality flag of column values (whether a column is allowed or not allowed to contain `NULL` values) is not part of the data type system. The `NOT NULL` constraint for each column is implemented as the `attnotnull` attribute in the [pg_attribute](https://docs.vmware.com/en/VMware-Greenplum/6/greenplum-database/ref_guide-system_catalogs-pg_attribute.html) system catalog, i.e., at the table metadata level. Consequently, all basic Greenplum types can contain `NULL` values by default, and in the {{ ydb-full-name }} type system they must be mapped to [optional](../../../yql/reference/types/optional.md) types.

Below is a table of correspondence between Greenplum types and {{ ydb-short-name }}. All other data types, except those listed, are not supported.

| Greenplum data type | Data type {{ ydb-full-name }} | Notes |
| --- | --- | --- |
| `boolean` | `Optional<Bool>` |  |
| `smallint` | `Optional<Int16>` |  |
| `int2` | `Optional<Int16>` |  |
| `integer` | `Optional<Int32>` |  |
| `int` | `Optional<Int32>` |  |
| `int4` | `Optional<Int32>` |  |
| `serial` | `Optional<Int32>` |  |
| `serial4` | `Optional<Int32>` |  |
| `bigint` | `Optional<Int64>` |  |
| `int8` | `Optional<Int64>` |  |
| `bigserial` | `Optional<Int64>` |  |
| `serial8` | `Optional<Int64>` |  |
| `real` | `Optional<Float>` |  |
| `float4` | `Optional<Float>` |  |
| `double precision` | `Optional<Double>` |  |
| `float8` | `Optional<Double>` |  |
| `json` | `Optional<Json>` |  |
| `date` | `Optional<Date>` | Valid date range from 1970-01-01 to 2105-12-31. If the value goes beyond the range boundaries, `NULL` is returned. |
| `timestamp` | `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes beyond the range boundaries, the value `NULL` is returned. |
| `bytea` | `Optional<String>` |  |
| `character` | `Optional<Utf8>` | Default [collation rules](https://www.postgresql.org/docs/current/collation.html), the string is padded with spaces to the required length. |
| `character varying` | `Optional<Utf8>` | Default [collation rules](https://www.postgresql.org/docs/current/collation.html). |
| `text` | `Optional<Utf8>` | Default [collation rules](https://www.postgresql.org/docs/current/collation.html). |
