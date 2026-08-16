# Working with PostgreSQL databases

{% include [!](_includes/experimental_connectors_warning.md) %}

This section describes the basic information about working with an external [PostgreSQL](http://postgresql.org) database.

To work with an external PostgreSQL database, you need to perform the following steps:

1. Create a [secret](../../datamodel/secrets.md) containing the password for connecting to the database.


   ```yql
   CREATE SECRET postgresql_datasource_user_password WITH (value = "<password>");
   ```

2. Create an [external data source](../../datamodel/external_data_source.md) that describes a specific database within a PostgreSQL cluster. By default, the [namespace](https://www.postgresql.org/docs/current/catalog-pg-namespace.html) `public` is used for reading, but this value can be changed using the optional parameter `SCHEMA`. The network connection is established via the standard ([Frontend/Backend Protocol](https://www.postgresql.org/docs/current/protocol.html)) over TCP transport (`PROTOCOL="NATIVE"`). Encryption of connections to the external database can be enabled using the parameter `USE_TLS="TRUE"`.


   ```yql
   CREATE EXTERNAL DATA SOURCE postgresql_datasource WITH (
       SOURCE_TYPE="PostgreSQL",
       LOCATION="<host>:<port>",
       DATABASE_NAME="<database>",
       AUTH_METHOD="BASIC",
       LOGIN="user",
       PASSWORD_SECRET_PATH="postgresql_datasource_user_password",
       PROTOCOL="NATIVE",
       USE_TLS="TRUE",
       SCHEMA="<schema>"
   );
   ```

3. {% include [!](_includes/connector_deployment.md) %}
4. [Execute a query](#query) to the database.

## Query syntax {#query}

The following SQL query form is used to work with PostgreSQL:


```yql
SELECT * FROM postgresql_datasource.<table_name>
```


where:

- `postgresql_datasource`: external data source identifier
- `<table_name>` is the name of the table inside the external data source.

## Limitations {#limitations}

When working with PostgreSQL clusters, there are a number of limitations:

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
   | `Decimal` |

## Supported data types

In a PostgreSQL database, the optionality of column values (whether a column is allowed or not allowed to contain `NULL` values) is not part of the data type system. The constraint `NOT NULL` for each column is implemented as an attribute `attnotnull` in the system catalog [pg_attribute](https://www.postgresql.org/docs/current/catalog-pg-attribute.html), that is, at the table metadata level. Consequently, all basic PostgreSQL types can by default contain `NULL` values, and in the {{ ydb-full-name }} type system they must be mapped to [optional](../../../yql/reference/types/optional.md) types.

Below is a table of correspondence between PostgreSQL types and {{ ydb-short-name }}. All other data types, except those listed, are not supported.

| PostgreSQL data type | Data type {{ ydb-full-name }} | Notes |
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
| `date` | `Optional<Date>` | Valid date range from 1970-01-01 to 2105-12-31. If the value goes outside the range, `NULL` is returned. |
| `timestamp` | `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes outside the range, the value `NULL` is returned. |
| `bytea` | `Optional<String>` |  |
| `character` | `Optional<Utf8>` | [Default collation rules](https://www.postgresql.org/docs/current/collation.html), the string is padded with spaces to the required length. |
| `character varying` | `Optional<Utf8>` | [Default collation rules](https://www.postgresql.org/docs/current/collation.html). |
| `text` | `Optional<Utf8>` | Default [collation rules](https://www.postgresql.org/docs/current/collation.html). |
| `json` | `Optional<Json>` |  |
| `numeric(p,s)` | `Optional<Decimal(p,s)>` | `p` (precision) - the total number of digits in the number, `s` (scale) - the number of digits after the decimal point. Types `numeric` without parameters (so-called "unconstrained") are converted to `Optional<Decimal(35, 0)>`. Types `numeric` with `p > 35` or `s < 0` are not supported. |
