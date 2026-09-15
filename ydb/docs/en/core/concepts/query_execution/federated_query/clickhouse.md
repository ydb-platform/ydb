# Working with ClickHouse databases

{% include [!](_includes/experimental_connectors_warning.md) %}

This section describes basic information about working with an external database [ClickHouse](https://clickhouse.com).

To work with an external ClickHouse database, you need to follow these steps:

1. Create a [secret](../../datamodel/secrets.md) containing the password for connecting to the database.


   ```yql
   CREATE SECRET clickhouse_datasource_user_password WITH (value = "<password>");
   ```

2. Create an [external data source](../../datamodel/external_data_source.md) that describes the target database inside the ClickHouse cluster. To connect to ClickHouse, you can use either the [native TCP protocol](https://clickhouse.com/docs/en/interfaces/tcp) (`PROTOCOL="NATIVE"`) or the [HTTP protocol](https://clickhouse.com/docs/en/interfaces/http) (`PROTOCOL="HTTP"`). Enable encryption of connections to the external database using the `USE_TLS="TRUE"` parameter.


   ```yql
   CREATE EXTERNAL DATA SOURCE clickhouse_datasource WITH (
       SOURCE_TYPE="ClickHouse",
       LOCATION="<host>:<port>",
       DATABASE_NAME="<database>",
       AUTH_METHOD="BASIC",
       LOGIN="<login>",
       PASSWORD_SECRET_PATH="clickhouse_datasource_user_password",
       PROTOCOL="NATIVE",
       USE_TLS="TRUE"
   );
   ```

3. {% include [!](_includes/connector_deployment.md) %}
4. [Execute a query](#query) to the database.

## Query syntax {#query}

The following SQL query form is used to work with ClickHouse:


```yql
SELECT * FROM clickhouse_datasource.<table_name>
```


where:

- `clickhouse_datasource`: external data source identifier
- `<table_name>` is the name of the table inside the external data source.

## Limitations {#limitations}

When working with ClickHouse clusters, there are a number of limitations:

1. {% include [!](_includes/supported_requests.md) %}
2. {% include [!](_includes/datetime_limits.md) %}
3. {% include [!](_includes/predicate_pushdown_preamble.md) %}

   {% include [!](_includes/predicate_pushdown_examples.md) %}

   Supported data types for filter pushdown:

   | Data type {{ ydb-short-name }} |
   | --- |
   | `Bool` |
   | `Int8` |
   | `Uint8` |
   | `Int16` |
   | `Uint16` |
   | `Int32` |
   | `Uint32` |
   | `Int64` |
   | `Uint64` |
   | `Float` |
   | `Double` |
   | `String` |

## Supported data types

By default, ClickHouse columns physically cannot contain the `NULL` value; however, you can create a table with columns of optional, or [nullable](https://clickhouse.com/docs/en/sql-reference/data-types/nullable), types. The column types displayed by {{ ydb-short-name }} when retrieving data from an external ClickHouse database will depend on whether the ClickHouse table uses primitive or optional types. However, due to the limitations of {{ ydb-short-name }} types used for storing dates and times discussed above, all similar ClickHouse types are displayed in {{ ydb-short-name }} as [optional](../../../yql/reference/types/optional.md).

Below are the tables of correspondence between ClickHouse and {{ ydb-short-name }} types. All other data types, except those listed, are not supported.

### Primitive data types

| ClickHouse data type | Data type {{ ydb-full-name }} | Notes |
| --- | --- | --- |
| `Bool` | `Bool` |  |
| `Int8` | `Int8` |  |
| `UInt8` | `Uint8` |  |
| `Int16` | `Int16` |  |
| `UInt16` | `Uint16` |  |
| `Int32` | `Int32` |  |
| `UInt32` | `Uint32` |  |
| `Int64` | `Int64` |  |
| `UInt64` | `Uint64` |  |
| `Float32` | `Float` |  |
| `Float64` | `Double` |  |
| `Date` | `Date` |  |
| `Date32` | `Optional<Date>` | Valid date range from 1970-01-01 to 2105-12-31. If the value goes outside the range, `NULL` is returned. |
| `DateTime` | `Optional<DateTime>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes out of range, the value `NULL` is returned. |
| `DateTime64` | `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes out of range, the value `NULL` is returned. |
| `String` | `String` |  |
| `FixedString` | `String` | Null bytes `FixedString` are transferred to `String` unchanged. |

### Optional data types

| ClickHouse data type | Data type {{ ydb-full-name }} | Notes |
| --- | --- | --- |
| `Nullable(Bool)` | `Optional<Bool>` |  |
| `Nullable(Int8)` | `Optional<Int8>` |  |
| `Nullable(UInt8)` | `Optional<Uint8>` |  |
| `Nullable(Int16)` | `Optional<Int16>` |  |
| `Nullable(UInt16)` | `Optional<Uint16>` |  |
| `Nullable(Int32)` | `Optional<Int32>` |  |
| `Nullable(UInt32)` | `Optional<Uint32>` |  |
| `Nullable(Int64)` | `Optional<Int64>` |  |
| `Nullable(UInt64)` | `Optional<Uint64>` |  |
| `Nullable(Float32)` | `Optional<Float>` |  |
| `Nullable(Float64)` | `Optional<Double>` |  |
| `Nullable(Date)` | `Optional<Date>` |  |
| `Nullable(Date32)` | `Optional<Date>` | Valid date range from 1970-01-01 to 2105-12-31. If the value goes beyond the range boundaries, `NULL` is returned. |
| `Nullable(DateTime)` | `Optional<DateTime>` | Allowed time value range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes beyond the range boundaries, the `NULL` value is returned. |
| `Nullable(DateTime64)` | `Optional<Timestamp>` | Allowed time value range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes beyond the range boundaries, the value `NULL` is returned. |
| `Nullable(String)` | `Optional<String>` |  |
| `Nullable(FixedString)` | `Optional<String>` | Null bytes `FixedString` are transferred to `String` without changes. |
