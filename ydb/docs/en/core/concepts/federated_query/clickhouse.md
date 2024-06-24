# Working with ClickHouse databases

This section describes the basic information about working with the external ClickHouse database [ClickHouse](https://clickhouse.com).

To work with the external ClickHouse database, the following steps must be completed:
1. Create a [secret](../datamodel/secrets.md) containing the password to connect to the database.
    ```sql
    CREATE OBJECT clickhouse_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Create an [external data source](../datamodel/external_data_source.md) describing the target database inside the ClickHouse cluster. To connect to ClickHouse, you can use either the [native TCP protocol](https://clickhouse.com/docs/en/interfaces/tcp) (`PROTOCOL="NATIVE"`) or the [HTTP protocol](https://clickhouse.com/docs/en/interfaces/http) (`PROTOCOL="HTTP"`). To enable encryption for connections to the external database, use the `USE_TLS="TRUE"` parameter.
    ```sql
    CREATE EXTERNAL DATA SOURCE clickhouse_datasource WITH (
        SOURCE_TYPE="ClickHouse", 
        LOCATION="<host>:<port>", 
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="<login>",
        PASSWORD_SECRET_NAME="clickhouse_datasource_user_password",
        PROTOCOL="NATIVE",
        USE_TLS="TRUE"
    );
    ```

1. {% include [!](_includes/connector_deployment.md) %}
1. [Execute a query](#query) to the database.


## Query syntax {#query}
To work with ClickHouse, use the following SQL query form:

```sql
SELECT * FROM clickhouse_datasource.<table_name>
```

Where:
- `clickhouse_datasource` is the identifier of the external data source;
- `<table_name>` is the table's name within the external data source.

## Limitations

There are several limitations when working with ClickHouse clusters:

1. {% include [!](_includes/supported_requests.md) %}
1. {% include [!](_includes/datetime_limits.md) %}
1. {% include [!](_includes/predicate_pushdown.md) %}

## Supported data types

By default, ClickHouse columns cannot physically contain `NULL` values. However, users can create tables with columns of optional or [nullable](https://clickhouse.com/docs/en/sql-reference/data-types/nullable) types. The column types displayed in {{ ydb-short-name }} when extracting data from the external ClickHouse database will depend on whether primitive or optional types are used in the ClickHouse table. Due to the previously discussed limitations of {{ ydb-short-name }} types used to store dates and times, all similar ClickHouse types are displayed in {{ ydb-short-name }} as [optional](../../yql/reference/types/optional.md).

Below are the mapping tables for ClickHouse and {{ ydb-short-name }} types. All other data types, except those listed, are not supported.

### Primitive data types

|ClickHouse data type|{{ ydb-full-name }} data type|Notes|
|---|----|------|
|`Bool`|`Bool`||
|`Int8`|`Int8`||
|`UInt8`|`Uint8`||
|`Int16`|`Int16`||
|`UInt16`|`Uint16`||
|`Int32`|`Int32`||
|`UInt32`|`Uint32`||
|`Int64`|`Int64`||
|`UInt64`|`Uint64`||
|`Float32`|`Float`||
|`Float64`|`Double`||
|`Date`|`Date`||
|`Date32`|`Optional<Date>`|Valid date range from 1970-01-01 to 2105-12-31. Values outside this range return `NULL`.|
|`DateTime`|`Optional<DateTime>`|Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`DateTime64`|`Optional<Timestamp>`|Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`String`|`String`||
|`FixedString`|`String`|Null bytes in `FixedString` are transferred to `String` unchanged.|

### Optional data types

|ClickHouse data type|{{ ydb-full-name }} data type|Notes|
|---|----|------|
|`Nullable(Bool)`|`Optional<Bool>`||
|`Nullable(Int8)`|`Optional<Int8>`||
|`Nullable(UInt8)`|`Optional<Uint8>`||
|`Nullable(Int16)`|`Optional<Int16>`||
|`Nullable(UInt16)`|`Optional<Uint16>`||
|`Nullable(Int32)`|`Optional<Int32>`||
|`Nullable(UInt32)`|`Optional<Uint32>`||
|`Nullable(Int64)`|`Optional<Int64>`||
|`Nullable(UInt64)`|`Optional<Uint64>`||
|`Nullable(Float32)`|`Optional<Float>`||
|`Nullable(Float64)`|`Optional<Double>`||
|`Nullable(Date)`|`Optional<Date>`||
|`Nullable(Date32)`|`Optional<Date>`|Valid date range from 1970-01-01 to 2105-12-31. Values outside this range return `NULL`.|
|`Nullable(DateTime)`|`Optional<DateTime>`|Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`Nullable(DateTime64)`|`Optional<Timestamp>`|Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`Nullable(String)`|`Optional<String>`||
|`Nullable(FixedString)`|`Optional<String>`|Null bytes in `FixedString` are transferred to `String` unchanged.|