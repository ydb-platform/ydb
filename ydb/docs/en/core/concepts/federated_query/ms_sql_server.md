# Working with Microsoft SQL Server databases

This section provides basic information about working with an external [Microsoft SQL Server](https://learn.microsoft.com/en-us/sql/?view=sql-server-ver16) databases.

To work with an external Microsoft SQL Server database, you need to follow these steps:
1. Create a [secret](../datamodel/secrets.md) containing the password for connecting to the database.
    ```sql
    CREATE OBJECT ms_sql_server_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Create an [external data source](../datamodel/external_data_source.md) that describes a specific Microsoft SQL Server database. The `LOCATION` parameter contains the network address of the Microsoft SQL Server instance to connect to. The `DATABASE_NAME` specifies the database name (for example, `master`). The `LOGIN` and `PASSWORD_SECRET_NAME` parameters are used for authentication to the external database. You can enable encryption for connections to the external database using the `USE_TLS="TRUE"` parameter.
    ```sql
    CREATE EXTERNAL DATA SOURCE ms_sql_server_datasource WITH (
        SOURCE_TYPE="Microsoft SQL Server",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="ms_sql_server_datasource_user_password",
        USE_TLS="TRUE"
    );
    ```
1. {% include [!](_includes/connector_deployment.md) %}
1. [Execute a query](#query) to the database.

## Query syntax { #query }
The following SQL query format is used to work with Microsoft SQL Server:

```sql
SELECT * FROM ms_sql_server_datasource.<table_name>
```

where:
- `ms_sql_server_datasource` - the external data source identifier;
- `<table_name>` - the table name within the external data source.

## Limitations

When working with Microsoft SQL Server clusters, there are a number of limitations:

1. {% include [!](_includes/supported_requests.md) %}
2. {% include [!](_includes/datetime_limits.md) %}
3. {% include [!](_includes/predicate_pushdown.md) %}

## Supported data types

In the Microsoft SQL Server database, the optionality of column values (whether the column can contain `NULL` values or not) is not a part of the data type system. The `NOT NULL` constraint for any column of any table is stored within the `IS_NULLABLE` column the [INFORMATION_SCHEMA.COLUMNS](https://learn.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/columns-transact-sql?view=sql-server-ver16) system table, i.e., at the table metadata level. Therefore, all basic Microsoft SQL Server types can contain `NULL` values by default, and in the {{ ydb-full-name }} type system, they should be mapped to [optional](../yql/reference/yql-core/types/optional.md).

Below is a correspondence table between Microsoft SQL Server types and {{ ydb-short-name }} types. All other data types, except those listed, are not supported.

| Microsoft SQL Server Data Type | {{ ydb-full-name }} Data Type | Notes |
|---|----|------|
|`bit`|`Optional<Bool>`||
|`tinyint`|`Optional<Int8>`||
|`smallint`|`Optional<Int16>`||
|`int`|`Optional<Int32>`||
|`bigint`|`Optional<Int64>`||
|`real`|`Optional<Float>`||
|`float`|`Optional<Double>`||
|`date`|`Optional<Date>`|Valid date range from 1970-01-01 to 2105-12-31. Values outside this range return `NULL`.|
|`smalldatetime`|`Optional<Datetime>`|Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`datetime`|`Optional<Timestamp>`|Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`datetime2`|`Optional<Timestamp>`|Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`binary`|`Optional<String>`||
|`varbinary`|`Optional<String>`||
|`image`|`Optional<String>`||
|`char`|`Optional<Utf8>`||
|`varchar`|`Optional<Utf8>`||
|`text`|`Optional<Utf8>`||
|`nchar`|`Optional<Utf8>`||
|`nvarchar`|`Optional<Utf8>`||
|`ntext`|`Optional<Utf8>`||
