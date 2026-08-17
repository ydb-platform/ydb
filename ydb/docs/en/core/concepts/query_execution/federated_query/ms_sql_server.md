# Working with Microsoft SQL Server databases

{% include [!](_includes/experimental_connectors_warning.md) %}

This section describes the basic information about working with an external [Microsoft SQL Server](https://learn.microsoft.com/ru-ru/sql/?view=sql-server-ver16) database.

To work with an external Microsoft SQL Server database, you need to perform the following steps:

1. Create a [secret](../../datamodel/secrets.md) containing the password for connecting to the database.


   ```yql
   CREATE SECRET ms_sql_server_datasource_user_password WITH (value = "<password>");
   ```

2. Create an [external data source](../../datamodel/external_data_source.md) that describes a specific Microsoft SQL Server database. The `LOCATION` parameter contains the network address of the Microsoft SQL Server instance to which the connection is made. The `DATABASE_NAME` parameter specifies the database name (for example, `master`). The values of the `LOGIN` and `PASSWORD_SECRET_PATH` parameters are used for authentication to the external database. You can enable encryption of connections to the external database using the `USE_TLS="TRUE"` parameter.


   ```yql
   CREATE EXTERNAL DATA SOURCE ms_sql_server_datasource WITH (
       SOURCE_TYPE="MsSQLServer",
       LOCATION="<host>:<port>",
       DATABASE_NAME="<database>",
       AUTH_METHOD="BASIC",
       LOGIN="user",
       PASSWORD_SECRET_PATH="ms_sql_server_datasource_user_password",
       USE_TLS="TRUE"
   );
   ```

3. {% include [!](_includes/connector_deployment.md) %}
4. [Execute a query](#query) on the database.

## Query syntax {#query}

The following SQL query form is used to work with Microsoft SQL Server:


```yql
SELECT * FROM ms_sql_server_datasource.<table_name>
```


where:

- `ms_sql_server_datasource`: external data source identifier
- `<table_name>` is the name of the table inside the external data source.

## Limitations {#limitations}

When working with Microsoft SQL Server clusters, there are a number of limitations:

1. {% include [!](_includes/supported_requests.md) %}
2. {% include [!](_includes/datetime_limits.md) %}
3. {% include [!](_includes/predicate_pushdown_preamble.md) %}

   {% include [!](_includes/predicate_pushdown_examples.md) %}

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

In a Microsoft SQL Server database, the optionality flag of column values (whether the column is allowed or prohibited from containing `NULL` values) is not part of the data type system. The `NOT NULL` constraint for any column of any table is stored as the value of the `IS_NULLABLE` column of the [INFORMATION_SCHEMA.COLUMNS](https://learn.microsoft.com/ru-ru/sql/relational-databases/system-information-schema-views/columns-transact-sql?view=sql-server-ver16) system table, that is, at the table metadata level. Consequently, all basic Microsoft SQL Server data types can by default contain `NULL` values, and in the {{ ydb-full-name }} type system they must be mapped to [optional](../../../yql/reference/types/optional.md) types.

Below is a table of correspondence between Microsoft SQL Server types and {{ ydb-short-name }}. All other data types, except those listed, are not supported.

| Microsoft SQL Server data type | Data type {{ ydb-full-name }} | Notes |
| --- | --- | --- |
| `bit` | `Optional<Bool>` |  |
| `tinyint` | `Optional<Int8>` |  |
| `smallint` | `Optional<Int16>` |  |
| `int` | `Optional<Int32>` |  |
| `bigint` | `Optional<Int64>` |  |
| `real` | `Optional<Float>` |  |
| `float` | `Optional<Double>` |  |
| `date` | `Optional<Date>` | Valid date range from 1970-01-01 to 2105-12-31. If the value goes beyond the range boundaries, `NULL` is returned. |
| `smalldatetime` | `Optional<Datetime>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes beyond the range boundaries, the value `NULL` is returned. |
| `datetime` | `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes beyond the range boundaries, the value `NULL` is returned. |
| `datetime2` | `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes beyond the range boundaries, the value `NULL` is returned. |
| `binary` | `Optional<String>` |  |
| `varbinary` | `Optional<String>` |  |
| `image` | `Optional<String>` |  |
| `char` | `Optional<Utf8>` |  |
| `varchar` | `Optional<Utf8>` |  |
| `text` | `Optional<Utf8>` |  |
| `nchar` | `Optional<Utf8>` |  |
| `nvarchar` | `Optional<Utf8>` |  |
| `ntext` | `Optional<Utf8>` |  |
