# Working with MySQL databases

{% include [!](_includes/experimental_connectors_warning.md) %}

This section describes basic information about working with an external [MySQL](https://www.mysql.com/) database.

To work with an external MySQL database, you need to perform the following steps:

1. Create a [secret](../../datamodel/secrets.md) containing the password for connecting to the database.


   ```yql
   CREATE SECRET mysql_datasource_user_password WITH (value = "<password>");
   ```

2. Create an [external data source](../../datamodel/external_data_source.md) describing a specific MySQL database. The `LOCATION` parameter contains the network address of the MySQL instance to connect to. `DATABASE_NAME` specifies the database name (for example, `mysql`). For authentication to the external database, the values of the `LOGIN` and `PASSWORD_SECRET_PATH` parameters are used. You can enable encryption of connections to the external database using the `USE_TLS="TRUE"` parameter.


   ```yql
   CREATE EXTERNAL DATA SOURCE mysql_datasource WITH (
       SOURCE_TYPE="MySQL",
       LOCATION="<host>:<port>",
       DATABASE_NAME="<database>",
       AUTH_METHOD="BASIC",
       LOGIN="user",
       PASSWORD_SECRET_PATH="mysql_datasource_user_password",
       USE_TLS="TRUE"
   );
   ```

3. {% include [!](_includes/connector_deployment.md) %}
4. [Execute a query](#query) to the database.

## Query syntax {#query}

The following SQL query form is used to work with MySQL:


```yql
SELECT * FROM mysql_datasource.<table_name>
```


where:

- `mysql_datasource`: external data source identifier
- `<table_name>` is the name of the table inside the external data source.

## Limitations {#limitations}

When working with MySQL clusters, there are a number of limitations:

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

## Supported data types

In MySQL, the optionality of column values (whether a column is allowed or not allowed to contain `NULL` values) is not part of the data type system. The `NOT NULL` constraint for any column of any table is stored as the value of the `IS_NULLABLE` column of the [INFORMATION_SCHEMA.COLUMNS](https://dev.mysql.com/doc/refman/8.4/en/information-schema-columns-table.html) system table, i.e., at the table metadata level. Consequently, all basic MySQL types can by default contain `NULL` values, and in the {{ ydb-full-name }} type system they must be mapped to [optional](../../../yql/reference/types/optional.md) types.

Below is a table of correspondence between MySQL types and {{ ydb-short-name }}. All other data types, except those listed, are not supported.

| MySQL data type | Data type {{ ydb-full-name }} | Notes |
| --- | --- | --- |
| `bool` | `Optional<Bool>` |  |
| `tinyint` | `Optional<Int8>` |  |
| `tinyint unsigned` | `Optional<Uint8>` |  |
| `smallint` | `Optional<Int16>` |  |
| `smallint unsigned` | `Optional<Uint16>` |  |
| `mediumint` | `Optional<Int32>` |  |
| `mediumint unsigned` | `Optional<Uint32>` |  |
| `int` | `Optional<Int32>` |  |
| `int unsigned` | `Optional<Uint32>` |  |
| `bigint` | `Optional<Int64>` |  |
| `bigint unsigned` | `Optional<Uint64>` |  |
| `float` | `Optional<Float>` |  |
| `real` | `Optional<Float>` |  |
| `double` | `Optional<Double>` |  |
| `date` | `Optional<Date>` | Valid date range from 1970-01-01 to 2105-12-31. If the value goes beyond the range boundaries, `NULL` is returned. |
| `datetime` | `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes beyond the range boundaries, the value `NULL` is returned. |
| `timestamp` | `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. If the value goes beyond the range boundaries, the value `NULL` is returned. |
| `tinyblob` | `Optional<String>` |  |
| `blob` | `Optional<String>` |  |
| `mediumblob` | `Optional<String>` |  |
| `longblob` | `Optional<String>` |  |
| `tinytext` | `Optional<String>` |  |
| `text` | `Optional<String>` |  |
| `mediumtext` | `Optional<String>` |  |
| `longtext` | `Optional<String>` |  |
| `char` | `Optional<Utf8>` |  |
| `varchar` | `Optional<Utf8>` |  |
| `binary` | `Optional<String>` |  |
| `varbinary` | `Optional<String>` |  |
| `json` | `Optional<Json>` |  |
