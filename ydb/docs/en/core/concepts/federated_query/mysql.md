# Working with MySQL databases

This section provides basic information about working with an external [MySQL](https://www.mysql.com/) databases.

To work with an external MySQL database, you need to follow these steps:
1. Create a [secret](../datamodel/secrets.md) containing the password for connecting to the database.
    ```sql
    CREATE OBJECT mysql_datasource_user_password (TYPE SECRET) WITH (value = "<password>");
    ```
1. Create an [external data source](../datamodel/external_data_source.md) that describes a specific MySQL database. The `LOCATION` parameter contains the network address of the MySQL instance to connect to. The `DATABASE_NAME` specifies the database name (for example, `mysql`). The `LOGIN` and `PASSWORD_SECRET_NAME` parameters are used for authentication to the external database. You can enable encryption for connections to the external database using the `USE_TLS="TRUE"` parameter.
    ```sql
    CREATE EXTERNAL DATA SOURCE mysql_datasource WITH (
        SOURCE_TYPE="MySQL",
        LOCATION="<host>:<port>",
        DATABASE_NAME="<database>",
        AUTH_METHOD="BASIC",
        LOGIN="user",
        PASSWORD_SECRET_NAME="mysql_datasource_user_password",
        USE_TLS="TRUE"
    );
    ```
1. {% include [!](_includes/connector_deployment.md) %}
1. [Execute a query](#query) to the database.

## Query syntax { #query }
The following SQL query format is used to work with MySQL:

```sql
SELECT * FROM mysql_datasource.<table_name>
```

where:
- `mysql_datasource` - the external data source identifier;
- `<table_name> - the table name within the external data source.

## Limitations

When working with MySQL clusters, there are a number of limitations:

1. {% include [!](_includes/supported_requests.md) %}
2. {% include [!](_includes/datetime_limits.md) %}
3. {% include [!](_includes/predicate_pushdown.md) %}

## Supported data types

In the MySQL database, the optionality of column values (whether the column can contain `NULL` values or not) is not a part of the data type system. The `NOT NULL` constraint for any column of any table is stored within the `IS_NULLABLE` column the [INFORMATION_SCHEMA.COLUMNS](https://dev.mysql.com/doc/refman/8.4/en/information-schema-columns-table.html) system table, i.e., at the table metadata level. Therefore, all basic MySQL types can contain `NULL` values by default, and in the {{ ydb-full-name }} type system they should be mapped to [optional](../../yql/reference/types/optional.md).

Below is a correspondence table between MySQL types and {{ ydb-short-name }} types. All other data types, except those listed, are not supported.

| MySQL Data Type | {{ ydb-full-name }} Data Type | Notes |
|---|----|------|
|`bool`|`Optional<Bool>`||
|`tinyint`|`Optional<Int8>`||
|`tinyint unsigned`|`Optional<Uint8>`||
|`smallint`|`Optional<Int16>`||
|`smallint unsigned`|`Optional<Uint16>`||
|`mediumint`|`Optional<Int32>`||
|`mediumint unsigned`|`Optional<Uint32>`||
|`int`|`Optional<Int32>`||
|`int unsigned`|`Optional<Uint32>`||
|`bigint`|`Optional<Int64>`||
|`bigint unsigned`|`Optional<Uint64>`||
|`float`|`Optional<Float>`||
|`real`|`Optional<Float>`||
|`double`|`Optional<Double>`||
|`date`|`Optional<Date>`|Valid date range from 1970-01-01 to 2105-12-31. Values outside this range return `NULL`.|
|`datetime`| `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`timestamp`| `Optional<Timestamp>` | Valid time range from 1970-01-01 00:00:00 to 2105-12-31 23:59:59. Values outside this range return `NULL`.|
|`tinyblob`|`Optional<String>`||
|`blob`|`Optional<String>`||
|`mediumblob`|`Optional<String>`||
|`longblob`|`Optional<String>`||
|`tinytext`|`Optional<String>`||
|`text`|`Optional<String>`||
|`mediumtext`|`Optional<String>`||
|`longtext`|`Optional<String>`||
|`char`|`Optional<Utf8>`||
|`varchar`|`Optional<Utf8>`||
|`binary`|`Optional<String>`||
|`varbinary`|`Optional<String>`||
|`json`|`Optional<Json>`||
