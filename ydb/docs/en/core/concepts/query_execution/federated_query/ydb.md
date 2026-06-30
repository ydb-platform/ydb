# Working with {{ ydb-short-name }} databases

{% include [!](_includes/experimental_connectors_warning.md) %}

{{ ydb-full-name }} can act as an external data source for another {{ ydb-full-name }} database. This section describes how to set up collaboration between two independent {{ ydb-short-name }} databases in federated query processing mode.

To connect to an external {{ ydb-short-name }} database from another {{ ydb-short-name }} database that acts as a federated query processing engine, you need to perform the following steps on the latter:

1. Prepare authentication credentials to access the remote {{ ydb-short-name }} database. Currently, in federated queries to {{ ydb-short-name }}, the [login and password](../../../security/authentication.md#static-credentials) authentication method is available (other methods are not supported). The password to the external database is stored as a [secret](../../datamodel/secrets.md):


   ```yql
    CREATE SECRET ydb_datasource_user_password WITH (value = "<password>");
   ```

2. Create an [external data source](../../datamodel/external_data_source.md) describing an external {{ ydb-short-name }} database. The `LOCATION` parameter contains the network address of the {{ ydb-short-name }} instance to which a network connection is made. In `DATABASE_NAME`, specify the database name (for example, `local`). For authentication to the external database, use the values of the `LOGIN` and `PASSWORD_SECRET_PATH` parameters. You can enable encryption of connections to the external database using the `USE_TLS="TRUE"` parameter. If encryption is enabled, then in the `<port>` field of the `LOCATION` parameter, you must specify the gRPCs port of the external {{ ydb-short-name }}; otherwise, specify the gRPC port.


   ```yql
   CREATE EXTERNAL DATA SOURCE ydb_datasource WITH (
       SOURCE_TYPE="Ydb",
       LOCATION="<host>:<port>",
       DATABASE_NAME="<database>",
       AUTH_METHOD="BASIC",
       LOGIN="user",
       PASSWORD_SECRET_PATH="ydb_datasource_user_password",
       USE_TLS="TRUE"
   );
   ```

3. {% include [!](_includes/connector_deployment.md) %}
4. [Execute a query](#query) to an external data source.

## Query syntax {#query}

To retrieve data from tables of an external {{ ydb-short-name }} database, use the following SQL query form:


```yql
SELECT * FROM ydb_datasource.`<table_name>`
```


where:

- `ydb_datasource`: external data source identifier
- `<table_name>` is the full table name within the [directory hierarchy](../../architecture.md#ydb-hierarchy) in the {{ ydb-short-name }} database, for example, `table`, `dir1/table1`, or `dir1/dir2/table3`.

If the table is at the top level of the hierarchy (does not belong to any directory), you may omit the backticks around the table name "`":


```yql
SELECT * FROM ydb_datasource.<table_name>
```


## Limitations {#limitations}

When working with external {{ ydb-short-name }} data sources, there are a number of limitations:

1. {% include [!](_includes/supported_requests.md) %}
2. {% include [!](_includes/predicate_pushdown_preamble.md) %}

   | Description | Example | Limitation |
   | --- | --- | --- |
   | Filters of the form `IS NULL`/`IS NOT NULL` | `WHERE column1 IS NULL` or `WHERE column1 IS NOT NULL` |  |
   | Logical conditions `OR`, `NOT`, `AND` and parentheses to control calculation priority. | `WHERE column1 IS NULL OR (column2 IS NOT NULL AND column3 > 10)`. |  |
   | [Comparison operators](../../../yql/reference/syntax/expressions.md#comparison-operators) with other columns or constants. | `WHERE column1 > column2 OR column3 <= 10`. |  |
   | Pattern matching operator `LIKE`. | `WHERE column1 LIKE '_abc%'` | Currently, only simple patterns based on prefixes (`'abc_'`, `'abc%'`), suffixes (`'_abc'`, `'%abc'`), or substring search (`'_abc_'`, `'%abc%'`, `'_abc%'`, `'%abc_'`) are supported for pushdown. If you need to push down more complex patterns, it is recommended to use `REGEXP`. |
   | String pattern matching operator `REGEXP`. | `WHERE column1 REGEXP '.*abc.*'` |  |

   When using other types of filters, pushdown to the source is not performed: filtering of the external table rows will be performed on the federated {{ ydb-short-name }} side, which means that {{ ydb-short-name }} will perform a full scan of the external table at the time of query processing.

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
   | `Utf8` |

## Supported data types

When working with tables located in an external database {{ ydb-short-name }}, users have access to a limited set of data types. All other types, except those listed below, are not supported. In some cases, type conversion is performed, meaning that columns of a table from the external database {{ ydb-short-name }} change their type after the table is read by the database {{ ydb-short-name }} processing the federated query.

| Data type of the external source {{ ydb-short-name }} | Data type in the federated {{ ydb-short-name }} |
| --- | --- |
| `Bool` | `Bool` |
| `Int8` | `Int8` |
| `Int16` | `Int16` |
| `Int32` | `Int32` |
| `Int64` | `Int64` |
| `Uint8` | `Uint8` |
| `Uint16` | `Uint16` |
| `Uint32` | `Uint32` |
| `Uint64` | `Uint64` |
| `Float` | `Float` |
| `Double` | `Double` |
| `String` | `String` |
| `Utf8` | `Utf8` |
| `Date` | `Date` |
| `Datetime` | `Datetime` |
| `Timestamp` | `Timestamp` |
| `Json` | `Json` |
| `JsonDocument` | `Json` |
