# Database Limits

This section describes the parameters of limits set in {{ ydb-short-name }}.

## Schema Object Limits {#schema-object}

The table below shows the limits that apply to schema objects: tables, databases, and columns. The "Object" column specifies the type of schema object that the limit applies to.
The "Error type" column shows the status that the query ends with if an error occurs. For more information about statuses, see [Error handling in the API](../../reference/ydb-sdk/error_handling.md).

| Objects | Limit | Value | Explanation | Internal<br/>name | Error<br/>type |
| :--- | :--- | :--- | :--- | :---: | :---: |
| Database | Maximum path depth | 32 | Maximum number of nested path elements (directories, tables). | MaxDepth | SCHEME_ERROR |
| Database | Maximum number of paths (schema objects) | 10,000 | Maximum number of path elements (directories, tables) in a database. | MaxPaths | GENERIC_ERROR |
| Database | Maximum number of tablets | 200,000 | Maximum number of tablets (table shards and system tablets) that can run in the database. An error is returned if a query to create, copy, or update a table exceeds this limit. When a database reaches the maximum number of tablets, no automatic table sharding takes place. | MaxShards | GENERIC_ERROR |
| Database | Maximum object name length | 255 | Limits the number of characters in the name of a schema object, such as a directory or a table. | MaxPathElementLength | SCHEME_ERROR |
| Database | Maximum ACL size | 10 KB | Maximum total size of all access control rules that can be saved for the schema object in question. | MaxAclBytesSize | GENERIC_ERROR |
| Directory | Maximum number of objects | 100,000 | Maximum number of tables and child directories created in a directory. | MaxChildrenInDir | SCHEME_ERROR |
| Table | Maximum number of table shards | 35,000 | Maximum number of table shards. | MaxShardsInPath | GENERIC_ERROR |
| Table | Maximum number of columns | 200 | Limits the total number of columns in a table. | MaxTableColumns | GENERIC_ERROR |
| Table | Maximum column name length | 255 | Limits the number of characters in a column name. | MaxTableColumnNameLength | GENERIC_ERROR |
| Table | Maximum number of columns in a primary key | 20 | Each table must have a primary key. The number of columns in the primary key may not exceed this limit. | MaxTableKeyColumns | GENERIC_ERROR |
| Table | Maximum number of indexes | 20 | Maximum number of indexes other than the primary key index that can be created in a table. | MaxTableIndices | GENERIC_ERROR |
| Table | Maximum number of followers | 3 | Maximum number of read-only replicas that can be specified when creating a table with followers. | MaxFollowersCount | GENERIC_ERROR |
| Table | Maximum number of tables to copy | 10,000 | Limit on the size of the table list for persistent table copy operations. | MaxConsistentCopyTargets | GENERIC_ERROR |

{wide-content}

## Size Limits for Stored Data {#data-size}

| Parameter | Value | Error type |
| :--- | :--- | :---: |
| Maximum total size of all columns in a primary key | 1 MB | GENERIC_ERROR |
| Maximum size of a string column value | 16 MB | GENERIC_ERROR |

## Analytical Table Limits

| Parameter | Value |
:--- | :---
| Maximum row size | 8 MB |
| Maximum size of an inserted data block | 8 MB |

## Limits on Query Execution {#query}

The table below lists the limits that apply to query execution.

| Parameter | Default | Explanation | Effect<br/>in case of<br/>a violation<br/>of the limit |
| :--- | :--- | :--- | :---: |
| Query duration | 1800 seconds (30 minutes) | The maximum amount of time allowed for a single query to execute. | Return status code TIMEOUT |
| Maximum number of sessions per cluster node | 1,000 | The limit on the number of sessions that clients can create with each {{ ydb-short-name }} node. | Return status code OVERLOADED |
| Maximum query text length | 10 KB | The maximum allowable length of YQL query text. | Return status code BAD_REQUEST |
| Maximum size of parameter values | 50 MB | The maximum total size of parameters passed when executing a previously prepared query. | Return status code BAD_REQUEST |
| Maximum size of a row | 50 MB | The maximum total size of all fields of a single row returned or produced by the query. | Return status code PRECONDITION_FAILED |
| Maximum number of locks per DataShard | 10000 | The number of lock ranges per DataShard | Convert some locks to WholeShardLock |

{% cut "Legacy Limits" %}

In previous versions of {{ ydb-short-name }}, queries were typically executed using an API called "Table Service". This API had the following limitations, which have been addressed by replacing it with a new API called "Query Service".

| Parameter | Default | Explanation | Status<br/>in case of<br/>a violation<br/>of the limit |
| :--- | :--- | :--- | :---: |
| Maximum number of rows in query results | 1,000 | The complete results of some queries executed using the `ExecuteDataQuery` method may contain more rows than allowed. In such cases, the query will return the maximum number of rows allowed, and the result will have the `truncated` flag set. | SUCCESS |
| Maximum query result size | 50 MB | The complete results of some queries may exceed the set limit. If this occurs, the query will fail and return no data. | PRECONDITION_FAILED |

{% endcut %}