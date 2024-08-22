# Database limits

This section describes the parameters of limits set in {{ ydb-short-name }}.

## Schema object limits {#schema-object}

The table below shows the limits that apply to schema objects: tables, databases, and columns. The _Object_ column specifies the type of schema object that the limit applies to.
The _Error type_ column shows the status that the query ends with if an error occurs. For more information about statuses, see [Error handling in the API](../../reference/ydb-sdk/error_handling.md).

| Objects | Limit | Value | Explanation | Internal<br/>name | Error<br/>type |
| :--- | :--- | :--- | :--- | :---: | :---: |
| Database | Maximum path depth | 32 | Maximum number of nested path elements (directories, tables). | MaxDepth | SCHEME_ERROR |
| Database | Maximum number of paths (schema objects) | 10,000 | Maximum number of path elements (directories, tables) in a database. | MaxPaths | GENERIC_ERROR |
| Database | Maximum number of tablets | 200,000 | Maximum number of tablets (table shards and system tablets) that can run in the database. An error is returned if a query to create, copy, or update a table exceeds this limit. When a database reaches the maximum number of tablets, no automatic table sharding takes place. | MaxShards | GENERIC_ERROR |
| Database | Maximum object name length | 255 | Limits the number of characters in the name of a schema object, such as a directory or a table | MaxPathElementLength | SCHEME_ERROR |
| Database | Maximum ACL size | 10 KB | Maximum total size of all access control rules that can be saved for the schema object in question. | MaxAclBytesSize | GENERIC_ERROR |
| Directory | Maximum number of objects | 100,000 | Maximum number of tables and child directories created in a directory. | MaxChildrenInDir | SCHEME_ERROR |
| Table | Maximum number of table shards | 35,000 | Maximum number of table shards. | MaxShardsInPath | GENERIC_ERROR |
| Table | Maximum number of columns | 200 | Limits the total number of columns in a table. | MaxTableColumns | GENERIC_ERROR |
| Table | Maximum column name length | 255 | Limits the number of characters in a column name | MaxTableColumnNameLength | GENERIC_ERROR |
| Table | Maximum number of columns in a primary key | 20 | Each table must have a primary key. The number of columns in the primary key may not exceed this limit. | MaxTableKeyColumns | GENERIC_ERROR |
| Table | Maximum number of indexes | 20 | Maximum number of indexes other than the primary key index that can be created in a table. | MaxTableIndices | GENERIC_ERROR |
| Table | Maximum number of followers | 3 | Maximum number of read-only replicas that can be specified when creating a table with followers. | MaxFollowersCount | GENERIC_ERROR |
| Table | Maximum number of tables to copy | 10,000 | Limit on the size of the table list for persistent table copy operations | MaxConsistentCopyTargets | GENERIC_ERROR |

## Size limits for stored data {#data-size}

| Parameter | Value | Error type |
| :--- | :--- | :---: |
| Maximum total size of all columns in a primary key | 1 MB | GENERIC_ERROR |
| Maximum size of a string column value | 16 MB | GENERIC_ERROR |

## Analytical table limits

| Parameter | Value |
:--- | :---
| Maximum row size | 8 MB |
| Maximum size of an inserted data block | 8 MB |

## Limits on query execution {#query}

The table below lists the limits that apply to query execution. The _Call_ column specifies the public API call that will end with the error status specified in the _Status_ column.

| Parameter | Value | Call | Explanation | Status<br/>in case of<br/>a violation<br/>of the limit |
| :--- | :--- | :--- | :--- | :---: |
| Maximum number of rows in query results | 1,000 | ExecuteDataQuery | Complete results of some queries executed using the `ExecuteDataQuery` method may contain more rows than allowed. In this case, a query will return the maximum number of rows allowed, and the result will have the `Truncated` flag set. This limitation does not apply to other query execution methods, like calls to methods of `QueryService` interface, [scan queries](../../concepts/scan_query.md), or `ReadTable` operations. | SUCCESS |
| Maximum query result size | 50 MB | ExecuteDataQuery | Complete results of some queries may exceed the set limit. In this case, a query will fail returning no data. | PRECONDITION_FAILED |
| Maximum number of sessions per cluster node | 1,000 | CreateSession | Using the library for working with {{ ydb-short-name }}, an application can create sessions within a connection. Sessions are linked to a node. You can create a limited number of sessions with a single node. | OVERLOADED |
| Maximum query text length | 10 KB | ExecuteDataQuery | Limit on the length of YQL query text. | BAD_REQUEST |
| Maximum size of parameter values | 50 MB | ExecuteDataQuery | Limit on the total size of the parameters passed when executing a previously prepared query. | BAD_REQUEST |

## Topic limits {#topic}

| Parameter | Value |
| :--- | :--- |
| Maximum size of a transmitted message | 12 MB |
