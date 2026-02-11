# Database Limits

This section describes the parameters of limits set in {{ ydb-short-name }}.

## Configuring Schema Object Limits {#configure-limits}

Database administrators can configure schema object limits via the `config.yaml` file in the `scheme_shard_config.scheme_limits` section. When not specified, the default values shown in the table below are used.

Example configuration:

```yaml
scheme_shard_config:
  scheme_limits:
    max_paths: 10000              # Maximum number of schema objects (tables, directories). Default: 10000
    max_table_columns: 200        # Maximum columns per table. Default: 200
    max_table_indices: 20         # Maximum indexes per table. Default: 20
    max_shards: 200000            # Maximum shards in database. Default: 200000
```

All fields in `scheme_limits` are optional. Available parameters correspond to the internal names shown in the table below.

## Schema Object Limits {#schema-object}

The table below shows the limits that apply to schema objects: tables, databases, and columns. The "Object" column specifies the type of schema object that the limit applies to.
The "Error type" column shows the status that the query ends with if an error occurs. For more information about statuses, see [Error handling in the API](../../reference/ydb-sdk/error_handling.md).

| Objects | Limit | Value | Explanation | Internal<br/>name | Error<br/>type |
| :--- | :--- | :--- | :--- | :---: | :---: |
| Database | Maximum path depth | 32 | Maximum number of nested path elements (directories, tables). Can be configured via `scheme_limits.max_depth` in config.yaml. | MaxDepth | SCHEME_ERROR |
| Database | Maximum number of paths (schema objects) | 10,000 | Maximum number of path elements (directories, tables) in a database. Can be configured via `scheme_limits.max_paths` in config.yaml. | MaxPaths | GENERIC_ERROR |
| Database | Maximum number of tablets | 200,000 | Maximum number of tablets (table shards and system tablets) that can run in the database. An error is returned if a query to create, copy, or update a table exceeds this limit. When a database reaches the maximum number of tablets, no automatic table sharding takes place. Can be configured via `scheme_limits.max_shards` in config.yaml. | MaxShards | GENERIC_ERROR |
| Database | Maximum object name length | 255 | Limits the number of characters in the name of a schema object, such as a directory or a table. | MaxPathElementLength | SCHEME_ERROR |
| Database | Maximum ACL size | 10 KB | Maximum total size of all access control rules that can be saved for the schema object in question. | MaxAclBytesSize | GENERIC_ERROR |
| Directory | Maximum number of objects | 100,000 | Maximum number of tables and child directories created in a directory. Can be configured via `scheme_limits.max_children_in_dir` in config.yaml. | MaxChildrenInDir | SCHEME_ERROR |
| Table | Maximum number of table shards | 35,000 | Maximum number of table shards. Can be configured via `scheme_limits.max_shards_in_path` in config.yaml. | MaxShardsInPath | GENERIC_ERROR |
| Table | Maximum number of columns | 200 | Limits the total number of columns in a table. Can be configured via `scheme_limits.max_table_columns` in config.yaml. | MaxTableColumns | GENERIC_ERROR |
| Table | Maximum column name length | 255 | Limits the number of characters in a column name. | MaxTableColumnNameLength | GENERIC_ERROR |
| Table | Maximum number of columns in a primary key | 20 | Each table must have a primary key. The number of columns in the primary key may not exceed this limit. Can be configured via `scheme_limits.max_table_key_columns` in config.yaml. | MaxTableKeyColumns | GENERIC_ERROR |
| Table | Maximum number of indexes | 20 | Maximum number of indexes other than the primary key index that can be created in a table. Can be configured via `scheme_limits.max_table_indices` in config.yaml. | MaxTableIndices | GENERIC_ERROR |
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
| Query duration | 2 hours | The maximum amount of time allowed for a single query to execute. | Returns status code `TIMEOUT` |
| Maximum number of sessions per cluster node | 1,000 | The limit on the number of sessions that clients can create with each {{ ydb-short-name }} node. | Returns status code `OVERLOADED` |
| Maximum query text length | 10 KB | The maximum allowable length of YQL query text. | Returns status code `BAD_REQUEST` |
| Maximum size of parameter values | 50 MB | The maximum total size of parameters passed when executing a previously prepared query. | Returns status code `BAD_REQUEST` |
| Maximum size of a row | 50 MB | The maximum total size of all fields of a single row returned or produced by the query. | Returns status code `PRECONDITION_FAILED` |
| Maximum number of [locks](../glossary.md#optimistic-locking) per DataShard | 10,000 | The number of lock ranges per DataShard | Converts some locks to whole-shard locks, which use less memory but lock the entire shard instead of just a part. |

{% cut "Legacy Limits" %}

In previous versions of {{ ydb-short-name }}, queries were typically executed using an API called "Table Service". This API had the following limitations, which have been addressed by replacing it with a new API called "Query Service".

| Parameter | Default | Explanation | Status<br/>in case of<br/>a violation<br/>of the limit |
| :--- | :--- | :--- | :---: |
| Maximum number of rows in query results | 1,000 | The complete results of some queries executed using the `ExecuteDataQuery` method may contain more rows than allowed. In such cases, the query will return the maximum number of rows allowed, and the result will have the `truncated` flag set. | SUCCESS |
| Maximum query result size | 50 MB | The complete results of some queries may exceed the set limit. If this occurs, the query will fail and return no data. | PRECONDITION_FAILED |

{% endcut %}
