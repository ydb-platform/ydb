# Change database settings

Changes database settings. Only a database administrator can perform this operation.

## Syntax

```yql
ALTER DATABASE path SET (key = value, ...)
```

### Parameters

* `path` — path to the database.
* `key` — name of the setting to change:
  * `MAX_PATHS` — [maximum number of paths](../../../../concepts/limits-ydb.md#schema-object) (schema objects) in the database.
  * `MAX_SHARDS` — [maximum number of tablets](../../../../concepts/limits-ydb.md#schema-object) in the database.
  * `MAX_CHILDREN_IN_DIR` — [maximum number of objects in a directory](../../../../concepts/limits-ydb.md#schema-object).
  * `MAX_SHARDS_IN_PATH` — [maximum number of tablets associated with a single schema object](../../../../concepts/limits-ydb.md#schema-object). For example, the maximum number of partitions of one table.
* `value` — new value for the setting.

## Examples

Change the limit on the maximum number of paths (schema objects) for database `/Root/test`:

```yql
ALTER DATABASE `/Root/test` SET (MAX_PATHS = 20000);
```
