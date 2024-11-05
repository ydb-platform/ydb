# DROP ASYNC REPLICATION

The `DROP ASYNC REPLICATION` statement deletes an [asynchronous replication](../../../concepts/async-replication.md) instance. When an asynchronous replication instance is [deleted](../../../concepts/async-replication.md#drop), the following objects are also deleted:

* automatically created [streams of changes](../../../concepts/glossary.md#changefeed)
* [replicas](../../../concepts/glossary.md#replica-object) (optionally)

## Syntax {#syntax}

```yql
DROP ASYNC REPLICATION <name> [CASCADE]
```

### Parameters

* `name` — the name of the asynchronous replication instance.
* `CASCADE` — cascaded deletion of the replicas that were created for a given asynchronous replication instance.

## Examples {#examples}

This section contains examples of YQL statements that drop the asynchronous replication instance created with the following expression:

```yql
CREATE ASYNC REPLICATION my_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_NAME = 'my_secret'
);
```

The following statement drops an asynchronous replication instance and the automatically created stream of changes for the `original_table` table, but the `replica_table` table is not deleted:

```yql
DROP ASYNC REPLICATION my_replication;
```

The following statement drops an asynchronous replication instance, the automatically created stream of changes for the `original_table` table, and the `replica_table` table:

```yql
DROP ASYNC REPLICATION my_replication CASCADE;
```

## See also

* [CREATE ASYNC REPLICATION](create-async-replication.md)
* [ALTER ASYNC REPLICATION](alter-async-replication.md)
