# DROP ASYNC REPLICATION

The `DROP ASYNC REPLICATION` statement deletes an [asynchronous replication](../../../concepts/async-replication.md) instance. By default, when an asynchronous replication instance is [deleted](../../../concepts/async-replication.md#drop), the following objects are also deleted:

* automatically created [streams of changes](../../../concepts/glossary.md#changefeed)
* [replicas](../../../concepts/glossary.md#replica-object) (optionally)

## Syntax {#syntax}

```yql
DROP ASYNC REPLICATION <name> [CASCADE] [FORCE]
```

### Parameters

* `name` — the name of the asynchronous replication instance.
* `CASCADE` — cascaded deletion of the replicas that were created for a given asynchronous replication instance.
* `FORCE` deletes the replication locally after completion without accessing the source or resolving its credentials. Omit it to preserve ordinary source cleanup. This option applies only to asynchronous replication. If used together, specify `CASCADE` before `FORCE`.

## Deleting without source access {#force}

Use `FORCE` when the source is unavailable and its automatically created changefeeds must be abandoned. First request [forced completion](alter-async-replication.md). A successful `ALTER` response confirms that this request was accepted:

```yql
ALTER ASYNC REPLICATION my_replication
SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');
```

After the request is accepted, issue `FORCE DROP`. It waits for pending local completion before deleting the replication. To retain the destination tables and data:

```yql
DROP ASYNC REPLICATION my_replication FORCE;
```

The retained tables remain writable. To delete the destination tables owned by the replication as well, add `CASCADE`:

```yql
DROP ASYNC REPLICATION my_replication CASCADE FORCE;
```

To inspect the actual completion state, use [`ydb scheme describe`](../../../reference/ydb-cli/commands/scheme-describe.md) with JSON output:

```bash
ydb scheme describe my_replication --format proto-json-base64
```

The `done` member in the response means that the replication has actually reached `DONE`. You can issue `FORCE DROP` while completion is pending; it finishes the remaining local work without waiting for source access.

Local completion and deletion can take time and continue after a destination restart. A table created under the same name after the original destination was deleted is not owned by the replication and is retained.

{% note warning %}

`FORCE` permanently discards the replication's source cleanup metadata. Source changefeeds, consumers, and their configuration remain unchanged. Restoring source connectivity does not trigger delayed cleanup; remove unwanted source resources separately. If ordinary cleanup is required, omit `FORCE`.

{% endnote %}

The cluster administrator must enable `enable_async_replication_force_drop`, which is disabled by default. Enable it only after every deployed server version and every permitted rollback version supports this option and its recovery behavior. Disabling the flag rejects new FORCE requests; it does not cancel already accepted deletion operations.

## Examples {#examples}

This section contains examples of YQL statements that drop the asynchronous replication instance created with the following expression:

```yql
CREATE ASYNC REPLICATION my_replication
FOR original_table AS replica_table
WITH (
    CONNECTION_STRING = 'grpcs://example.com:2135/?database=/Root/another_database',
    TOKEN_SECRET_PATH = 'my_secret'
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
