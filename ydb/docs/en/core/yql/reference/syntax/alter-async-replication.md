# ALTER ASYNC REPLICATION

The `ALTER ASYNC REPLICATION` statement modifies the status and parameters of an [asynchronous replication instance](../../../concepts/async-replication.md).

## Syntax {#syntax}

```yql
ALTER ASYNC REPLICATION <name> SET (option = value [, ...])
```

### Parameters {#params}

* `name` — a name of the asynchronous replication instance.
* `SET (option = value [, ...])` — asynchronous replication parameters:

    * `STATE` — the state of asynchronous replication. This parameter can only be used in combination with the `FAILOVER_MODE` parameter (see below). Valid values are:

        * `DONE` — [completion of the asynchronous replication process](../../../concepts/async-replication.md#done).

    * `FAILOVER_MODE` — the mode for changing the replication state. This parameter can only be used in combination with the `STATE` parameter. Valid values are:

        * `FORCE` — forced failover.

* Authentication settings for the source database (one of the following):

  {% include [x](_includes/async_replication_authentification.md) %}

## Examples {#examples}

The following statement forces the asynchronous replication process to complete:

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = "DONE", FAILOVER_MODE = "FORCE");
```

The following query changes the secret:

```yql
ALTER ASYNC REPLICATION my_replication SET (TOKEN_SECRET_PATH = "my_token");
```

## See also

* [CREATE ASYNC REPLICATION](create-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)
