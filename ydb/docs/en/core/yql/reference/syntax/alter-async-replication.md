# ALTER ASYNC REPLICATION

The `ALTER ASYNC REPLICATION` statement modifies the status and parameters of an [asynchronous replication instance](../../../concepts/async-replication.md).

## Syntax {#syntax}

```yql
ALTER ASYNC REPLICATION <name> SET (option = value [, ...])
```

### Parameters {#params}

* `name` — a name of the asynchronous replication instance.
* `SET (option = value [, ...])` — asynchronous replication parameters:

    * `STATE`: the state of asynchronous replication. Valid values are:

        * `PAUSED`: pause the application of changes. Changefeeds on the source continue accumulating records within their retention period. Replica objects already attached remain read-only.
        * `STANDBY`: start or resume change data replication. For replication created with `INITIAL_SCAN = FALSE`, attaches previously restored target tables. Before starting replication, finish importing all tables as described in the [initial synchronization guide](../../../devops/async-replication-initial-sync.md).
        * `DONE`: [complete the asynchronous replication process](../../../concepts/async-replication.md#done). For forced completion, also specify `FAILOVER_MODE = 'FORCE'`.

    * `FAILOVER_MODE`: the replication completion mode. Applies only with `STATE = 'DONE'`. This parameter is not required for `PAUSED` or `STANDBY`. Valid values are:

        * `FORCE` — forced failover.

* Authentication settings for the source database (one of the following):

  {% include [x](_includes/async_replication_authentification.md) %}

## Examples {#examples}

The following statement starts replication after [importing the initial data](../../../devops/async-replication-initial-sync.md#import):

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = 'STANDBY');
```

The following statement pauses the application of changes:

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = 'PAUSED');
```

To resume replication, use `STATE = 'STANDBY'`. The pause and subsequent processing of accumulated changes must fit within the [record retention period](../../../concepts/cdc.md#retention-period).

The following statement forces the asynchronous replication process to complete:

```yql
ALTER ASYNC REPLICATION my_replication SET (STATE = "DONE", FAILOVER_MODE = "FORCE");
```

The following query changes the secret:

```yql
ALTER ASYNC REPLICATION my_replication SET (TOKEN_SECRET_PATH = "my_token");
```

## See also

* [Initial synchronization using backup and restore](../../../devops/async-replication-initial-sync.md)
* [CREATE ASYNC REPLICATION](create-async-replication.md)
* [DROP ASYNC REPLICATION](drop-async-replication.md)
