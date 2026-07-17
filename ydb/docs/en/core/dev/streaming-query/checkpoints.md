# Checkpoints

A checkpoint is a saved state of a running [streaming query](../../concepts/streaming-query/streaming-query.md) required for recovery after processing failures. {{ ydb-short-name }} periodically saves checkpoints of all running streaming queries.

## Checkpoint contents {#contents}

A checkpoint contains:

- [offsets](../../concepts/datamodel/topic.md#consumer-offset) in input topics — positions up to which events have been read and processed;
- aggregation states — intermediate results of operations, for example accumulated values in [GROUP BY HOP](../../yql/reference/syntax/select/group-by.md#group-by-hop).

{{ ydb-short-name }} stores read offsets in its own checkpoints rather than relying on [consumer](../../concepts/datamodel/topic.md#consumer) offsets in an external system. This means that when a query is deleted ([DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md)), the offsets are deleted together with the checkpoint — the external system does not know how far the query has read the topic.

## Recovery after a failure {#recovery}

In case of a processing failure (compute node restart, network outage, timeout), the query automatically restarts and restores the state from the last checkpoint: it resumes reading from the saved offsets and restores aggregation states.


```mermaid
sequenceDiagram
    participant Топик
    participant Запрос as Запрос<br/>GROUP BY HOP (1 мин)
    participant Приемник

    Note over Запрос: Чекпоинт сохранён<br/>смещение = 2, sum = 10
    Топик->>Запрос: value = 3 (смещение 3)
    Note over Запрос: sum = 13
    Топик->>Запрос: value = 7 (смещение 4)
    Note over Запрос: sum = 20
    Запрос-xЗапрос: Сбой обработки
    Note over Запрос: Восстановление из чекпоинта<br/>смещение = 2, sum = 10
    Топик->>Запрос: value = 3 (повторно)
    Note over Запрос: sum = 13
    Топик->>Запрос: value = 7 (повторно)
    Note over Запрос: sum = 20
    Note over Запрос: Окно закрыто
    Запрос->>Приемник: sum = 20
```


Events that arrived between the last checkpoint and the failure will be reprocessed. This provides the [at-least-once](../../dev/streaming-query/guarantees.md#at-least-once) guarantee — each event will be processed at least once.

Saving and selecting a checkpoint for recovery happens automatically. Old checkpoints are deleted after a new one is successfully saved.

## Checkpoint deletion when recreating a query {#drop-checkpoint}

When a query is deleted ( [DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md)), the checkpoint is deleted along with it. Since offsets are only stored in the checkpoint, a new query ([CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md)) has no saved position and starts reading from the end of the topic. Any events that arrived in the topic between the deletion of the old query and the start of the new one will not be read.


```mermaid
sequenceDiagram
    participant Топик
    participant Запрос v1
    participant Запрос v2

    Топик->>Запрос v1: События A..D
    Note over Запрос v1: Чекпоинт: смещение = 4
    Note over Запрос v1: DROP STREAMING QUERY<br/>(чекпоинт удалён)
    Note over Топик: События E, F поступают в топик
    Note over Запрос v2: CREATE STREAMING QUERY<br/>(старт с конца топика)
    Топик--xЗапрос v2: E, F (не прочитаны)
    Топик->>Запрос v2: G (новое)
```


A similar situation occurs if the data pointed to by the offset in the checkpoint has already been removed from the topic due to [TTL](../../concepts/datamodel/topic.md#message-retention).

For more details on how this behavior affects delivery guarantees, see the [{#T}](guarantees.md#incomplete-windows-restart) section.

## Disabling checkpoints {#disable}

To reduce overhead, you can disable checkpoint saving using the `ydb.DisableCheckpoints` pragma.

{% note warning %}

When checkpoints are disabled, there are no data consistency guarantees during user-initiated or internal query restarts. Use only for debugging.

{% endnote %}


```sql
CREATE STREAMING QUERY query_without_checkpoints AS
DO BEGIN

PRAGMA ydb.DisableCheckpoints = "TRUE";

INSERT INTO
    output_topic
SELECT
    *
FROM
    input_topic;

END DO
```


## See also

- [{#T}](guarantees.md) — data delivery guarantees and observed anomalies.
- [{#T}](../../concepts/streaming-query/streaming-query.md) — general description of streaming queries.
