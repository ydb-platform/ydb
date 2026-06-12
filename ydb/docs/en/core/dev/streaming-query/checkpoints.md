# Checkpoints

A **checkpoint** is persisted state of a running [streaming query](../../concepts/streaming-query.md), used to recover processing after failures. {{ ydb-short-name }} periodically checkpoints all running streaming queries.

## Checkpoint contents {#contents}

A checkpoint contains:

- [Offsets](../../concepts/datamodel/topic.md#consumer-offset) in input topics — positions up to which events were read and processed;
- Aggregation state — intermediate results such as accumulators for [GROUP BY HOP](../../yql/reference/syntax/select/group-by.md#group-by-hop).

{{ ydb-short-name }} stores read offsets in its own checkpoints and does not rely on external [consumer](../../concepts/datamodel/topic.md#consumer) offsets. When a query is removed ([DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md)), offsets are removed with the checkpoint — external systems are not aware how far the query read in the topic.

## Recovery after failure {#recovery}

When processing fails (compute node restart, network interruption, timeout), the query restarts automatically and restores state from the latest checkpoint: it resumes reading from saved offsets and restores aggregation state.

```mermaid
sequenceDiagram
    participant Topic
    participant Query as Query<br/>GROUP BY HOP (1 min)
    participant Sink

    Note over Query: Checkpoint saved<br/>offset = 2, sum = 10
    Topic->>Query: value = 3 (offset 3)
    Note over Query: sum = 13
    Topic->>Query: value = 7 (offset 4)
    Note over Query: sum = 20
    Query-xQuery: Processing failure
    Note over Query: Recover from checkpoint<br/>offset = 2, sum = 10
    Topic->>Query: value = 3 (again)
    Note over Query: sum = 13
    Topic->>Query: value = 7 (again)
    Note over Query: sum = 20
    Note over Query: Window closed
    Query->>Sink: sum = 20
```

Events that arrived between the last checkpoint and the failure are processed again. That provides [at-least-once](guarantees.md#at-least-once) delivery — each event is processed at least once.

Saving and selecting checkpoints for recovery is automatic. Old checkpoints are removed after a new one is saved successfully.

## Checkpoint deleted when recreating a query {#drop-checkpoint}

When you delete a query ([DROP STREAMING QUERY](../../yql/reference/syntax/drop-streaming-query.md)), its checkpoint is deleted with it. Because offsets live only in the checkpoint, a new query ([CREATE STREAMING QUERY](../../yql/reference/syntax/create-streaming-query.md)) has no saved position and starts reading from the end of the topic. Events that arrived between deleting the old query and starting the new one are not read.

```mermaid
sequenceDiagram
    participant Topic
    participant Query v1
    participant Query v2

    Topic->>Query v1: Events A..D
    Note over Query v1: Checkpoint: offset = 4
    Note over Query v1: DROP STREAMING QUERY<br/>(checkpoint removed)
    Note over Topic: Events E, F arrive
    Note over Query v2: CREATE STREAMING QUERY<br/>(start at end of topic)
    Topic--xQuery v2: E, F (not read)
    Topic->>Query v2: G (new)
```

The same happens if data referenced by an offset in the checkpoint has already been removed from the topic due to [TTL](../../concepts/datamodel/topic.md#retention-time).

For how this affects delivery guarantees, see [{#T}](guarantees.md#incomplete-windows-restart).

## Disabling checkpoints {#disable}

To reduce overhead, you can disable checkpointing with pragma `ydb.DisableCheckpoints`.

{% note warning %}

With checkpoints disabled there are no consistency guarantees across user or internal restarts. Use only for debugging.

{% endnote %}

```sql
CREATE STREAMING QUERY query_without_checkpoints AS
DO BEGIN

PRAGMA ydb.DisableCheckpoints = "TRUE";

INSERT INTO
    ydb_source.output_topic
SELECT
    *
FROM
    ydb_source.input_topic;

END DO
```

## See also

- [{#T}](guarantees.md) — delivery guarantees and anomalies.
- [{#T}](../../concepts/streaming-query.md) — streaming queries overview.
