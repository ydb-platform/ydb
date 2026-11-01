# DataShard freeze and drain

This change implements the DataShard part of database freeze. It provides an
internal `TEvDataShard::TEvDatabaseFreeze` protocol; database-wide coordination,
scheme admission control and a public API are separate changes.

## Caller contract

The caller must prevent new scheme mutations, wait for previously accepted
scheme work and determine the complete participant set, including private index
tables. Freezing one shard does not freeze the database or prevent its owning
SchemeShard from creating other shards.

Requests identify the owning SchemeShard, its domain path, an epoch and a stage.
The epoch and stage are persisted. Stages advance monotonically within an epoch;
stale epochs and skipped stages are rejected. Responses distinguish a pending
stage (`Ready=false`) from a completed stage or an error (`Ready=true` plus
status). A pending stage is polled with the same epoch and stage.

The caller must complete each stage on every participant before advancing:

1. **FENCE_WRITES** rejects new logical writes. Previously accepted prepared
   transactions may complete their existing commit/abort protocol. Completion
   waits for outstanding write transactions and local commits. Readsets and
   volatile transaction resolution remain operational.
2. **DRAIN_INDEXES** continues accepting asynchronous index change exchange
   while outgoing index queues drain. Initial CDC scans and their schema
   finalization must complete. Queue removal follows the destination's committed
   apply acknowledgement.
3. **DRAIN_CDC** closes incoming change exchange and stops periodic resolved
   records. Streams configured with resolved timestamps emit a final resolved
   record. Completion requires an empty change queue, no removal transaction in
   flight and no committed lock change records. CDC records are removed after
   the topic writer's acknowledgement; broadcast resolved records require
   acknowledgements from every participating topic partition.

After a sender restart, writer initialization can also confirm records already
persisted in the topic through its durable source sequence number. The original
writer acknowledgement is then unnecessary. Undelivered records still require
replay and confirmation before drain can complete.

The final resolved version advances the persisted immediate write edge. Both
immediate and planned writes after thaw respect that edge.

Unconfirmed removal, such as discarding a dropped sender's records, produces
`PRECONDITION_FAILED`. This failure remains sticky across retries and restarts
until explicit thaw. Errors do not implicitly reopen the shard.

**OPEN** with a newer epoch removes the database fence. It preserves an
independent legacy per-table freeze and resumes periodic CDC heartbeats.
Late commands from an older freeze cannot restore the fence.

## Scope

The fence covers DataShard logical writes: query/write transactions, direct
uploads and erases, TTL, distributed erase, CommitWrites, replication apply,
incoming change exchange, S3 row upload and direct part attachment, index build
uploads and writing scheme operations. New background index scans are rejected;
existing scans cannot write through fenced destinations. Reads and physical
maintenance that preserves logical rows can continue.

Successful drain confirms delivery of collected changes to source CDC topics.
It does not confirm application by a replication consumer, recreate historical
changes omitted by restore or disabled change collection, or expose a common
database-wide MVCC boundary. Other tablet types are outside this protocol.

## Tests

`DataShardDatabaseFreeze` checks write admission, prepared and volatile
transactions, delayed proposals, direct writes, TTL, replication, index scans,
scheme writes, restart, epochs and compatibility with per-table freeze.

Its CDC tests exercise the internal protocol against real topic writers:
delayed data acknowledgements, final resolved acknowledgements from multiple
topic partitions, distributed writes after thaw, pending drain across restart,
recovery through durable topic sequence numbers, replay of undelivered records,
sticky delivery loss, superseding thaw and initial scan finalization.

`DatabaseFreezeDrainsAsyncIndexBeforeClosingItsInput` checks index propagation
before closing incoming change exchange. The direct restore test
`DatabaseFreezeRejectsAlreadyDownloadedPartAttachment` checks that a part
downloaded before the fence cannot be attached afterward.
