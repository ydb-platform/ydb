# Database freeze

`CmsService.AlterDatabase` accepts an optional `freeze` boolean. Its presence is
significant: `false` requests thaw. Freeze cannot be combined with database
mutation settings. `path`, `generation`, `idempotency_key` and `operation_params`
remain available. `cancel_after` and `forget_after` are unsupported.

An ASYNC request returns a CMS operation ID. Poll it with
`OperationService.GetOperation`. A SYNC request uses the same persisted operation
and waits for completion. A disconnected client can recover the operation using
its ID or repeat the request with the same idempotency key. A different value
with the same key is rejected. A new request supersedes a pending request;
explicit thaw can therefore cancel a drain waiting for an unavailable topic.
Completed operation IDs continue to describe their original request, including
after thaw and Console restart.

## Completion protocol

Console persists the operation and a monotonically increasing epoch. It resolves
the owning SchemeShard, verifying the database incarnation. SchemeShard persists
the schema fence, waits for accepted scheme operations and long jobs, and fixes
the participant set from current row-table partitions, including private index
tables. Retired split sources are excluded: split completion already requires
draining their change queues and activating their successors.

The coordinator advances each phase only after every participant acknowledges:

1. **FENCE_WRITES** rejects new logical writes. Prepared distributed transactions
   already accepted by a shard may finish their existing commit/abort protocol.
   Pending immediate requests may be rejected. Readsets and volatile transaction
   resolution remain operational. Completion waits for outstanding writes and
   local commits.
2. **DRAIN_INDEXES** continues accepting asynchronous index change exchange while
   all source queues drain. Initial CDC scans and their schema finalization must
   complete. Source queue removal follows the destination's committed apply ACK.
3. **DRAIN_CDC** closes incoming change exchange, stops periodic resolved records,
   and emits a final resolved record for streams with resolved timestamps
   enabled. The version also advances the immediate write edge so resumed writes
   cannot fall behind it. The plan interval for prepared writes also respects
   that persisted write edge after thaw. Completion requires an empty change queue, no removal
   transaction in flight, and no committed lock change records. The existing CDC
   sender removes a record only after the topic writer ACK; broadcast resolved
   records require ACKs from all participating topic partitions.

Unconfirmed removal (for example a dropped sender) produces
`PRECONDITION_FAILED`, rather than successful drain. The failure remains sticky
while frozen, including across another freeze attempt. Errors do not implicitly
reopen a partially frozen database; use explicit thaw.

Thaw applies a newer OPEN epoch to all participants. The database fence is
independent of the legacy per-table freeze: thaw preserves a manually frozen
table. SchemeShard keeps schema changes fenced until thaw succeeds, including
after a failed thaw. Workers can
be recreated and retry through tablet pipes; persisted epochs reject stale
commands, and completed stages do not move backwards within an epoch. Workers
poll pending participants at one-second intervals.

## Scope

The fence covers DataShard logical writes: query/write transactions, direct
uploads and erases, TTL, distributed erase, CommitWrites, replication apply,
incoming change exchange, S3 row upload and direct part attachment, index build
uploads, and writing scheme operations. Existing background index scans cannot
write through their fenced destinations; new index scans are rejected. Reads
and physical maintenance that preserves logical rows can continue.

ColumnShard databases return `UNSUPPORTED`; this protocol does not freeze column
tables, independent topic producers, or other tablet types. All participants must
support the new protocol. An older Console's immediate AlterDatabase success is
rejected as unsupported by the new RPC handler; an unresponsive participant
cannot yield successful completion.

SUCCESS confirms delivery of collected changes to the source CDC topics. It
does not confirm application by a replication consumer, recreate historical
changes omitted by restore or disabled change collection, or expose a database
wide MVCC boundary. Administrative access that writes tablet storage directly is
outside the logical DataShard protocol.

## Test coverage inventory

This inventory describes added source tests, not measured execution coverage.

| Behavior | Tests |
| --- | --- |
| ASYNC ID / GetOperation, delayed topic ACK, read availability, freeze/thaw and subsequent CDC | `CmsDatabaseFreeze.OperationWaitsForTopicAckAndThawRestoresWrites` |
| SYNC completion after application | `CmsDatabaseFreeze.SyncWaitsForApplication` |
| Resolved delivery to all topic partitions | `CmsDatabaseFreeze.LastResolvedAcknowledgementIsRequired` |
| Initial scan completion | `CmsDatabaseFreeze.InitialScanFinalizationMustComplete` |
| Unconfirmed removal and sticky error | `CmsDatabaseFreeze.DiscardIsNotATopicAcknowledgement` |
| Pending Console / SchemeShard / DataShard recovery | `CmsDatabaseFreeze.PendingOperationSurvivesConsoleSchemeShardAndDataShardRestarts` |
| Cancellation and late ACK | `CmsDatabaseFreeze.ThawCancelsPendingDrainAndIgnoresLateAcknowledgements` |
| Idempotency and historical operation lookup after restart | `CmsDatabaseFreeze.IdempotencyAndOperationHistorySurviveConsoleRestart` |
| Exclusive settings, SDK false presence, empty database, legacy Alter | `CmsDatabaseFreeze.FreezeIsExclusiveIncludingFalse`, `SdkPreservesFreezePresence`, `EmptyDatabaseAndLegacyAlter` |
| Schema fence, unsupported column tables | `CmsDatabaseFreeze.SchemaChangesAreBlockedUntilThaw`, `UnsupportedColumnTableDoesNotFreezeRowTables` |
| Thaw failure keeps the schema fence; successful retry reopens it | `CmsDatabaseFreeze.FailedThawKeepsSchemaFencedUntilSuccessfulRetry` |
| Authorization | Existing Console authorization cases, extended with freeze and thaw |
| Immediate/prepared writes, reads, thaw | `DataShardDatabaseFreeze.FreezeReadWriteThaw` |
| Uncommitted lock cannot commit while frozen | `DataShardDatabaseFreeze.UncommittedLockCannotBeCommittedWhileFrozen` |
| Prepared and volatile commit drain | Both variants of `DataShardDatabaseFreeze.PreparedTransactionFinishesBeforeFreeze` |
| Delayed proposal queue | `DataShardDatabaseFreeze.WriteAlreadyInProposeQueueIsRejected` |
| Direct upload/erase, S3 rows, TTL, replication apply | `DataShardDatabaseFreeze.DirectUploadAndEraseAreFenced`, `S3UploadsAreFenced`, `TtlRequestsAreFenced`, `ReplicationApplyIsFenced` |
| CommitWrites and distributed erase | `DataShardDatabaseFreeze.CommitWritesAndDistributedEraseAreFenced` |
| Twenty scheme-operation protocols checked directly at the shard | `DataShardDatabaseFreeze.SchemeWriteProtocolsAreFencedAtTheShard` |
| Unsafe truncate rejection, then successful truncate after thaw | `DataShardDatabaseFreeze.UnsafeTruncateIsFenced` |
| All ten index scan entry protocols | `DataShardDatabaseFreeze.BackgroundIndexScansAreFenced` |
| Downloaded S3 part cannot attach after fencing | `DatabaseFreezeRejectsAlreadyDownloadedPartAttachment` in direct restore tests |
| Async index propagation drains before sink sealing | `DatabaseFreezeDrainsAsyncIndexBeforeClosingItsInput` in change exchange tests |
| Persisted fence / stale epochs / skipped stages / legacy freeze | `DataShardDatabaseFreeze.RestartAndStaleEpoch`, `CannotSkipFenceStage`, `DatabaseThawPreservesManualTableFreeze` |

Compilation, test execution, sanitizers and quantitative coverage must be checked
separately. Additional failure-injection coverage can exercise split/merge already
in flight, multiple simultaneous schema jobs, and partition topology changes
during CDC delivery.
