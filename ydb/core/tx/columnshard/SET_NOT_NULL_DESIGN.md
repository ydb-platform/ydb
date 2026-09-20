# SET NOT NULL for column tables

## Status and scope

This change provides an implementation building block, admission guards, and a
test contract. It does **not** implement the distributed ALTER operation.
Enabling the new flag does not enable `SET NOT NULL` on column tables: the
existing unsupported-operation checks remain in place until the protocol below
is implemented.

Implemented components:

- `EnableColumnStoreSetNotNull`, feature flag 336, defaults to `false`. SQL and
  SchemeShard reject column-table requests with a feature-specific error when
  the flag is disabled. The existing `EnableSetColumnConstraint` flag continues
  to control the shared constraint-operation API.
- `validation/not_null/`: a streaming Arrow-table validator. It checks the
  requested columns across batches, records a sticky failure, and requires an
  explicit successful end of stream before reporting validation success. It
  does not read storage, acquire a write fence, or change a schema.
- Unit tests for the validator and feature-flag admission; an active
  compatibility test for disabled-feature behavior. Enabled-feature
  compatibility tests are explicitly skipped specifications, not evidence that
  the distributed operation works.

The first complete implementation should support standalone writable column
tables. Reject tables inside a column store and read-only copies before creating
an operation. A store's schema preset is shared: changing it for one table can
change sibling tables that have not been validated.

Required user-visible behavior is:

```sql
ALTER TABLE table_name ALTER COLUMN column_name SET NOT NULL;
```

The operation succeeds only if every currently visible row has a non-NULL value
in every requested column. An empty table succeeds. On validation failure the
schema and data remain unchanged and nullable writes become available again.
`DROP NOT NULL` and existing row-table operations retain their behavior.

## Correctness invariants

1. No shard may acknowledge validation before all writes accepted before its
   write fence have either become visible to the scan or been aborted.
2. A NULL-producing write cannot commit between that scan and publication of the
   constraint. Check the current constraint state even when a request carries a
   historical nullable schema version.
3. A successful scan means a successful, complete stream of **visible merged
   rows**. Missing blobs, scan errors, actor termination, timeouts, and incomplete
   streams never mean that the table contains no NULLs.
4. All validation responses belong to the same operation, target schema, fence,
   and shard generation/attempt. A reply from an older attempt cannot complete a
   newer attempt.
5. Rebooting SchemeShard or ColumnShard preserves the fence and resumes or
   cancels the same operation. Cancelling removes the temporary fence only after
   the operation can no longer publish the constraint.

A `SELECT ... WHERE column IS NULL` followed by an ALTER does not satisfy these
invariants: another writer can insert a NULL between the two requests. Checking
only portion metadata also cannot establish the condition: existing portions do
not provide complete NULL-count metadata, and obsolete/deleted row versions must
not cause a false rejection.

## Reuse the SchemeShard operation

The existing row-table state machine already has the right broad phases in
`schemeshard_set_column_constraint__progress.cpp`:

| State | Column-table responsibility |
| --- | --- |
| `Locking` | Acquire a durable schema lock; freeze the participating shard set. |
| `LockingNullWrites` | Install a durable write fence on every participating shard and wait for previously accepted writes. |
| `Validating` | Scan the visible rows on every shard at a snapshot protected by the fence. |
| `Finishing` | On success publish `NotNull=true`; on failure/cancellation keep the nullable schema. Remove the temporary fence as part of the finishing protocol. |
| `Unlocking` | Release the schema lock after every shard has finished. |
| `Done` | Return the existing operation result and retain its diagnostics. |

For a first implementation, a temporary **table-wide write fence** is easier to
make correct than allowing selected writes during validation. Reads continue.
Existing writes must drain; new writes receive a retryable error. A later
optimization can allow writes whose resulting values are provably non-NULL,
including the retained values of partial updates.

Concrete SchemeShard integration:

- `schemeshard_set_column_constraint__create.cpp`: accept row tables or supported
  standalone column tables. For a column table, validate the new feature flag,
  writable/standalone status, column names, duplicate names, and the current
  schema before persisting an operation. Keep row-table behavior unchanged.
- `schemeshard__operation_create_lock.cpp`: remove the row-only path check for
  this supported case. Its current unconditional `Tables.at(pathId)` and
  split-operation dependency loop must remain row-table-only. Drop-lock is
  already based on path/lock ownership rather than row-table metadata.
- Add `CheckLocks` checks to the relevant OLAP ALTER, DROP, MOVE, COPY, and
  resharding entry points. `NotUnderOperation()` is insufficient: it checks path
  state, whereas the long-running constraint operation uses `LockedPaths`.
  `olap/operations/read_only_copy_table.cpp` already checks its source lock and
  provides an existing example.
- `AlterMainTableTemplate` can continue emitting `ESchemeOpAlterTable` with
  `Internal=true` and `LockGuard.OwnerTxId`. The alter-table dispatcher already
  routes column tables to `CreateAlterColumnTable`.
- Extend `olap/operations/alter/abstract/converter.h` and
  `olap/columns/update.{h,cpp}` to carry the temporary constraint state. Reject
  externally supplied temporary state and direct `NotNull=true` transitions
  outside the validated internal flow. Do not remove the existing rejection in
  `TOlapColumnBase::ApplyDiff` until all phases are connected.
- In `InitiateValidationShards`, use
  `ColumnTables.at(pathId)->GetColumnShards()` and `TabletIdToShardIdx` for column
  tables instead of `Tables.at(pathId)->GetPartitions()`.
- Add a ColumnShard validation request/response path while keeping the existing
  DataShard path. Persist terminal shard results with the existing operation;
  retry delivery on pipe failure. A timeout must produce retry or failure, never
  a synthetic successful shard response.

`olap/operations/alter/in_store/schema/update.cpp` currently rejects any
`AlterSchema`. Store support needs a separate design: acquire a store-level
schema lock, freeze all affected tables and shard membership, validate every
table using the preset, and publish one preset version. It must not be obtained
by bypassing that existing rejection.

## Persistent state and wire protocol

Keep the existing `TSetColumnConstraintOperationInfo` states and
`Schema::SetColumnConstraint` / `SetColumnConstraintShardStatus` tables. They
already persist the operation ID, target path, column names, lock transaction,
current subtransaction, cancellation, failure, and terminal shard statuses.

The complete column-table implementation additionally needs a durable fence
identity and a validation snapshot. Use the operation ID plus the schema version
that installed the fence, and persist the chosen snapshot before starting a
scan. Persist the participant set when needed to verify membership after reboot;
schema-lock enforcement must prevent resharding from silently changing it.
On ColumnShard persist the owner transaction/epoch, target column IDs, base schema
version, phase, and proof snapshot with the table metadata. A successful scan
becomes a durable validated proof before the shard reports itself prepared for
publication. Keep the fence until coordinated activation or cancellation.

An additive representation for schema propagation is
`SetNotNullInProgress=false` on `TOlapColumnDescription` and an optional patch
field on `TOlapColumnDiff` in `flat_scheme_op.proto`. Free field numbers at the
time of this design are 16 and 11 respectively. Preserve the field through
SchemeShard serialization, local-database reload, conversion, ColumnShard schema
versions, schema description, and KQP metadata. The final `NotNull` field already
exists and must remain false during validation so readers retain nullable types.

A boolean alone is insufficient for idempotency. The ColumnShard must persist
the owner operation/fence identity alongside the state and reject attempts to
release another operation's fence. Recovery must restore the fence before
accepting writes. Feature-flag changes affect admission of new operations;
turning the flag off must not abandon an existing fence or bypass recovery.

Validation messages must identify at least:

- protocol version/capability, operation ID, owner/table path and tablet ID;
- target column IDs and schema version, fence identity, read snapshot;
- SchemeShard generation and retry round;
- terminal result: validated, NULL found, unsupported, or execution failure.

The response echoes the identity and attempt. Persist a successful shard result
only after the collector receives successful end of stream. Distinguish a NULL
violation from an operational failure in user diagnostics; neither permits
`NotNull=true` publication.

## ColumnShard fence and scan

`columnshard__write.cpp` accepts requests using a schema looked up by the
request's schema version. Merely changing the newest Arrow field to
`nullable=false` does not reject requests carrying an older version. Install the
fence in all write entry/commit paths, including SQL writes, BulkUpsert, long
transactions, buffered blob writes, and writes already prepared before the
schema change. Do not treat a missing column as a safe write without considering
the operation's missing-column/default/partial-update semantics.

After activation, validate the resulting complete rows after partial-update
merge and default materialization, excluding delete-only tombstones. The current
`PrepareForModification` in `engines/scheme/versions/abstract_scheme.cpp` checks
NULLs in selected Insert/Replace/Upsert paths and has different handling for
external defaults. It is not a universal final-row check for Update/Increment.
Rejecting stale schema versions alone does not close this remaining gap for
writes using the current schema.

`transactions/operators/schema.cpp` contains `TWaitTxs` and a transaction-
completion subscriber used by MOVE. It is a starting point for draining work,
not proof that every accepted write is covered: in-flight blob actors and
uncommitted write operations also need an explicit accounting and recovery rule.
Schema proposal currently does not validate `kAlterTable` data.

In particular, inspect these paths when implementing the fence:

- `tablet/write_queue.cpp`, `TWriteTask::Execute`, retains an already accepted
  request's schema and Arrow data;
- `blobs_action/transaction/tx_blobs_written.cpp`, `TTxBlobsWritingFinished`,
  commits immediate/BulkUpsert writes and completes their visibility changes;
- the `CommitWriteLock` branch of the write handler,
  `transactions/operators/ev_write/abstract.h`, and
  `operations/manager.cpp::CommitTransactionOnExecute` finalize transactional
  writes without passing through ordinary write admission again.

Drain already prepared/planned distributed writes to their existing decision;
never abort them unilaterally. Unprepared work may use the normal transaction
abort path, but validation must wait for its finalization. Choose the scan
snapshot only after previously admitted writes have completed their visibility
updates, not merely after they have left an admission queue.

Do not reuse the copy-table `ReadOnly` state or `MoveTablePropose` as the fence:
the former has copy-specific semantics and the latter removes the path mapping
needed by the internal scan. Add explicit temporary state. The existing async
schema-proposal completion path also needs an outcome, not just a completion
notification: `tx_finish_async.cpp`/`FinishProposeOnExecute` must propagate a
validation error without replying PREPARED. After a distributed plan is accepted,
rollback is no longer a valid response; validation and cancellation decisions
must precede that point.

Use the existing internal scan rather than scanning physical chunks directly:

- `columnshard.h`: `TEvColumnShard::TEvInternalScan`;
- `engines/reader/transaction/tx_internal_scan.cpp`: the internal scan forces
  deduplication, selects the trivial reader, accepts projected column IDs, and
  registers its read with `InFlightReadsTracker`;
- `engines/reader/actor/actor.cpp`: normal Arrow scan output and backpressure;
- `validation/not_null/validator.{h,cpp}`: check each projected Arrow table and
  call `Finish()` only after successful terminal scan completion.

The collector must wait until its chosen snapshot is readable. The public KQP
scan handler in `columnshard__scan.cpp` already waits against `GetMaxReadVersion`;
the internal-scan handler does not perform that same wait on entry. Keep the
read snapshot pinned until completion, cancel the child scan on failure, and
acknowledge batches to bound memory consumption.
The internal scan currently does not forward its request's `ItemsLimit` or
`SchemaVersion` into its read description; its scanner constructor uses
`context(snapshot, 0)`. Therefore explicit schema pinning and a `LIMIT 1` NULL
probe are not available merely by filling those request fields. Integrate and
test schema selection before relying on it. Scan acknowledgement windows bound
queued output, not every reader allocation.

Normal reader semantics must handle old schema versions, default values, absent
columns, tiered blobs, deletes, and overwritten rows. Materialize an absent old
column as its logical default/NULL before validation. A column absent from the
collector's projected batch is an invalid/incomplete scan result, not evidence
of non-NULL data. An old NULL row overwritten with a non-NULL value must succeed;
an old non-NULL row overwritten with NULL must fail.

## Upgrade, downgrade, and mixed versions

Adding optional protobuf fields preserves decoding but does not establish
behavioral compatibility. An old ColumnShard may ignore a temporary-state field
and acknowledge an ALTER without installing any fence. Never publish a
constraint based on that acknowledgement alone.

Before enabling admission, require explicit protocol support from every
participant and from the binaries allowed to host those tablets. An unsupported
response or capability timeout aborts safely. A participant capability reply is
not enough if the tablet can subsequently move to an old binary: rolling-upgrade
policy must keep unsupported binaries from hosting fenced tablets. Keep the
feature off throughout a mixed-version rollout until that condition is met.

Downgrading with an operation in progress is unsupported until recovery on the
older binary is implemented and tested. The current row-specific SchemeShard
code assumes `Tables.at(pathId)` when resuming validation; allowing it to resume
a column-table operation can fail. Finish or cancel every operation and verify
all fences have been released before such a downgrade.

After completion the final schema uses the existing `NotNull` representation,
but downgrade compatibility still needs executable evidence. In particular,
historical nullable schema versions and prepared/cached writers did not exist
for a column that was NOT NULL from creation. The compatibility tests must
verify actual NULL rejection after downgrade, including BulkUpsert, rather than
checking only the described schema.

SQL and BulkUpsert downgrade tests alone do not cover a raw write carrying a
historical nullable schema version. Add a direct stale-schema write test on the
older ColumnShard binary. Until that passes, require a supported binary-version
floor or backport the current-schema nullability/minimum-writable-schema-version
check before allowing downgrade, even after the operation has completed. A
minimum writable schema version is useful only when every eligible binary
enforces it; an optional field ignored by an older binary is not such a floor.

## Test contract

Implemented tests belong to three separate levels:

- `validation/not_null/ut/`: chunked Arrow input, requested-column validation,
  NULL detection, completion/failure state, and compatibility of the validator
  with decoded batch representations. These tests do not exercise tablets.
- `KqpOlap::AlterTableSetNotNullOnColumnTableFeatureDisabled`,
  `SetNotNullTest::ColumnTableFeatureDisabled`, and the runtime feature-flag
  default test: disabled admission through SQL and SchemeShard and a false
  default. These do not exercise successful ALTER.
- `ydb/tests/compatibility/olap/test_set_not_null.py`:
  `TestColumnTableSetNotNullDisabled` tests nullable data across restarts with
  different binaries while the feature is off. `TestColumnTableSetNotNull`
  specifies success/downgrade and failed-validation behavior and remains
  skipped until the distributed protocol is implemented.

Before removing the enabled-feature guards, add and run:

1. Successful ALTER on empty and populated tables, one and multiple shards,
   multiple requested columns, missing/duplicate names, and already-NOT-NULL
   columns; verify types through DescribeTable and actual reads.
2. Rejection on NULL in any shard, NULL in the last batch, an old portion missing
   an added column, and nullable defaults; verify unchanged schema/data and
   resumed nullable writes after failure.
3. Deletes/overwrites, compaction, TTL, tiered reads, chunked and encoded input;
   validate current visible rows rather than all historical rows.
4. Every write path with explicit NULL, omitted columns, historical schemas,
   prepared transactions, and buffered writes paused before/after fence
   installation. After success none may commit a NULL.
5. Reboots of SchemeShard and ColumnShard at every phase; duplicate/stale replies,
   pipe loss, scan failure, timeout, cancellation, and disabled flag during
   recovery. Assert that failure never publishes the constraint and that fences
   are not leaked.
6. Concurrent ALTER, DROP, MOVE, COPY, and resharding; reject conflicts while the
   long-running schema lock is held.
7. Old-data/new-binary validation, a mixed-version participant that cannot fence,
   completed-operation downgrade with real NULL writes, and explicit rejection
   or prevention of downgrade while a fence is active.

Only after those paths are connected and their tests pass should
`EnableColumnStoreSetNotNull=true` admit successful column-table operations.
