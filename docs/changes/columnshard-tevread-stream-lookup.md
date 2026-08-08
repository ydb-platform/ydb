# Stream Lookup Join over column-store tables: `TEvDataShard::TEvRead` support in ColumnShard

## Summary

Enabled **Stream Lookup Join** (and plain stream lookup) to target **column-store (OLAP)** tables on the right (lookup) side.

Previously the lookup side of a stream lookup join was required to be a row-store (DataShard) table because:

1. the join optimizer explicitly refused to build a lookup join when the right table was OLAP;
2. the data executer treated any stream-lookup input over OLAP as a DML operation and rejected it;
3. **ColumnShard did not implement the DataShard read-iterator protocol** (`TEvDataShard::TEvRead` → `TEvDataShard::TEvReadResult`) that the stream lookup worker uses to fetch rows from a shard.

This change implements the read-iterator point-lookup path in ColumnShard and relaxes the two downstream guards, so a stream lookup can now read a column-store lookup table directly.

## Motivation

The KQP stream lookup / stream lookup join worker issues `TEvDataShard::TEvRead` point lookups (by full primary key) against the shard that owns the lookup table and consumes the resulting `TEvDataShard::TEvReadResult`. Row-store shards already speak this protocol; column-store shards did not, which is why the optimizer disabled the strategy for OLAP right tables. Supporting the protocol in ColumnShard removes that limitation.

## Changes

### 1. New read-iterator handler in ColumnShard

**File:** `ydb/core/tx/columnshard/columnshard__read.cpp` (new)

Adds `TColumnShard::Handle(TEvDataShard::TEvRead::TPtr&, const TActorContext&)` plus a private adapter class `TReadIteratorRestoreTask` and a `SendReadError` helper.

- `TReadIteratorRestoreTask` is a subclass of `NOlap::NDataReader::IRestoreTask`, driven by the existing `NOlap::NDataReader::TActor`. It bridges the DataShard read-iterator protocol to the ColumnShard internal scan engine:
  - **`DoBuildRequestInitiator()`** — builds a `TEvColumnShard::TEvInternalScan` for the resolved path/snapshot. It constructs a point-ranges filter from the requested keys using `NOlap::TRangesBuilder` (each key becomes an inclusive `[key, key]` range), assigns it as a `TPKRangesFilter`, and adds the scan column ids.
  - **`DoOnDataChunk(table)`** — converts each incoming FORMAT_ARROW chunk into FORMAT_CELLVEC using `NArrow::TArrowToYdbConverter`, appending rows into a `TOwnedCellVecBatch` via a small `IRowWriter`. The converter selects/reorders arrow columns **by name**, so building the output schema in the reader's requested order guarantees correct CELLVEC column ordering.
  - **`DoOnFinished()` / `DoOnError()`** — send a single `TEvDataShard::TEvReadResult`.
  - **`SendResult(code)`** — fills the reply record: `ReadId`, `SeqNo = 1`, status code, `ResultFormat = FORMAT_CELLVEC`, the read `Snapshot` (step/txId), `Finished = true`, `RowCount`, and attaches the cell-vec batch on success. Guarded so it is sent at most once.
  - Lifecycle: `IsActive()` returns `!Finished`; `GetTimeout()` returns 60s.

- `TColumnShard::Handle(TEvRead)` validates and dispatches the request:
  - Rejects requests with no `TableId`, non-empty `Ranges` (only point/key reads are supported), no `Keys`, or when there is no primary index — replying with an appropriate error status via `SendReadError`.
  - Resolves the path: `TSchemeShardLocalPathId::FromRawValue(TableId)` → `TablesManager.ResolveInternalPathIdOptional(...)` → `TUnifiedPathId::BuildValid(...)`.
  - Resolves the read snapshot: uses the request snapshot if present, otherwise `GetMaxReadVersion()`, then `TablesManager.ResolveReadSnapshot(...)`.
  - Builds the result schema and scan column ids from `record.GetColumns()` against `indexInfo.GetColumns()` (in requested order). When no columns are requested (e.g. `Count(*)`), falls back to the first PK column so the scan still produces rows.
  - Extracts the primary key description (`GetPrimaryKeyColumns()` / `GetPrimaryKey()`) and copies the requested keys.
  - Registers a `NOlap::NDataReader::TActor` driving the `TReadIteratorRestoreTask`.

**Scope / limitations:** only point lookups by full primary key are supported (exactly what the stream lookup worker issues). Range reads are explicitly rejected as `UNSUPPORTED`.

### 2. Wiring

**File:** `ydb/core/tx/columnshard/ya.make`

- Added `columnshard__read.cpp` to `SRCS`.
- Added `ydb/core/tx/columnshard/data_reader` to `PEERDIR`.

**File:** `ydb/core/tx/columnshard/columnshard_impl.h`

- Declared `void Handle(TEvDataShard::TEvRead::TPtr& ev, const TActorContext& ctx);`.
- Registered `HFunc(TEvDataShard::TEvRead, Handle);` in `StateWork`.

### 3. Relax data-executer OLAP guard

**File:** `ydb/core/kqp/executer_actor/kqp_data_executer.cpp`

In `HasDmlOperationOnOlap()`, removed the branch that returned `true` for `kStreamLookup` inputs. A stream lookup is a read-only point lookup and is now supported over ColumnShard, so it must not be classified as a DML operation on OLAP (which would otherwise fail the transaction).

### 4. Relax join optimizer OLAP guard

**File:** `ydb/core/kqp/opt/logical/kqp_opt_log_join.cpp`

In `KqpJoinToIndexLookupImpl`, the guard that rejected an OLAP right table was changed from:

```cpp
if (rightTableDesc.Metadata->Kind == NYql::EKikimrTableKind::Olap)
```

to:

```cpp
if (rightTableDesc.Metadata->Kind == NYql::EKikimrTableKind::Olap && !useStreamIndexLookupJoin)
```

so an OLAP right table is allowed specifically when the stream index lookup join strategy is in use.

### 5. Tests

**File:** `ydb/core/kqp/ut/stream_lookup/kqp_stream_lookup_ut.cpp`

- Parameterized helper `DoSimpleStreamLookupJoin(bool leftColumn, bool rightColumn)` creating row/column tables per flags. It sets `expectStreamLookup = !(leftColumn && rightColumn)` and asserts both the chosen plan strategy (presence/absence of `StreamLookup` in the AST) and the exact result rows.
- Added three cases:
  - `StreamLookupJoinRightColumnTable(false, true)` — row left, column right → stream lookup (the newly-enabled path).
  - `StreamLookupJoinLeftColumnTable(true, false)` — column left, row right → stream lookup.
  - `StreamLookupJoinBothColumnTables(true, true)` — both column-store.
- Uses `SetEnableKqpDataQueryStreamIdxLookupJoin(true)`, `SetEnableOlapSink(true)`, `SetWithSampleTables(false)`.

## Behavior notes

- **Row left + column right:** now uses a stream lookup that reads the column-store lookup table through the new `TEvRead` handler — the capability that was previously impossible.
- **Both sides column-store:** the optimizer independently keeps the plan in block form and chooses a block-based broadcast `MapJoin` (`KqpBlockReadOlapTableRanges` + `MapJoinCore`) rather than a stream lookup. This is a legitimate, independent OLAP-on-OLAP strategy choice, not a read-iterator limitation; the result rows are identical. The test expectation (`expectStreamLookup = !(leftColumn && rightColumn)`) reflects this.

## Verification

Build and tests via:

```bash
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/stream_lookup -F '*StreamLookupJoin*ColumnTable*'
```

Result: `Total 1 suite: 1 - GOOD`, `Total 3 tests: 3 - GOOD`, `Ok`.

## Possible follow-ups (not implemented)

- Broader regression runs of the full `ydb/core/kqp/ut/stream_lookup` suite and `ydb/core/tx/columnshard` unit tests to confirm no wider impact from the new `TEvRead` handler.
- Multi-column primary keys and multi-row lookup batches.
- Explicit coverage of the zero-columns (`Count(*)`) fallback path.
- Range-read support in the read iterator (currently rejected as `UNSUPPORTED`).
