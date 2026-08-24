# ColumnShard — agent guide

ColumnShard is the YDB tablet that stores **OLAP (column-oriented) tables**. It is a
`TTabletExecutedFlat` actor that ingests writes, persists data as immutable
**portions** of columnar blobs, runs background compaction / TTL / GC, and serves
scan reads to KQP.

Owner team: `@ydb-platform/cs` (see [`.github/TESTOWNERS`](../../../../.github/TESTOWNERS)).
Root repo rules: [`AGENTS.md`](../../../../AGENTS.md).

## Build & test

Run from the repo root. Tests include the build; do not pass `-j` and do not force rebuild.

```bash
# Build the tablet library
./ya make --build relwithdebinfo ydb/core/tx/columnshard

# Run a unit-test suite (read/write path)
./ya make --build relwithdebinfo -tA ydb/core/tx/columnshard/ut_rw 2>&1 | tail

# Run a single test by filter
./ya make --build relwithdebinfo -tA ydb/core/tx/columnshard/ut_rw -F '*TestWriteRead*' 2>&1 | tail

# Re-run to catch flakes
./ya make --build relwithdebinfo -tA ydb/core/tx/columnshard/ut_schema -F '*Ttl*' --test-retries 5 2>&1 | tail

# Run every OLAP/ColumnShard suite owned by @ydb-platform/cs (excluding keyvalue)
./ya make --build relwithdebinfo -tA \
    ydb/core/tx/columnshard \
    ydb/core/kqp/ut/olap \
    ydb/core/tx/schemeshard/olap \
    ydb/core/tx/schemeshard/ut_olap \
    ydb/core/tx/schemeshard/ut_olap_reboots \
    ydb/tests/olap \
    ydb/tests/functional/tpc \
    ydb/tests/compatibility/olap \
    ydb/tests/stress/olap_workload 2>&1 | tail
```

C++20 or earlier only.

## Entry points

Start reading here — these are the widest, most central functions:

* [`columnshard.cpp`](columnshard.cpp) — actor bootstrap and top-level `Handle(...)` event dispatch.
* [`columnshard_impl.h`](columnshard_impl.h) / [`columnshard_impl.cpp`](columnshard_impl.cpp) — `TColumnShard` tablet class: all event handlers, background activity, schema transactions.
* [`columnshard__write.cpp`](columnshard__write.cpp), [`columnshard__scan.cpp`](columnshard__scan.cpp), [`columnshard__init.cpp`](columnshard__init.cpp) — the write, scan and init transaction flows (`TTx*` classes).
* [`columnshard.h`](columnshard.h) — public events (`TEvColumnShard`) and status conversion.
* [`ya.make`](ya.make) — sources, `PEERDIR` dependencies and `RECURSE_FOR_TESTS`.

## Code map

| Area | Path | What it does |
|------|------|--------------|
| Tablet core | [`columnshard*.cpp/.h`](columnshard_impl.h) | Actor, event dispatch, tablet lifecycle. |
| Transactions | [`transactions/`](transactions) | `TTxController`, per-op operators (write/schema/backup/sharing). |
| Storage engine | [`engines/`](engines) | `column_engine_logs` — portion catalog, versions, snapshots. |
| Portions | [`engines/portions/`](engines/portions) | Immutable columnar data unit (`TPortionInfo`, accessors, constructors). |
| Readers | [`engines/reader/`](engines/reader) | `plain_reader` / `simple_reader` / `trivial_reader` scan pipelines; duplicate resolution. |
| Changes | [`engines/changes/`](engines/changes) | Compaction, TTL/eviction, append — background data mutations. |
| Scheme | [`engines/scheme/`](engines/scheme) | `TIndexInfo`, column features, schema versions/diffs. |
| Blobs | [`blobs_action/`](blobs_action), [`blob_cache.*`](blob_cache.h) | Blob read/write/GC over BlobStorage and tiers (S3). |
| Data accessors | [`data_accessor/`](data_accessor) | In-memory / local-db portion metadata access. |
| Tables | [`tables_manager.*`](tables_manager.h) | Path→table registry, loaded from local DB. |
| Normalizers | [`normalizer/`](normalizer) | One-shot migrations that repair persisted state on startup. |
| Counters | [`counters/`](counters) | Monitoring / sensors. |
| Test hooks | [`hooks/`](hooks) | `ICSController` fault-injection interface used by unit tests. |

## Tests

Owned by `@ydb-platform/cs`. Unit tests live next to the code; broader tests live elsewhere in the repo.

In-module unit tests (see `RECURSE_FOR_TESTS` in [`ya.make`](ya.make)):

* [`ut_rw/`](ut_rw) — read/write, compaction, GC, normalizers, backup.
* [`ut_schema/`](ut_schema) — schema changes, TTL, tiers, copy-table.
* [`engines/ut/`](engines/ut) — column engine, predicate ranges, portion validation, snapshots.
* [`engines/reader/trivial_reader/duplicates/ut/`](engines/reader/trivial_reader/duplicates/ut) — duplicate-resolution manager/filters.
* [`backup/`](backup), [`data_accessor/`](data_accessor), [`export/`](export) — subsystem tests.

Related suites elsewhere owned by `@ydb-platform/cs` (from [`.github/TESTOWNERS`](../../../../.github/TESTOWNERS)):

* [`ydb/core/kqp/ut/olap`](../../../kqp/ut/olap) — end-to-end OLAP query tests.
* [`ydb/core/tx/schemeshard/olap`](../../schemeshard/olap) — schemeshard OLAP schema logic.
* [`ydb/core/tx/schemeshard/ut_olap`](../../schemeshard/ut_olap) — schemeshard OLAP unit tests.
* [`ydb/core/tx/schemeshard/ut_olap_reboots`](../../schemeshard/ut_olap_reboots) — schemeshard OLAP reboot/recovery tests.
* [`ydb/tests/olap`](../../../../tests/olap) — OLAP functional tests.
* [`ydb/tests/functional/tpc`](../../../../tests/functional/tpc) — TPC-* benchmark functional tests.
* [`ydb/tests/compatibility/olap`](../../../../tests/compatibility/olap) — cross-version compatibility tests.
* [`ydb/tests/stress/olap_workload`](../../../../tests/stress/olap_workload) — OLAP stress workload.

Tests use `ICSController` ([`hooks/abstract/abstract.h`](hooks/abstract/abstract.h)) to intercept background events (compaction, GC, indexation) deterministically.

## Conventions & invariants — do not break

* **Persisted schema is append-only.** [`columnshard_schema.h`](columnshard_schema.h) defines local-DB tables; changing column meaning or removing tables breaks existing tablets. Add a **normalizer** in [`normalizer/`](normalizer) to migrate instead.
* **Portions are immutable.** Never mutate a written portion; produce a new one via a `changes/` operation and swap atomically under a snapshot.
* **Reads are snapshot-isolated.** Respect `TSnapshot` (plan step + tx id) ordering; do not expose data from an uncommitted snapshot.
* **State changes go through `TTx*` transactions** with the `Execute` (mutate local DB) / `Complete` (side effects, send events) split. Keep `Execute` deterministic and side-effect-free.
* **Enums used in serialization** need `GENERATE_ENUM_SERIALIZATION` in [`ya.make`](ya.make).
* Follow the module [`.clang-format`](.clang-format).

## Namespaces

`NKikimr::NColumnShard` — tablet/actor layer. `NKikimr::NOlap` — storage engine, portions, readers, changes.
