# ColumnShard

ColumnShard is the YDB tablet that stores **OLAP (column-oriented) tables**. Where
DataShard serves row-oriented OLTP tables, ColumnShard is optimized for analytical
workloads: large scans, aggregations and column projections over append-mostly data.

It is implemented as a `TTabletExecutedFlat` actor. It accepts bulk/streaming writes,
persists data as immutable **portions** of columnar blobs, maintains schema versions
and snapshots, runs background compaction / TTL / garbage collection, and serves scan
reads to the KQP query engine.

Owner team: `@ydb-platform/cs`.

## What it does

* **Ingest.** Incoming writes are buffered and flushed into columnar blobs. Data is
  written to BlobStorage (and optionally tiered to S3-compatible storage).
* **Organize.** Data is stored as **portions** — immutable, self-describing units of
  columnar data plus metadata, grouped per table (path) and versioned by snapshot.
* **Maintain.** Background operations compact small portions into larger ones, apply
  TTL / eviction to tiers, and garbage-collect blobs that are no longer referenced.
* **Serve reads.** Scan requests from KQP are executed through a reader pipeline that
  filters by predicate ranges, resolves duplicates across portions, and streams
  columnar batches back.

## Key concepts

* **Portion** — the immutable atomic unit of stored data. Once written it is never
  mutated; changes produce new portions that atomically replace old ones under a new
  snapshot.
* **Snapshot** — `(plan step, tx id)` version stamp. Reads are snapshot-isolated;
  writes and background changes become visible only after their snapshot is committed.
* **Column engine** — the in-memory catalog of portions, schema versions and snapshots
  (`engines/column_engine_logs`).
* **Transactions (`TTx*`)** — every state change runs as a tablet transaction with an
  `Execute` phase (mutates the local database deterministically) and a `Complete` phase
  (performs side effects and sends events).
* **Normalizers** — one-shot migrations that repair or upgrade persisted state on
  tablet startup, so the append-only local-DB schema can evolve safely.

## Architecture overview

```
        write / scan events
                |
                v
       +--------------------+        background: compaction, TTL, GC
       |    TColumnShard    |<--------------------------------------+
       | (tablet actor)     |                                       |
       +---------+----------+                                       |
                 | TTx* transactions                                |
                 v                                                  |
        +-------------------+      portions       +-----------------+------+
        |   Column engine   |-------------------->|  Changes (compaction,  |
        | (portion catalog, |                     |  TTL, append)          |
        |  schema, snapshot) |                    +------------------------+
        +---------+---------+
                  | portions / blobs
                  v
        +-------------------+        +---------------------------+
        |  Blob operations  |------->| BlobStorage / S3 tiers    |
        |  + blob cache     |        +---------------------------+
        +-------------------+

        Reads: KQP scan ---> reader pipeline (predicate ranges,
                             duplicate resolution) ---> columnar batches
```

## Source layout

| Path | Contents |
|------|----------|
| `columnshard*.cpp/.h` | Tablet actor, event dispatch, lifecycle, write/scan/init transaction flows. |
| `columnshard_schema.h` | Local-DB (persistent) schema definitions. |
| `transactions/` | Transaction controller and per-operation operators. |
| `engines/` | Storage engine: column catalog, portions, readers, changes, scheme. |
| `engines/portions/` | Portion representation, accessors and constructors. |
| `engines/reader/` | Scan reader pipelines and duplicate resolution. |
| `engines/changes/` | Compaction, TTL/eviction and append operations. |
| `engines/scheme/` | Index/schema info, column features, schema versioning. |
| `blobs_action/`, `blob_cache.*` | Blob read/write/GC over BlobStorage and tiers. |
| `data_accessor/` | Portion-metadata access backends. |
| `tables_manager.*` | Path→table registry loaded from the local DB. |
| `normalizer/` | Startup migrations for persisted state. |
| `counters/` | Monitoring sensors. |
| `hooks/` | `ICSController` fault-injection interface for tests. |

## Build & test

Run from the repository root. Tests include the build.

```bash
# Build
./ya make --build relwithdebinfo ydb/core/tx/columnshard

# Run a unit-test suite
./ya make --build relwithdebinfo -tA ydb/core/tx/columnshard/ut_rw 2>&1 | tail

# Run a single test
./ya make --build relwithdebinfo -tA ydb/core/tx/columnshard/ut_schema -F '*Ttl*' 2>&1 | tail
```

Unit tests live next to the code (`ut_rw/`, `ut_schema/`, `engines/ut/`, …). End-to-end
OLAP query tests live in [`ydb/core/kqp/ut/olap`](../../../kqp/ut/olap); functional,
stress and compatibility suites live under [`ydb/tests`](../../../../tests).

## For AI agents

See [`AGENTS.md`](AGENTS.md) for the code map, entry points, test commands and the
invariants that must not be broken.
