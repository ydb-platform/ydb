# SchemeShard database freeze coordination

SchemeShard exposes an internal `TEvSchemeShard::TEvDatabaseFreeze` request for a
domain path, an epoch and the requested freeze state. The durable DataShard
protocol is described in [the DataShard document](../datashard/database_freeze.md).
Public CMS operation management is a separate layer.

## Schema admission and participants

A freeze request persists a schema fence before asking DataShards to drain.
New schema mutations, split/merge, index builds, imports and column-constraint
operations in the domain are rejected. New TTL runs are paused. Internal
continuations of already accepted schema work may finish before participants
are fixed. Initial CDC scan finalization remains allowed throughout drain.

The coordinator waits for accepted scheme transactions and long-running index,
import, constraint and backup/restore work. It then persists the current row-table
partitions, including private index tables. Retired split sources are excluded;
accepted split/merge operations must complete before participant selection.
Column tables are rejected as unsupported before closing any DataShard write
paths. Domains sharing a SchemeShard have independent fences and participant
sets. A tenant SchemeShard uses its own local domain path when addressing shards.

## Completion and recovery

A disposable worker drives the persisted participant set through global phases:

1. `FENCE_WRITES` on every participant.
2. `DRAIN_INDEXES` on every participant.
3. `DRAIN_CDC` on every participant.

The next phase starts only after all participants acknowledge the previous one.
A pending response has `Ready=false`; completion requires `Ready=true` and a
successful status. Errors finish the request without implicitly reopening the
domain. The caller polls the same epoch and requested state until completion.
Workers retry pending shards through tablet pipes at one-second intervals.

After a SchemeShard restart, a repeated request resumes waiting for schema work
or recreates the worker. It can replay completed phases safely: DataShard stages
do not move backwards. The persistent schema fence also covers the interval
before the caller reconnects. Completed results and failures survive restart.
Older epochs and conflicting states within one epoch are rejected. Replies from
a replaced worker cannot complete a newer epoch.

Thaw uses a newer epoch and sends `OPEN` to every participant. Schema admission
remains closed until every shard acknowledges. A failed thaw keeps the schema
fence; a successful retry with a newer epoch reopens the domain. Per-table freeze
settings remain independent.

## Validation

`ut_database_freeze` exercises real SchemeShard and DataShard tablets. Its tests
cover empty domains, epochs, all-table writes and reads, global phase barriers,
private indexes, accepted table creation, running index builds and split with
undelivered CDC records, TTL pause and resumption, shard
and coordinator restarts, delayed replies, pending and failed thaw, rejected
imports and constraints, shared and tenant SchemeShards, and unsupported column
tables. Real topic writer ACKs and initial CDC scan finalization are also tested.
