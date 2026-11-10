# CdcAggregator

Resolved timestamps of a CDC stream are published by one tablet per stream
instead of every DataShard sending heartbeats to every topic partition.
For S DataShards and P partitions a round of equal heartbeats costs S reports
and P publications instead of S×P deliveries. Row changes and schema changes
keep going directly from DataShards to PersQueue.

The barrier mode is chosen when a stream is created
(`TCdcStreamDescription.BarrierMode`) and never changes. Legacy streams keep the
old protocol; the first version covers new streams with resolved timestamps and
a fixed set of topic partitions.

## Components

```
SchemeShard ── Configure/Activate ──▶ CdcAggregator ◀── ReportDeliveredBatch ── CdcAggregatorService
     │                                   │      ▲                                   ▲
     │ BarrierMode, AggregatorTabletId   │      └── ReplaceSource (split/merge)     │ ReportDelivered
     ▼                                   │                                          │
 DataShard CDC sender ── rows, schema ───┼──▶ PersQueue partitions ◀────────────────┘
                                         └── CmdPublishCdcBarrier ──▶ (one per partition)
```

| Component | Owns | Durable state |
| --- | --- | --- |
| DataShard CDC sender | Heartbeat ↔ delivered prefix of the source | Heartbeat stays in the change queue until the aggregator persists a covering version |
| CdcAggregatorService | Batching of reports per aggregator, max per source | None: losing it only causes sender retries |
| CdcAggregator | Set of sources, their progress, the ready barrier | LocalDB |
| PersQueue partition | Barrier record in the topic and the last committed barrier | Partition data |
| SchemeShard | Mode, aggregator tablet, initial configuration, lifecycle | Scheme metadata |

## Contracts

Each stage implements one side of a contract and fakes the other side with an
edge actor in tests.

| Boundary | Contract | Used by stages |
| --- | --- | --- |
| SchemeShard → CdcAggregator | `TEvConfigure`, `TEvActivate` (`public/events.h`, `protos/cdc_aggregator.proto`) | 1, 5.1, 5.2 |
| Service → CdcAggregator | `TEvReportDeliveredBatch` | 1, 2 |
| Split/merge → CdcAggregator | `TEvReplaceSource` | 1, 5.1, 5.2 |
| Diagnostics → CdcAggregator | `TEvDescribeStream`: configuration, barrier, sources without progress | 1, 5.1, 5.2 |
| Sender → Service | `TEvReportDelivered`, `MakeCdcAggregatorServiceId` (local events) | 2, 3 |
| CdcAggregator → PersQueue | `TPersQueuePartitionRequest.CmdPublishCdcBarrier` | 4 |
| SchemeShard → DataShard | `TCdcStreamDescription.BarrierMode`, `AggregatorTabletId` | 3, 5.1 |
| SchemeShard → PersQueue | `TPQTabletConfig.CdcBarrierBinding` | 4, 5.1 |
| Rollout | `TFeatureFlags.EnableCdcAggregator` | 5.1 |

Guarantees carried by the messages:

- `Delivered(source, H)`: the source will produce no change below H, and all its
  changes below H are durably written to the topic. The aggregator cannot check
  this; the sender (stage 3) and split/merge (stage 5.1) must ensure it.
- A success from the aggregator or PersQueue means the state is committed.
  Responses and publications are sent from `Complete`, never before commit.
- `PersistedVersion` is the committed progress of the source; it is never lower
  than the reported version. A response for an older batch does not confirm a
  newer version still waiting in the service.
- The barrier is the minimum over all current sources. A source without a
  report blocks it; an empty set never produces a barrier.
- The barrier and per-source progress never decrease. Older reports are
  acknowledged with the higher persisted version instead of being rejected.

## Layout

| Directory | Layer | Depends on |
| --- | --- | --- |
| `public/` | Wire contract | protos |
| `stream/` | Domain aggregate of one stream, repository port, use cases | base, scheme |
| `tablet/` | Tablet transport, LocalDB repository, transactions | `stream/`, `public/`, tablet_flat |

The domain has no actors, LocalDB or protobuf, so its invariants are tested
with plain command sequences. The tablet maps protobuf to typed commands,
runs a use case inside a transaction and replies from `Complete`.

## Corner cases found in the current code

| Case | Source | Handling |
| --- | --- | --- |
| Heartbeat versions are aligned to the interval, but final heartbeats of database freeze are not | `datashard/cdc_stream_heartbeat.cpp`, `datashard_database_freeze.cpp` | Any `TRowVersion` is accepted; no alignment is assumed |
| Altering the resolved interval re-creates heartbeat state and may emit a lower version | `AdjustVersion` with a smaller interval | Lower reports are acknowledged without a change |
| Heartbeats start only when the stream becomes Ready after the initial scan | `datashard_cdc_stream_common.cpp` | Sources without reports block the barrier until then |
| Split children start heartbeats from `TRowVersion::Min()` and jump to the current interval | `datashard_split_dst.cpp` | Children start without progress after `ReplaceSource` |
| Merge: several parents replace themselves with the same child | `change_exchange_split.cpp`, `datashard_change_sender_activation.cpp` | The existing child keeps its progress; each parent is replaced once |
| A late report of a replaced parent | Sender retries | `RETIRED_SOURCE`; the parent never returns to the set |
| A read-only transaction completes after preceding read-write commits | `TLogicRedo::CommitROTransaction` | Duplicates are answered from a transaction, not from the handler |
| RAM state of a later transaction is visible before its commit | Executor runs `Execute` of the next transaction before the previous `Complete` | Responses and the publishable barrier are captured in `Execute` of their own transaction |
| `TEvWriteAccepted` does not mean the write is durable | `datashard/change_sender_cdc_stream.cpp` | Stage 3 reports only after a successful `TEvWriteResponse` |
| `HasPendingRecords` ignores records prepared by partition senders | `change_exchange/change_sender.h` | Stage 3 needs its own delivered-prefix tracking |
| Disabling resolved timestamps stops heartbeats of an aggregated stream | `alter_cdc_stream_unit.cpp` | Stage 5.1 must reject or handle it; the aggregator would wait forever |

## Stages

| Stage | Result | Fakes until integration |
| --- | --- | --- |
| 1 | Tablet: configuration, progress, `ReplaceSource`, ready barrier, recovery | Edge actors send commands |
| 2 | CdcAggregatorService on every DataShard node | Edge actor answers as the tablet |
| 3 | Sender reports the delivered prefix in the aggregated mode | Edge actor registered as the service |
| 4 | PersQueue command and the tablet publisher | Edge actor answers as the tablet or the partition |
| 5.1 | SchemeShard lifecycle: create, configure, activate, split/merge, drop | Fake Hive boots the real tablet |
| 5.2 | Retries, restarts and partial completion of the stage 5.1 operations | — |
