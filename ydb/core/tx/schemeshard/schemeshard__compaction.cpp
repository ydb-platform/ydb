#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard { 

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCompaction(const TShardIdx& shardId) {
    TabletCounters->Simple()[COUNTER_BACKGROUND_COMPACTION_QUEUE_SIZE].Set(CompactionQueue->Size());
    TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_TIMEOUT].Increment(CompactionQueue->ResetTimeoutCount());

    auto ctx = TActivationContext::ActorContextFor(SelfId());

    auto it = ShardInfos.find(shardId);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info "
            "for background compaction# " << shardId
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RunBackgroundCompaction "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", in queue# " << CompactionQueue->Size() << " shards"
        << ", running# " << CompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        new TEvDataShard::TEvCompactTable(pathId.OwnerId, pathId.LocalPathId));

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::Handle(TEvDataShard::TEvCompactTableResult::TPtr &ev, const TActorContext &ctx) { 
    const auto& record = ev->Get()->Record;

    const TTabletId tabletId(record.GetTabletId());
    const TShardIdx shardIdx = GetShardIdx(tabletId);

    auto pathId = TPathId(
        record.GetPathId().GetOwnerId(),
        record.GetPathId().GetLocalId());

    // it's OK to OnDone InvalidShardIdx
    // note, that we set 0 search height to move shard to the end of queue
    auto duration = CompactionQueue->OnDone(TShardCompactionInfo(shardIdx, 0));

    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished background compaction of unknown shard "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << ", in queue# " << CompactionQueue->Size() << " shards"
            << ", running# " << CompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished background compaction "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << ", in queue# " << CompactionQueue->Size() << " shards"
            << ", running# " << CompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    }

    auto& histCounters = TabletCounters->Percentile();

    switch (record.GetStatus()) {
    case NKikimrTxDataShard::TEvCompactTableResult::OK:
    case NKikimrTxDataShard::TEvCompactTableResult::NOT_NEEDED:
        TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_OK].Increment(1);
        if (duration)
            histCounters[COUNTER_BACKGROUND_COMPACTION_OK_LATENCY].IncrementFor(duration.MilliSeconds());
        break;
    case NKikimrTxDataShard::TEvCompactTableResult::FAILED:
        TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_FAILED].Increment(1);
        break;
    }
}

} // NSchemeShard 
} // NKikimr
