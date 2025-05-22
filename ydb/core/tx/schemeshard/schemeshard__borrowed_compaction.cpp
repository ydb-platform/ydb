#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBorrowedCompaction(const TShardIdx& shardIdx) {
    UpdateBorrowedCompactionQueueMetrics();

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info "
            "for borrowed compaction# " << shardIdx
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RunBorrowedCompaction "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << BorrowedCompactionQueue->GetWakeupDelta()
        << ", rate# " << BorrowedCompactionQueue->GetRate()
        << ", in queue# " << BorrowedCompactionQueue->Size() << " shards"
        << ", running# " << BorrowedCompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    std::unique_ptr<TEvDataShard::TEvCompactBorrowed> request(
        new TEvDataShard::TEvCompactBorrowed(pathId.OwnerId, pathId.LocalPathId));

    RunningBorrowedCompactions[shardIdx] = PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnBorrowedCompactionTimeout(const TShardIdx& shardIdx) {
    UpdateBorrowedCompactionQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_BORROWED_COMPACTION_TIMEOUT].Increment(1);

    RunningBorrowedCompactions.erase(shardIdx);

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info "
            "for timeout borrowed compaction# " << shardIdx
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Borrowed compaction timeout "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << BorrowedCompactionQueue->GetWakeupDelta()
        << ", in queue# " << BorrowedCompactionQueue->Size() << " shards"
        << ", running# " << BorrowedCompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    // retry
    EnqueueBorrowedCompaction(shardIdx);
}

void TSchemeShard::BorrowedCompactionHandleDisconnect(TTabletId tabletId, const TActorId& clientId) {
    auto tabletIt = TabletIdToShardIdx.find(tabletId);
    if (tabletIt == TabletIdToShardIdx.end())
        return; // just sanity check
    const auto& shardIdx = tabletIt->second;

    auto it = RunningBorrowedCompactions.find(shardIdx);
    if (it == RunningBorrowedCompactions.end())
        return;

    if (it->second != clientId)
        return;

    RunningBorrowedCompactions.erase(it);

    // disconnected from node we have requested borrowed compaction. We just resend request, because it
    // is safe: if first request is executing or has been already executed, then second request will be ignored.

    StartBorrowedCompaction(shardIdx);
}

void TSchemeShard::EnqueueBorrowedCompaction(const TShardIdx& shardIdx) {
    if (!BorrowedCompactionQueue)
        return;

    auto ctx = ActorContext();

    if (BorrowedCompactionQueue->Enqueue(shardIdx)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Borrowed compaction enqueued shard# " << shardIdx << " at schemeshard " << TabletID());
        UpdateBorrowedCompactionQueueMetrics();
    }
}

void TSchemeShard::RemoveBorrowedCompaction(const TShardIdx& shardIdx) {
    if (!BorrowedCompactionQueue)
        return;

    RunningBorrowedCompactions.erase(shardIdx);
    BorrowedCompactionQueue->Remove(shardIdx);
    UpdateBorrowedCompactionQueueMetrics();
}

void TSchemeShard::UpdateBorrowedCompactionQueueMetrics() {
    if (!BorrowedCompactionQueue)
        return;

    TabletCounters->Simple()[COUNTER_BORROWED_COMPACTION_QUEUE_SIZE].Set(BorrowedCompactionQueue->Size());
    TabletCounters->Simple()[COUNTER_BORROWED_COMPACTION_QUEUE_RUNNING].Set(BorrowedCompactionQueue->RunningSize());
}

void TSchemeShard::Handle(TEvDataShard::TEvCompactBorrowedResult::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;

    const TTabletId tabletId(record.GetTabletId());
    const TShardIdx shardIdx = GetShardIdx(tabletId);

    auto pathId = TPathId(
        record.GetPathId().GetOwnerId(),
        record.GetPathId().GetLocalId());

    auto duration = BorrowedCompactionQueue->OnDone(shardIdx);

    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished borrowed compaction of unknown shard "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << " in# " << duration.MilliSeconds()
            << ", next wakeup# " << BorrowedCompactionQueue->GetWakeupDelta()
            << ", rate# " << BorrowedCompactionQueue->GetRate()
            << ", in queue# " << BorrowedCompactionQueue->Size() << " shards"
            << ", running# " << BorrowedCompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished borrowed compaction "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << ", shardIdx# " << shardIdx
            << " in# " << duration.MilliSeconds()
            << ", next wakeup# " << BorrowedCompactionQueue->GetWakeupDelta()
            << ", rate# " << BorrowedCompactionQueue->GetRate()
            << ", in queue# " << BorrowedCompactionQueue->Size() << " shards"
            << ", running# " << BorrowedCompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    }

    RunningBorrowedCompactions.erase(shardIdx);

    TabletCounters->Cumulative()[COUNTER_BORROWED_COMPACTION_OK].Increment(1);
    UpdateBorrowedCompactionQueueMetrics();
}

} // NKikimr::NSchemeShard
