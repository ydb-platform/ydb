#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartDataErasure(const TShardIdx& shardIdx) {
    UpdateDataErasureQueueMetrics();

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Start] Failed to resolve shard info "
            "for data erasure# " << shardIdx
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Start] Data erasure "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << DataErasureQueue->GetWakeupDelta()
        << ", rate# " << DataErasureQueue->GetRate()
        << ", in queue# " << DataErasureQueue->Size() << " shards"
        << ", running# " << DataErasureQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    ui64 generation = 0;
    std::unique_ptr<TEvDataShard::TEvForceDataCleanup> request(
        new TEvDataShard::TEvForceDataCleanup(generation));

    /*RunningBorrowedCompactions[shardIdx] = */PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnDataErasureTimeout(const TShardIdx& shardIdx) {
    UpdateDataErasureQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_DATA_ERASURE_TIMEOUT].Increment(1);

    // RunningBorrowedCompactions.erase(shardIdx);

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Timeout] Failed to resolve shard info "
            "for timeout data erasure# " << shardIdx
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[DataErasure] [Timeout] Data erasure timeout "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << DataErasureQueue->GetWakeupDelta()
        << ", in queue# " << DataErasureQueue->Size() << " shards"
        << ", running# " << DataErasureQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    // retry
    EnqueueDataErasure(shardIdx);
}

void TSchemeShard::DataErasureHandleDisconnect(TTabletId tabletId, const TActorId& /*clientId*/) {
    auto tabletIt = TabletIdToShardIdx.find(tabletId);
    if (tabletIt == TabletIdToShardIdx.end())
        return; // just sanity check
    const auto& shardIdx = tabletIt->second;

    // auto it = RunningBorrowedCompactions.find(shardIdx);
    // if (it == RunningBorrowedCompactions.end())
    //     return;

    // if (it->second != clientId)
    //     return;

    // RunningBorrowedCompactions.erase(it);

    // disconnected from node we have requested borrowed compaction. We just resend request, because it
    // is safe: if first request is executing or has been already executed, then second request will be ignored.

    StartDataErasure(shardIdx);
}

void TSchemeShard::EnqueueDataErasure(const TShardIdx& shardIdx) {
    if (!DataErasureQueue)
        return;

    auto ctx = ActorContext();

    if (DataErasureQueue->Enqueue(shardIdx)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[DataErasure] [Enqueue] Enqueued shard# " << shardIdx << " at schemeshard " << TabletID());
        UpdateDataErasureQueueMetrics();
    }  else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[DataErasure] [Enqueue] Skipped or already exists shard# " << shardIdx << " at schemeshard " << TabletID());
    }
}
void TSchemeShard::RemoveDataErasure(const TShardIdx& shardIdx) {
    if (!DataErasureQueue)
        return;

    // RunningBorrowedCompactions.erase(shardIdx);
    DataErasureQueue->Remove(shardIdx);
    UpdateDataErasureQueueMetrics();
}

void TSchemeShard::UpdateDataErasureQueueMetrics() {
    if (!DataErasureQueue) {
        return;
    }

    TabletCounters->Simple()[COUNTER_DATA_ERASURE_QUEUE_SIZE].Set(DataErasureQueue->Size());
    TabletCounters->Simple()[COUNTER_DATA_ERASURE_QUEUE_RUNNING].Set(DataErasureQueue->RunningSize());
}

} // NKikimr::NSchemeShard
