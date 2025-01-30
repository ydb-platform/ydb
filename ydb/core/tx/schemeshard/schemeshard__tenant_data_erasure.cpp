#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartTenantDataErasure(const TShardIdx& shardIdx) {
    UpdateTenantDataErasureQueueMetrics();

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Start] Failed to resolve shard info "
            "for data erasure# " << shardIdx
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Start] Data erasure "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << TenantDataErasureQueue->GetWakeupDelta()
        << ", rate# " << TenantDataErasureQueue->GetRate()
        << ", in queue# " << TenantDataErasureQueue->Size() << " shards"
        << ", running# " << TenantDataErasureQueue->RunningSize() << " shards"
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

void TSchemeShard::OnTenantDataErasureTimeout(const TShardIdx& shardIdx) {
    UpdateTenantDataErasureQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_TENANT_DATA_ERASURE_TIMEOUT].Increment(1);

    // RunningBorrowedCompactions.erase(shardIdx);

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Timeout] Failed to resolve shard info "
            "for timeout data erasure# " << shardIdx
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[TenantDataErasure] [Timeout] Data erasure timeout "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << TenantDataErasureQueue->GetWakeupDelta()
        << ", in queue# " << TenantDataErasureQueue->Size() << " shards"
        << ", running# " << TenantDataErasureQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    // retry
    EnqueueTenantDataErasure(shardIdx);
}

void TSchemeShard::TenantDataErasureHandleDisconnect(TTabletId tabletId, const TActorId& /*clientId*/) {
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

    StartTenantDataErasure(shardIdx);
}

void TSchemeShard::EnqueueTenantDataErasure(const TShardIdx& shardIdx) {
    if (!TenantDataErasureQueue)
        return;

    auto ctx = ActorContext();

    if (TenantDataErasureQueue->Enqueue(shardIdx)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasure] [Enqueue] Enqueued shard# " << shardIdx << " at schemeshard " << TabletID());
        UpdateTenantDataErasureQueueMetrics();
    }  else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[TenantDataErasure] [Enqueue] Skipped or already exists shard# " << shardIdx << " at schemeshard " << TabletID());
    }
}
void TSchemeShard::RemoveTenantDataErasure(const TShardIdx& shardIdx) {
    if (!TenantDataErasureQueue)
        return;

    // RunningBorrowedCompactions.erase(shardIdx);
    TenantDataErasureQueue->Remove(shardIdx);
    UpdateTenantDataErasureQueueMetrics();
}

void TSchemeShard::UpdateTenantDataErasureQueueMetrics() {
    if (!TenantDataErasureQueue) {
        return;
    }

    TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_QUEUE_SIZE].Set(TenantDataErasureQueue->Size());
    TabletCounters->Simple()[COUNTER_TENANT_DATA_ERASURE_QUEUE_RUNNING].Set(TenantDataErasureQueue->RunningSize());
}

} // NKikimr::NSchemeShard
