#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartForcedCompaction(const TShardIdx& shardIdx) {
    UpdateForcedCompactionQueueMetrics();

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info "
            "for forced compaction# " << shardIdx
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RunForcedCompaction "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << ForcedCompactionQueue->GetWakeupDelta()
        << ", rate# " << ForcedCompactionQueue->GetRate()
        << ", in queue# " << ForcedCompactionQueue->Size() << " shards"
        << ", running# " << ForcedCompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    std::unique_ptr<TEvDataShard::TEvCompactTable> request(
        new TEvDataShard::TEvCompactTable(pathId.OwnerId, pathId.LocalPathId));

    RunningForcedCompactions[shardIdx] = PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        request.release());

    // Track the running shard
    auto tableIt = ForcedCompactionsByTable.find(pathId);
    if (tableIt != ForcedCompactionsByTable.end()) {
        tableIt->second.RunningShards.insert(shardIdx);
    }

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnForcedCompactionTimeout(const TShardIdx& shardIdx) {
    UpdateForcedCompactionQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_FORCED_COMPACTION_TIMEOUT].Increment(1);

    RunningForcedCompactions.erase(shardIdx);

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info "
            "for timeout forced compaction# " << shardIdx
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Forced compaction timeout "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << ForcedCompactionQueue->GetWakeupDelta()
        << ", in queue# " << ForcedCompactionQueue->Size() << " shards"
        << ", running# " << ForcedCompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    // Update tracking
    auto tableIt = ForcedCompactionsByTable.find(pathId);
    if (tableIt != ForcedCompactionsByTable.end()) {
        tableIt->second.RunningShards.erase(shardIdx);
    }

    // retry
    EnqueueForcedCompaction(shardIdx);
}

void TSchemeShard::ForcedCompactionHandleDisconnect(TTabletId tabletId, const TActorId& clientId) {
    auto tabletIt = TabletIdToShardIdx.find(tabletId);
    if (tabletIt == TabletIdToShardIdx.end())
        return; // just sanity check
    const auto& shardIdx = tabletIt->second;

    auto it = RunningForcedCompactions.find(shardIdx);
    if (it == RunningForcedCompactions.end())
        return;

    if (it->second != clientId)
        return;

    RunningForcedCompactions.erase(it);

    // disconnected from node we have requested forced compaction. We just resend request, because it
    // is safe: if first request is executing or has been already executed, then second request will be ignored.

    StartForcedCompaction(shardIdx);
}

void TSchemeShard::EnqueueForcedCompaction(const TShardIdx& shardIdx) {
    if (!ForcedCompactionQueue)
        return;

    auto ctx = ActorContext();

    if (ForcedCompactionQueue->Enqueue(shardIdx)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Forced compaction enqueued shard# " << shardIdx << " at schemeshard " << TabletID());
        UpdateForcedCompactionQueueMetrics();

        // Update tracking
        auto it = ShardInfos.find(shardIdx);
        if (it != ShardInfos.end()) {
            const auto& pathId = it->second.PathId;
            auto tableIt = ForcedCompactionsByTable.find(pathId);
            if (tableIt != ForcedCompactionsByTable.end()) {
                tableIt->second.QueuedShards.insert(shardIdx);
            }
        }
    }
}

void TSchemeShard::RemoveForcedCompaction(const TShardIdx& shardIdx) {
    if (!ForcedCompactionQueue)
        return;

    RunningForcedCompactions.erase(shardIdx);
    ForcedCompactionQueue->Remove(shardIdx);
    UpdateForcedCompactionQueueMetrics();

    // Update tracking
    auto it = ShardInfos.find(shardIdx);
    if (it != ShardInfos.end()) {
        const auto& pathId = it->second.PathId;
        auto tableIt = ForcedCompactionsByTable.find(pathId);
        if (tableIt != ForcedCompactionsByTable.end()) {
            tableIt->second.QueuedShards.erase(shardIdx);
            tableIt->second.RunningShards.erase(shardIdx);
        }
    }
}

void TSchemeShard::UpdateForcedCompactionQueueMetrics() {
    if (!ForcedCompactionQueue)
        return;

    TabletCounters->Simple()[COUNTER_FORCED_COMPACTION_QUEUE_SIZE].Set(ForcedCompactionQueue->Size());
    TabletCounters->Simple()[COUNTER_FORCED_COMPACTION_QUEUE_RUNNING].Set(ForcedCompactionQueue->RunningSize());
}

void TSchemeShard::Handle(TEvDataShard::TEvCompactTableResult::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;

    const TTabletId tabletId(record.GetTabletId());
    const TShardIdx shardIdx = GetShardIdx(tabletId);

    auto pathId = TPathId(
        record.GetPathId().GetOwnerId(),
        record.GetPathId().GetLocalId());

    auto duration = ForcedCompactionQueue->OnDone(shardIdx);

    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished forced compaction of unknown shard "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << " in# " << duration.MilliSeconds()
            << ", next wakeup# " << ForcedCompactionQueue->GetWakeupDelta()
            << ", rate# " << ForcedCompactionQueue->GetRate()
            << ", in queue# " << ForcedCompactionQueue->Size() << " shards"
            << ", running# " << ForcedCompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished forced compaction "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << ", shardIdx# " << shardIdx
            << " in# " << duration.MilliSeconds()
            << ", next wakeup# " << ForcedCompactionQueue->GetWakeupDelta()
            << ", rate# " << ForcedCompactionQueue->GetRate()
            << ", in queue# " << ForcedCompactionQueue->Size() << " shards"
            << ", running# " << ForcedCompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());

        // Update tracking
        auto tableIt = ForcedCompactionsByTable.find(pathId);
        if (tableIt != ForcedCompactionsByTable.end()) {
            tableIt->second.CompactedShards++;
            tableIt->second.RunningShards.erase(shardIdx);
            tableIt->second.QueuedShards.erase(shardIdx);
        }
    }

    RunningForcedCompactions.erase(shardIdx);

    TabletCounters->Cumulative()[COUNTER_FORCED_COMPACTION_OK].Increment(1);
    UpdateForcedCompactionQueueMetrics();
}

void TSchemeShard::Handle(TEvSchemeShard::TEvForceCompaction::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;

    TPathId pathId(record.GetPathId().GetOwnerId(), record.GetPathId().GetLocalPathId());

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Received TEvForceCompaction for pathId# " << pathId
        << " at schemeshard " << TabletID());

    // Check if path exists
    auto pathIt = PathsById.find(pathId);
    if (pathIt == PathsById.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Path not found for pathId# " << pathId
            << " at schemeshard " << TabletID());
        
        auto response = MakeHolder<TEvSchemeShard::TEvForceCompactionResult>(
            pathId.OwnerId, pathId.LocalPathId, 0);
        response->Record.SetErrorReason("Path not found");
        ctx.Send(ev->Sender, response.Release());
        return;
    }

    const auto& pathElement = pathIt->second;

    // Check if it's a table
    auto tableIt = Tables.find(pathId);
    if (tableIt == Tables.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "Not a table for pathId# " << pathId
            << " at schemeshard " << TabletID());
        
        auto response = MakeHolder<TEvSchemeShard::TEvForceCompactionResult>(
            pathId.OwnerId, pathId.LocalPathId, 0);
        response->Record.SetErrorReason("Not a table");
        ctx.Send(ev->Sender, response.Release());
        return;
    }

    const auto& tableInfo = tableIt->second;
    const auto& shards = tableInfo->GetPartitions();

    // Initialize tracking structure
    auto& compactionInfo = ForcedCompactionsByTable[pathId];
    compactionInfo.TotalShards = shards.size();
    compactionInfo.CompactedShards = 0;
    compactionInfo.QueuedShards.clear();
    compactionInfo.RunningShards.clear();

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Enqueuing " << shards.size() << " shards for forced compaction of pathId# " << pathId
        << " at schemeshard " << TabletID());

    // Enqueue all shards
    for (const auto& shard : shards) {
        EnqueueForcedCompaction(shard.ShardIdx);
    }

    auto response = MakeHolder<TEvSchemeShard::TEvForceCompactionResult>(
        pathId.OwnerId, pathId.LocalPathId, shards.size());
    ctx.Send(ev->Sender, response.Release());
}

void TSchemeShard::Handle(TEvSchemeShard::TEvGetForceCompactionProgress::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;

    TPathId pathId(record.GetPathId().GetOwnerId(), record.GetPathId().GetLocalPathId());

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Received TEvGetForceCompactionProgress for pathId# " << pathId
        << " at schemeshard " << TabletID());

    auto it = ForcedCompactionsByTable.find(pathId);
    if (it == ForcedCompactionsByTable.end()) {
        // No active forced compaction for this table
        auto response = MakeHolder<TEvSchemeShard::TEvGetForceCompactionProgressResult>(
            pathId.OwnerId, pathId.LocalPathId, 0, 0, 0);
        ctx.Send(ev->Sender, response.Release());
        return;
    }

    const auto& info = it->second;
    auto response = MakeHolder<TEvSchemeShard::TEvGetForceCompactionProgressResult>(
        pathId.OwnerId, pathId.LocalPathId,
        info.TotalShards,
        info.CompactedShards,
        info.QueuedShards.size() + info.RunningShards.size());
    
    ctx.Send(ev->Sender, response.Release());

    LOG_DEBUG_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "Forced compaction progress for pathId# " << pathId
        << ": total# " << info.TotalShards
        << ", compacted# " << info.CompactedShards
        << ", queued# " << (info.QueuedShards.size() + info.RunningShards.size())
        << " at schemeshard " << TabletID());
}

} // NKikimr::NSchemeShard
