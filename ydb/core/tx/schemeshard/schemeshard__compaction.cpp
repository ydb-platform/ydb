#include "schemeshard_impl.h"

namespace NKikimr {
namespace NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCompaction(const TShardIdx& shardId) {
    UpdateBackgroundCompactionQueueMetrics();

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
        << ", next wakeup# " << CompactionQueue->GetWakeupTime()
        << ", rate# " << CompactionQueue->GetRate()
        << ", in queue# " << CompactionQueue->Size() << " shards"
        << ", waiting after compaction# " << CompactionQueue->WaitingSize() << " shards"
        << ", running# " << CompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    std::unique_ptr<TEvDataShard::TEvCompactTable> request(new TEvDataShard::TEvCompactTable(pathId.OwnerId, pathId.LocalPathId));
    if (CompactionQueue->GetReadyQueue().GetConfig().CompactSinglePartedShards) {
        request->Record.SetCompactSinglePartedShards(true);
    }

    PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        request.release());

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnBackgroundCompactionTimeout(const TShardIdx& shardId) {
    UpdateBackgroundCompactionQueueMetrics();

    TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_TIMEOUT].Increment(1);

    auto ctx = TActivationContext::ActorContextFor(SelfId());

    auto it = ShardInfos.find(shardId);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info "
            "for timeout background compaction# " << shardId
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Background compaction timeout "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup# " << CompactionQueue->GetWakeupTime()
        << ", rate# " << CompactionQueue->GetRate()
        << ", in queue# " << CompactionQueue->Size() << " shards"
        << ", waiting after compaction# " << CompactionQueue->WaitingSize() << " shards"
        << ", running# " << CompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());
}

void TSchemeShard::Handle(TEvDataShard::TEvCompactTableResult::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;

    const TTabletId tabletId(record.GetTabletId());
    const TShardIdx shardIdx = GetShardIdx(tabletId);

    auto pathId = TPathId(
        record.GetPathId().GetOwnerId(),
        record.GetPathId().GetLocalId());

    // it's OK to OnDone InvalidShardIdx
    // move shard to the end of all queues
    TInstant now = AppData(ctx)->TimeProvider->Now();
    TTableInfo::TPartitionStats stats;
    stats.FullCompactionTs = now.Seconds();
    auto duration = CompactionQueue->OnDone(TShardCompactionInfo(shardIdx, stats));

    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished background compaction of unknown shard "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << ", next wakeup# " << CompactionQueue->GetWakeupTime()
            << ", rate# " << CompactionQueue->GetRate()
            << ", in queue# " << CompactionQueue->Size() << " shards"
            << ", waiting after compaction# " << CompactionQueue->WaitingSize() << " shards"
            << ", running# " << CompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished background compaction "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << ", next wakeup# " << CompactionQueue->GetWakeupTime()
            << ", rate# " << CompactionQueue->GetRate()
            << ", in queue# " << CompactionQueue->Size() << " shards"
            << ", waiting after compaction# " << CompactionQueue->WaitingSize() << " shards"
            << ", running# " << CompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    }

    auto& histCounters = TabletCounters->Percentile();

    switch (record.GetStatus()) {
    case NKikimrTxDataShard::TEvCompactTableResult::OK:
        TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_OK].Increment(1);
        if (duration)
            histCounters[COUNTER_BACKGROUND_COMPACTION_OK_LATENCY].IncrementFor(duration.MilliSeconds());
        break;
    case NKikimrTxDataShard::TEvCompactTableResult::NOT_NEEDED:
        TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_NOT_NEEDED].Increment(1);
        break;
    case NKikimrTxDataShard::TEvCompactTableResult::FAILED:
        TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_FAILED].Increment(1);
        break;
    case NKikimrTxDataShard::TEvCompactTableResult::BORROWED:
        TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_BORROWED].Increment(1);
        break;
    }

    UpdateBackgroundCompactionQueueMetrics();
}

void TSchemeShard::EnqueueCompaction(
    const TShardIdx& shardIdx,
    const TTableInfo::TPartitionStats& stats)
{
    if (!CompactionQueue)
        return;

    if (stats.HasBorrowed)
        return;

    CompactionQueue->Enqueue(TShardCompactionInfo(shardIdx, stats));
    UpdateBackgroundCompactionQueueMetrics();
}

void TSchemeShard::UpdateCompaction(
    const TShardIdx& shardIdx,
    const TTableInfo::TPartitionStats& newStats)
{
    if (!CompactionQueue)
        return;

    if (newStats.HasBorrowed) {
        RemoveCompaction(shardIdx);
        return;
    }

    TShardCompactionInfo info(shardIdx, newStats);
    if (!CompactionQueue->Update(info))
        CompactionQueue->Enqueue(std::move(info));
    UpdateBackgroundCompactionQueueMetrics();
}

void TSchemeShard::RemoveCompaction(const TShardIdx& shardIdx) {
    if (!CompactionQueue)
        return;

    CompactionQueue->Remove(TShardCompactionInfo(shardIdx));
    UpdateBackgroundCompactionQueueMetrics();
}

void TSchemeShard::ShardRemoved(const TShardIdx& shardIdx) {
    RemoveCompaction(shardIdx);
    RemoveShardMetrics(shardIdx);
}

void TSchemeShard::UpdateBackgroundCompactionQueueMetrics() {
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE].Set(CompactionQueue->Size());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_RUNNING].Set(CompactionQueue->RunningSize());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_WAITING_REPEAT].Set(CompactionQueue->WaitingSize());

    const auto& queue = CompactionQueue->GetReadyQueue();

    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE_SH].Set(queue.SizeBySearchHeight());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE_DELETES].Set(queue.SizeByRowDeletes());
}

void TSchemeShard::UpdateShardMetrics(
    const TShardIdx& shardIdx,
    const TTableInfo::TPartitionStats& newStats)
{
    THashMap<TShardIdx, TPartitionMetrics>::insert_ctx insertCtx;
    auto it = PartitionMetricsMap.find(shardIdx, insertCtx);
    if (it != PartitionMetricsMap.end()) {
        const auto& metrics = it->second;
        TabletCounters->Percentile()[COUNTER_SHARDS_WITH_SEARCH_HEIGHT].DecrementFor(metrics.SearchHeight);
        TabletCounters->Percentile()[COUNTER_SHARDS_WITH_FULL_COMPACTION].DecrementFor(metrics.HoursSinceFullCompaction);
        TabletCounters->Percentile()[COUNTER_SHARDS_WITH_ROW_DELETES].DecrementFor(metrics.RowDeletes);
    } else {
        it = PartitionMetricsMap.insert_direct(std::make_pair(shardIdx, TPartitionMetrics()), insertCtx);
    }

    auto& metrics = it->second;

    metrics.SearchHeight = newStats.SearchHeight;
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_SEARCH_HEIGHT].IncrementFor(metrics.SearchHeight);

    metrics.RowDeletes = newStats.RowDeletes;
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_ROW_DELETES].IncrementFor(metrics.RowDeletes);

    auto now = AppData()->TimeProvider->Now();
    auto compactionTime = TInstant::Seconds(newStats.FullCompactionTs);
    if (now >= compactionTime)
        metrics.HoursSinceFullCompaction = (now - compactionTime).Hours();
    else
        metrics.HoursSinceFullCompaction = 0;

    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_FULL_COMPACTION].IncrementFor(metrics.HoursSinceFullCompaction);
}

void TSchemeShard::RemoveShardMetrics(const TShardIdx& shardIdx) {
    auto it = PartitionMetricsMap.find(shardIdx);
    if (it == PartitionMetricsMap.end())
        return;

    const auto& metrics = it->second;
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_SEARCH_HEIGHT].DecrementFor(metrics.SearchHeight);
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_FULL_COMPACTION].DecrementFor(metrics.HoursSinceFullCompaction);
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_ROW_DELETES].DecrementFor(metrics.RowDeletes);

    PartitionMetricsMap.erase(it);
}

} // NSchemeShard
} // NKikimr
