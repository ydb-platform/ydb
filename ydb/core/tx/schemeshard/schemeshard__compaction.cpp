#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCompaction(const TShardCompactionInfo& info) {
    UpdateBackgroundCompactionQueueMetrics();

    auto ctx = ActorContext();

    const auto& shardIdx = info.ShardIdx;
    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info "
            "for background compaction# " << shardIdx
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "RunBackgroundCompaction "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", compactionInfo# " << info
        << ", next wakeup in# " << CompactionQueue->GetWakeupDelta()
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

void TSchemeShard::OnBackgroundCompactionTimeout(const TShardCompactionInfo& info) {
    UpdateBackgroundCompactionQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_TIMEOUT].Increment(1);

    auto ctx = ActorContext();

    const auto& shardIdx = info.ShardIdx;
    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Unable to resolve shard info "
            "for timeout background compaction# " << shardIdx
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Background compaction timeout "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", compactionInfo# " << info
        << ", next wakeup in# " << CompactionQueue->GetWakeupDelta()
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
    TPartitionStats stats;
    stats.FullCompactionTs = now.Seconds();
    auto duration = CompactionQueue->OnDone(TShardCompactionInfo(shardIdx, stats));

    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished background compaction of unknown shard "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << ", next wakeup in# " << CompactionQueue->GetWakeupDelta()
            << ", rate# " << CompactionQueue->GetRate()
            << ", in queue# " << CompactionQueue->Size() << " shards"
            << ", waiting after compaction# " << CompactionQueue->WaitingSize() << " shards"
            << ", running# " << CompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "Finished background compaction "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << ", shardIdx# " << shardIdx
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << ", next wakeup in# " << CompactionQueue->GetWakeupDelta()
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
    case NKikimrTxDataShard::TEvCompactTableResult::LOANED:
        TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_LOANED].Increment(1);
        break;
    }

    UpdateBackgroundCompactionQueueMetrics();
}

void TSchemeShard::EnqueueBackgroundCompaction(
    const TShardIdx& shardIdx,
    const TPartitionStats& stats)
{
    if (!CompactionQueue)
        return;

    auto ctx = ActorContext();

    if (stats.HasBorrowedData) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "background compaction enqueue skipped shard# " << shardIdx
            << " with borrowed parts at schemeshard " << TabletID());
        return;
    }

    if (stats.HasLoanedData) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "background compaction enqueue skipped shard# " << shardIdx
            << " with loaned parts at schemeshard " << TabletID());
        return;
    }

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "background compaction enqueue shard# " << shardIdx
        << " with partCount# " << stats.PartCount
        << ", rowCount# " << stats.RowCount
        << ", searchHeight# " << stats.SearchHeight
        << ", lastFullCompaction# " << TInstant::Seconds(stats.FullCompactionTs)
        << " at schemeshard " << TabletID());

    CompactionQueue->Enqueue(TShardCompactionInfo(shardIdx, stats));
    UpdateBackgroundCompactionQueueMetrics();
}

void TSchemeShard::UpdateBackgroundCompaction(
    const TShardIdx& shardIdx,
    const TPartitionStats& newStats)
{
    if (!CompactionQueue)
        return;

    auto ctx = ActorContext();

    if (newStats.HasBorrowedData) {
        if (RemoveBackgroundCompaction(shardIdx)) {
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "background compaction update removed shard# " << shardIdx
                << " with borrowed parts at schemeshard " << TabletID());
        }
        return;
    }

    if (newStats.HasLoanedData) {
        if (RemoveBackgroundCompaction(shardIdx)) {
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "background compaction update removed shard# " << shardIdx
                << " with loaned parts at schemeshard " << TabletID());
        }
        return;
    }

    LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
        "background compaction enqueue/update shard# " << shardIdx
        << " with partCount# " << newStats.PartCount
        << ", rowCount# " << newStats.RowCount
        << ", searchHeight# " << newStats.SearchHeight
        << ", lastFullCompaction# " << TInstant::Seconds(newStats.FullCompactionTs)
        << " at schemeshard " << TabletID());

    TShardCompactionInfo info(shardIdx, newStats);
    if (!CompactionQueue->Update(info)) {
        CompactionQueue->Enqueue(std::move(info));
    }
    UpdateBackgroundCompactionQueueMetrics();
}

bool TSchemeShard::RemoveBackgroundCompaction(const TShardIdx& shardIdx) {
    if (!CompactionQueue)
        return false;

    if (CompactionQueue->Remove(TShardCompactionInfo(shardIdx))) {
        UpdateBackgroundCompactionQueueMetrics();
        return true;
    }

    return false;
}

void TSchemeShard::ShardRemoved(const TShardIdx& shardIdx) {
    RemoveBackgroundCompaction(shardIdx);
    RemoveBorrowedCompaction(shardIdx);
    RemoveShardMetrics(shardIdx);
}

void TSchemeShard::UpdateBackgroundCompactionQueueMetrics() {
    if (!CompactionQueue)
        return;

    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE].Set(CompactionQueue->Size());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_RUNNING].Set(CompactionQueue->RunningSize());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_WAITING_REPEAT].Set(CompactionQueue->WaitingSize());

    const auto& queue = CompactionQueue->GetReadyQueue();

    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE_SH].Set(queue.SizeBySearchHeight());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE_DELETES].Set(queue.SizeByRowDeletes());
}

void TSchemeShard::UpdateShardMetrics(
    const TShardIdx& shardIdx,
    const TPartitionStats& newStats)
{
    if (newStats.HasBorrowedData)
        ShardsWithBorrowed.insert(shardIdx);
    else
        ShardsWithBorrowed.erase(shardIdx);
    TabletCounters->Simple()[COUNTER_SHARDS_WITH_BORROWED_DATA].Set(ShardsWithBorrowed.size());

    if (newStats.HasLoanedData)
        ShardsWithLoaned.insert(shardIdx);
    else
        ShardsWithLoaned.erase(shardIdx);
    TabletCounters->Simple()[COUNTER_SHARDS_WITH_LOANED_DATA].Set(ShardsWithLoaned.size());

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
    ShardsWithBorrowed.erase(shardIdx);
    TabletCounters->Simple()[COUNTER_SHARDS_WITH_BORROWED_DATA].Set(ShardsWithBorrowed.size());

    ShardsWithLoaned.erase(shardIdx);
    TabletCounters->Simple()[COUNTER_SHARDS_WITH_LOANED_DATA].Set(ShardsWithLoaned.size());

    auto it = PartitionMetricsMap.find(shardIdx);
    if (it == PartitionMetricsMap.end())
        return;

    const auto& metrics = it->second;
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_SEARCH_HEIGHT].DecrementFor(metrics.SearchHeight);
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_FULL_COMPACTION].DecrementFor(metrics.HoursSinceFullCompaction);
    TabletCounters->Percentile()[COUNTER_SHARDS_WITH_ROW_DELETES].DecrementFor(metrics.RowDeletes);

    PartitionMetricsMap.erase(it);
}

} // NKikimr::NSchemeShard
