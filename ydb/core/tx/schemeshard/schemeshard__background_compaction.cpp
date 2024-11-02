#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCompaction(const TShardCompactionInfo& info) {
    UpdateBackgroundCompactionQueueMetrics();

    auto ctx = ActorContext();

    const auto& shardIdx = info.ShardIdx;
    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[BackgroundCompaction] [Start] Failed to resolve shard info "
            "for background compaction# " << shardIdx
            << " at schemeshard# " << TabletID());

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[BackgroundCompaction] [Start] Compacting "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", compactionInfo# " << info
        << ", next wakeup in# " << BackgroundCompactionQueue->GetWakeupDelta()
        << ", rate# " << BackgroundCompactionQueue->GetRate()
        << ", in queue# " << BackgroundCompactionQueue->Size() << " shards"
        << ", waiting after compaction# " << BackgroundCompactionQueue->WaitingSize() << " shards"
        << ", running# " << BackgroundCompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    std::unique_ptr<TEvDataShard::TEvCompactTable> request(new TEvDataShard::TEvCompactTable(pathId.OwnerId, pathId.LocalPathId));
    if (BackgroundCompactionQueue->GetReadyQueue().GetConfig().CompactSinglePartedShards) {
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
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[BackgroundCompaction] [Timeout] Failed to resolve shard info "
            "for timeout background compaction# " << shardIdx
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[BackgroundCompaction] [Timeout] Compaction timeouted "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", compactionInfo# " << info
        << ", next wakeup in# " << BackgroundCompactionQueue->GetWakeupDelta()
        << ", rate# " << BackgroundCompactionQueue->GetRate()
        << ", in queue# " << BackgroundCompactionQueue->Size() << " shards"
        << ", waiting after compaction# " << BackgroundCompactionQueue->WaitingSize() << " shards"
        << ", running# " << BackgroundCompactionQueue->RunningSize() << " shards"
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
    auto duration = BackgroundCompactionQueue->OnDone(TShardCompactionInfo(shardIdx, stats));

    if (shardIdx == InvalidShardIdx) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[BackgroundCompaction] [Finished] Failed to resolve shard info "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << ", next wakeup in# " << BackgroundCompactionQueue->GetWakeupDelta()
            << ", rate# " << BackgroundCompactionQueue->GetRate()
            << ", in queue# " << BackgroundCompactionQueue->Size() << " shards"
            << ", waiting after compaction# " << BackgroundCompactionQueue->WaitingSize() << " shards"
            << ", running# " << BackgroundCompactionQueue->RunningSize() << " shards"
            << " at schemeshard " << TabletID());
    } else {
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[BackgroundCompaction] [Finished] Compaction completed "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << ", shardIdx# " << shardIdx
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << ", next wakeup in# " << BackgroundCompactionQueue->GetWakeupDelta()
            << ", rate# " << BackgroundCompactionQueue->GetRate()
            << ", in queue# " << BackgroundCompactionQueue->Size() << " shards"
            << ", waiting after compaction# " << BackgroundCompactionQueue->WaitingSize() << " shards"
            << ", running# " << BackgroundCompactionQueue->RunningSize() << " shards"
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
    if (!BackgroundCompactionQueue)
        return;

    auto ctx = ActorContext();

    if (stats.HasBorrowedData) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[BackgroundCompaction] [Enqueue] Skipped shard# " << shardIdx
            << " with borrowed parts at schemeshard " << TabletID());
        return;
    }

    if (stats.HasLoanedData) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[BackgroundCompaction] [Enqueue] Skipped shard# " << shardIdx
            << " with loaned parts at schemeshard " << TabletID());
        return;
    }

    if (BackgroundCompactionQueue->Enqueue(TShardCompactionInfo(shardIdx, stats))) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[BackgroundCompaction] [Enqueue] Enqueued shard# " << shardIdx
            << " with partCount# " << stats.PartCount
            << ", rowCount# " << stats.RowCount
            << ", searchHeight# " << stats.SearchHeight
            << ", lastFullCompaction# " << TInstant::Seconds(stats.FullCompactionTs)
            << " at schemeshard " << TabletID());
        
        UpdateBackgroundCompactionQueueMetrics();
    } else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[BackgroundCompaction] [Enqueue] Skipped or already exists shard# " << shardIdx
            << " with partCount# " << stats.PartCount
            << ", rowCount# " << stats.RowCount
            << ", searchHeight# " << stats.SearchHeight
            << ", lastFullCompaction# " << TInstant::Seconds(stats.FullCompactionTs)
            << " at schemeshard " << TabletID());
    }
}

void TSchemeShard::UpdateBackgroundCompaction(
    const TShardIdx& shardIdx,
    const TPartitionStats& newStats)
{
    if (!BackgroundCompactionQueue)
        return;

    auto ctx = ActorContext();

    if (newStats.HasBorrowedData) {
        if (RemoveBackgroundCompaction(shardIdx)) {
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[BackgroundCompaction] [Update] Removed shard# " << shardIdx
                << " with borrowed parts at schemeshard " << TabletID());
        }
        return;
    }

    if (newStats.HasLoanedData) {
        if (RemoveBackgroundCompaction(shardIdx)) {
            LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
                "[BackgroundCompaction] [Update] Removed shard# " << shardIdx
                << " with loaned parts at schemeshard " << TabletID());
        }
        return;
    }

    TShardCompactionInfo info(shardIdx, newStats);
    if (BackgroundCompactionQueue->Update(info)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[BackgroundCompaction] [Update] Updated shard# " << shardIdx
            << " with partCount# " << newStats.PartCount
            << ", rowCount# " << newStats.RowCount
            << ", searchHeight# " << newStats.SearchHeight
            << ", lastFullCompaction# " << TInstant::Seconds(newStats.FullCompactionTs)
            << " at schemeshard " << TabletID());
    } else if (BackgroundCompactionQueue->Enqueue(std::move(info))) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[BackgroundCompaction] [Update] Enqueued shard# " << shardIdx
            << " with partCount# " << newStats.PartCount
            << ", rowCount# " << newStats.RowCount
            << ", searchHeight# " << newStats.SearchHeight
            << ", lastFullCompaction# " << TInstant::Seconds(newStats.FullCompactionTs)
            << " at schemeshard " << TabletID());
    } else {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[BackgroundCompaction] [Update] Skipped shard# " << shardIdx
            << " with partCount# " << newStats.PartCount
            << ", rowCount# " << newStats.RowCount
            << ", searchHeight# " << newStats.SearchHeight
            << ", lastFullCompaction# " << TInstant::Seconds(newStats.FullCompactionTs)
            << " at schemeshard " << TabletID());
    }

    UpdateBackgroundCompactionQueueMetrics();
}

bool TSchemeShard::RemoveBackgroundCompaction(const TShardIdx& shardIdx) {
    if (!BackgroundCompactionQueue)
        return false;

    if (BackgroundCompactionQueue->Remove(TShardCompactionInfo(shardIdx))) {
        UpdateBackgroundCompactionQueueMetrics();
        return true;
    }

    return false;
}

void TSchemeShard::UpdateBackgroundCompactionQueueMetrics() {
    if (!BackgroundCompactionQueue)
        return;

    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE].Set(BackgroundCompactionQueue->Size());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_RUNNING].Set(BackgroundCompactionQueue->RunningSize());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_WAITING_REPEAT].Set(BackgroundCompactionQueue->WaitingSize());

    const auto& queue = BackgroundCompactionQueue->GetReadyQueue();

    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE_SH].Set(queue.SizeBySearchHeight());
    TabletCounters->Simple()[COUNTER_COMPACTION_QUEUE_SIZE_DELETES].Set(queue.SizeByRowDeletes());
}

}
