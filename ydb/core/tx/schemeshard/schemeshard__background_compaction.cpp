#include "schemeshard_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCompaction(const TShardCompactionInfo& info) {
    UpdateBackgroundCompactionQueueMetrics();

    auto ctx = ActorContext();

    const auto& shardIdx = info.ShardIdx;
    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        YDB_LOG_WARN_CTX(ctx, "[BackgroundCompaction] [Start] Failed to resolve shard info",
            {"shardIdx", shardIdx},
            {"schemeshard", TabletID()},
        );

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    YDB_LOG_INFO_CTX(ctx, "[BackgroundCompaction] [Start] Compacting",
        {"pathId", pathId},
        {"datashard", datashardId},
        {"compactionInfo", info},
        {"nextWakeup", BackgroundCompactionQueue->GetWakeupDelta()},
        {"rate", BackgroundCompactionQueue->GetRate()},
        {"queueSize", BackgroundCompactionQueue->Size()},
        {"waitingSize", BackgroundCompactionQueue->WaitingSize()},
        {"runningSize", BackgroundCompactionQueue->RunningSize()},
        {"schemeshard", TabletID()},
    );

    std::unique_ptr<TEvDataShard::TEvCompactTable> request(new TEvDataShard::TEvCompactTable(pathId.OwnerId, pathId.LocalPathId));
    if (BackgroundCompactionQueue->GetReadyQueue().GetConfig().CompactSinglePartedShards) {
        request->Record.SetCompactSinglePartedShards(true);
    }

    PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        request.release(),
        static_cast<ui64>(ECompactionType::Background));

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::OnBackgroundCompactionTimeout(const TShardCompactionInfo& info) {
    UpdateBackgroundCompactionQueueMetrics();
    TabletCounters->Cumulative()[COUNTER_BACKGROUND_COMPACTION_TIMEOUT].Increment(1);

    auto ctx = ActorContext();

    const auto& shardIdx = info.ShardIdx;
    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        YDB_LOG_WARN_CTX(ctx, "[BackgroundCompaction] [Timeout] Failed to resolve shard info",
            {"shardIdx", shardIdx},
            {"schemeshard", TabletID()},
        );
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    YDB_LOG_INFO_CTX(ctx, "[BackgroundCompaction] [Timeout] Compaction timeouted",
        {"pathId", pathId},
        {"datashard", datashardId},
        {"compactionInfo", info},
        {"nextWakeup", BackgroundCompactionQueue->GetWakeupDelta()},
        {"rate", BackgroundCompactionQueue->GetRate()},
        {"queueSize", BackgroundCompactionQueue->Size()},
        {"waitingSize", BackgroundCompactionQueue->WaitingSize()},
        {"runningSize", BackgroundCompactionQueue->RunningSize()},
        {"schemeshard", TabletID()},
    );
}

void TSchemeShard::HandleBackgroundCompactionResult(TEvDataShard::TEvCompactTableResult::TPtr &ev, const TActorContext &ctx) {
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
        YDB_LOG_WARN_CTX(ctx, "[BackgroundCompaction] [Finished] Failed to resolve shard info",
            {"pathId", pathId},
            {"datashard", tabletId},
            {"durationMs", duration.MilliSeconds()},
            {"status", (int)record.GetStatus()},
            {"nextWakeup", BackgroundCompactionQueue->GetWakeupDelta()},
            {"rate", BackgroundCompactionQueue->GetRate()},
            {"queueSize", BackgroundCompactionQueue->Size()},
            {"waitingSize", BackgroundCompactionQueue->WaitingSize()},
            {"runningSize", BackgroundCompactionQueue->RunningSize()},
            {"schemeshard", TabletID()},
        );
    } else {
        YDB_LOG_INFO_CTX(ctx, "[BackgroundCompaction] [Finished] Compaction completed",
            {"pathId", pathId},
            {"datashard", tabletId},
            {"shardIdx", shardIdx},
            {"durationMs", duration.MilliSeconds()},
            {"status", (int)record.GetStatus()},
            {"nextWakeup", BackgroundCompactionQueue->GetWakeupDelta()},
            {"rate", BackgroundCompactionQueue->GetRate()},
            {"queueSize", BackgroundCompactionQueue->Size()},
            {"waitingSize", BackgroundCompactionQueue->WaitingSize()},
            {"runningSize", BackgroundCompactionQueue->RunningSize()},
            {"schemeshard", TabletID()},
        );
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
        YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Enqueue] Skipped shard with borrowed parts",
            {"shardIdx", shardIdx},
            {"schemeshard", TabletID()},
        );
        return;
    }

    if (stats.HasLoanedData) {
        YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Enqueue] Skipped shard with loaned parts",
            {"shardIdx", shardIdx},
            {"schemeshard", TabletID()},
        );
        return;
    }

    if (BackgroundCompactionQueue->Enqueue(TShardCompactionInfo(shardIdx, stats))) {
        YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Enqueue] Enqueued shard",
            {"shardIdx", shardIdx},
            {"partCount", stats.PartCount},
            {"rowCount", stats.RowCount},
            {"searchHeight", stats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(stats.FullCompactionTs)},
            {"schemeshard", TabletID()},
        );

        UpdateBackgroundCompactionQueueMetrics();
    } else {
        YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Enqueue] Skipped or already exists shard",
            {"shardIdx", shardIdx},
            {"partCount", stats.PartCount},
            {"rowCount", stats.RowCount},
            {"searchHeight", stats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(stats.FullCompactionTs)},
            {"schemeshard", TabletID()},
        );
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
            YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Update] Removed shard with borrowed parts",
                {"shardIdx", shardIdx},
                {"schemeshard", TabletID()},
            );
        }
        return;
    }

    if (newStats.HasLoanedData) {
        if (RemoveBackgroundCompaction(shardIdx)) {
            YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Update] Removed shard with loaned parts",
                {"shardIdx", shardIdx},
                {"schemeshard", TabletID()},
            );
        }
        return;
    }

    TShardCompactionInfo info(shardIdx, newStats);
    if (BackgroundCompactionQueue->Update(info)) {
        YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Update] Updated shard",
            {"shardIdx", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"schemeshard", TabletID()},
        );
    } else if (BackgroundCompactionQueue->Enqueue(std::move(info))) {
        YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Update] Enqueued shard",
            {"shardIdx", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"schemeshard", TabletID()},
        );
    } else {
        YDB_LOG_TRACE_CTX(ctx, "[BackgroundCompaction] [Update] Skipped shard",
            {"shardIdx", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"schemeshard", TabletID()},
        );
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

    TabletCounters->Simple()[COUNTER_BACKGROUND_COMPACTION_QUEUE_SIZE].Set(BackgroundCompactionQueue->Size());
    TabletCounters->Simple()[COUNTER_BACKGROUND_COMPACTION_QUEUE_RUNNING].Set(BackgroundCompactionQueue->RunningSize());
    TabletCounters->Simple()[COUNTER_BACKGROUND_COMPACTION_QUEUE_WAITING_REPEAT].Set(BackgroundCompactionQueue->WaitingSize());

    const auto& queue = BackgroundCompactionQueue->GetReadyQueue();

    TabletCounters->Simple()[COUNTER_BACKGROUND_COMPACTION_QUEUE_SIZE_SH].Set(queue.SizeBySearchHeight());
    TabletCounters->Simple()[COUNTER_BACKGROUND_COMPACTION_QUEUE_SIZE_DELETES].Set(queue.SizeByRowDeletes());
}

}

#undef YDB_LOG_THIS_FILE_COMPONENT
