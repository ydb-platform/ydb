#include "schemeshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCompaction(const TShardCompactionInfo& info) {
    UpdateBackgroundCompactionQueueMetrics();

    auto ctx = ActorContext();

    const auto& shardIdx = info.ShardIdx;
    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        YDB_LOG_CTX_WARN(ctx, "",
            {"compaction", shardIdx},
            {"at_schemeshard", TabletID()});

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    YDB_LOG_CTX_INFO(ctx, ", next wakeup shards, waiting after shards shards at schemeshard",
        {"pathId", pathId},
        {"datashard", datashardId},
        {"compactionInfo", info},
        {"in", BackgroundCompactionQueue->GetWakeupDelta()},
        {"rate", BackgroundCompactionQueue->GetRate()},
        {"in_queue", BackgroundCompactionQueue->Size()},
        {"compaction", BackgroundCompactionQueue->WaitingSize()},
        {"running", BackgroundCompactionQueue->RunningSize()},
        {"TabletID", TabletID()});

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
        YDB_LOG_CTX_WARN(ctx, "",
            {"compaction", shardIdx},
            {"at_schemeshard", TabletID()});
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    YDB_LOG_CTX_INFO(ctx, ", next wakeup shards, waiting after shards shards at schemeshard",
        {"pathId", pathId},
        {"datashard", datashardId},
        {"compactionInfo", info},
        {"in", BackgroundCompactionQueue->GetWakeupDelta()},
        {"rate", BackgroundCompactionQueue->GetRate()},
        {"in_queue", BackgroundCompactionQueue->Size()},
        {"compaction", BackgroundCompactionQueue->WaitingSize()},
        {"running", BackgroundCompactionQueue->RunningSize()},
        {"TabletID", TabletID()});
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
        YDB_LOG_CTX_WARN(ctx, "ms, with, next wakeup shards, waiting after shards shards at schemeshard",
            {"pathId", pathId},
            {"datashard", tabletId},
            {"in", duration.MilliSeconds()},
            {"status", (int)record.GetStatus()},
            {"in", BackgroundCompactionQueue->GetWakeupDelta()},
            {"rate", BackgroundCompactionQueue->GetRate()},
            {"in_queue", BackgroundCompactionQueue->Size()},
            {"compaction", BackgroundCompactionQueue->WaitingSize()},
            {"running", BackgroundCompactionQueue->RunningSize()},
            {"TabletID", TabletID()});
    } else {
        YDB_LOG_CTX_INFO(ctx, "ms, with, next wakeup shards, waiting after shards shards at schemeshard",
            {"pathId", pathId},
            {"datashard", tabletId},
            {"shardIdx", shardIdx},
            {"in", duration.MilliSeconds()},
            {"status", (int)record.GetStatus()},
            {"in", BackgroundCompactionQueue->GetWakeupDelta()},
            {"rate", BackgroundCompactionQueue->GetRate()},
            {"in_queue", BackgroundCompactionQueue->Size()},
            {"compaction", BackgroundCompactionQueue->WaitingSize()},
            {"running", BackgroundCompactionQueue->RunningSize()},
            {"TabletID", TabletID()});
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
        YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Enqueue] Skipped with borrowed parts at schemeshard",
            {"shard", shardIdx},
            {"TabletID", TabletID()});
        return;
    }

    if (stats.HasLoanedData) {
        YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Enqueue] Skipped with loaned parts at schemeshard",
            {"shard", shardIdx},
            {"TabletID", TabletID()});
        return;
    }

    if (BackgroundCompactionQueue->Enqueue(TShardCompactionInfo(shardIdx, stats))) {
        YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Enqueue] Enqueued with at schemeshard",
            {"shard", shardIdx},
            {"partCount", stats.PartCount},
            {"rowCount", stats.RowCount},
            {"searchHeight", stats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(stats.FullCompactionTs)},
            {"TabletID", TabletID()});
        
        UpdateBackgroundCompactionQueueMetrics();
    } else {
        YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Enqueue] Skipped or already exists with at schemeshard",
            {"shard", shardIdx},
            {"partCount", stats.PartCount},
            {"rowCount", stats.RowCount},
            {"searchHeight", stats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(stats.FullCompactionTs)},
            {"TabletID", TabletID()});
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
            YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Removed with borrowed parts at schemeshard",
                {"shard", shardIdx},
                {"TabletID", TabletID()});
        }
        return;
    }

    if (newStats.HasLoanedData) {
        if (RemoveBackgroundCompaction(shardIdx)) {
            YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Removed with loaned parts at schemeshard",
                {"shard", shardIdx},
                {"TabletID", TabletID()});
        }
        return;
    }

    TShardCompactionInfo info(shardIdx, newStats);
    if (BackgroundCompactionQueue->Update(info)) {
        YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Updated with at schemeshard",
            {"shard", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"TabletID", TabletID()});
    } else if (BackgroundCompactionQueue->Enqueue(std::move(info))) {
        YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Enqueued with at schemeshard",
            {"shard", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"TabletID", TabletID()});
    } else {
        YDB_LOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Skipped with at schemeshard",
            {"shard", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"TabletID", TabletID()});
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
