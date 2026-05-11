#include "schemeshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBackgroundCompaction(const TShardCompactionInfo& info) {
    UpdateBackgroundCompactionQueueMetrics();

    auto ctx = ActorContext();

    const auto& shardIdx = info.ShardIdx;
    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        YDBLOG_CTX_WARN(ctx, " at schemeshard# ",
            {"#_num_0", "[BackgroundCompaction] [Start] Failed to resolve shard info "             "for background compaction# "},
            {"compaction", shardIdx},
            {"schemeshard", TabletID()});

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    YDBLOG_CTX_INFO(ctx, ", datashard# , compactionInfo# , next wakeup in# , rate# , in queue#  shards, waiting after compaction#  shards, running#  shards at schemeshard ",
        {"#_num_0", "[BackgroundCompaction] [Start] Compacting "         "for pathId# "},
        {"pathId", pathId},
        {"datashard", datashardId},
        {"compactionInfo", info},
        {"in", BackgroundCompactionQueue->GetWakeupDelta()},
        {"rate", BackgroundCompactionQueue->GetRate()},
        {"queue", BackgroundCompactionQueue->Size()},
        {"compaction", BackgroundCompactionQueue->WaitingSize()},
        {"running", BackgroundCompactionQueue->RunningSize()},
        {"#_TabletID()", TabletID()});

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
        YDBLOG_CTX_WARN(ctx, " at schemeshard# ",
            {"#_num_0", "[BackgroundCompaction] [Timeout] Failed to resolve shard info "             "for timeout background compaction# "},
            {"compaction", shardIdx},
            {"schemeshard", TabletID()});
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    YDBLOG_CTX_INFO(ctx, ", datashard# , compactionInfo# , next wakeup in# , rate# , in queue#  shards, waiting after compaction#  shards, running#  shards at schemeshard ",
        {"#_num_0", "[BackgroundCompaction] [Timeout] Compaction timeouted "         "for pathId# "},
        {"pathId", pathId},
        {"datashard", datashardId},
        {"compactionInfo", info},
        {"in", BackgroundCompactionQueue->GetWakeupDelta()},
        {"rate", BackgroundCompactionQueue->GetRate()},
        {"queue", BackgroundCompactionQueue->Size()},
        {"compaction", BackgroundCompactionQueue->WaitingSize()},
        {"running", BackgroundCompactionQueue->RunningSize()},
        {"#_TabletID()", TabletID()});
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
        YDBLOG_CTX_WARN(ctx, ", datashard#  in#  ms, with status# , next wakeup in# , rate# , in queue#  shards, waiting after compaction#  shards, running#  shards at schemeshard ",
            {"#_num_0", "[BackgroundCompaction] [Finished] Failed to resolve shard info "             "for pathId# "},
            {"pathId", pathId},
            {"datashard", tabletId},
            {"in", duration.MilliSeconds()},
            {"status", (int)record.GetStatus()},
            {"in", BackgroundCompactionQueue->GetWakeupDelta()},
            {"rate", BackgroundCompactionQueue->GetRate()},
            {"queue", BackgroundCompactionQueue->Size()},
            {"compaction", BackgroundCompactionQueue->WaitingSize()},
            {"running", BackgroundCompactionQueue->RunningSize()},
            {"#_TabletID()", TabletID()});
    } else {
        YDBLOG_CTX_INFO(ctx, ", datashard# , shardIdx#  in#  ms, with status# , next wakeup in# , rate# , in queue#  shards, waiting after compaction#  shards, running#  shards at schemeshard ",
            {"#_num_0", "[BackgroundCompaction] [Finished] Compaction completed "             "for pathId# "},
            {"pathId", pathId},
            {"datashard", tabletId},
            {"shardIdx", shardIdx},
            {"in", duration.MilliSeconds()},
            {"status", (int)record.GetStatus()},
            {"in", BackgroundCompactionQueue->GetWakeupDelta()},
            {"rate", BackgroundCompactionQueue->GetRate()},
            {"queue", BackgroundCompactionQueue->Size()},
            {"compaction", BackgroundCompactionQueue->WaitingSize()},
            {"running", BackgroundCompactionQueue->RunningSize()},
            {"#_TabletID()", TabletID()});
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
        YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Enqueue] Skipped shard#  with borrowed parts at schemeshard ",
            {"shard", shardIdx},
            {"#_TabletID()", TabletID()});
        return;
    }

    if (stats.HasLoanedData) {
        YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Enqueue] Skipped shard#  with loaned parts at schemeshard ",
            {"shard", shardIdx},
            {"#_TabletID()", TabletID()});
        return;
    }

    if (BackgroundCompactionQueue->Enqueue(TShardCompactionInfo(shardIdx, stats))) {
        YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Enqueue] Enqueued shard#  with partCount# , rowCount# , searchHeight# , lastFullCompaction#  at schemeshard ",
            {"shard", shardIdx},
            {"partCount", stats.PartCount},
            {"rowCount", stats.RowCount},
            {"searchHeight", stats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(stats.FullCompactionTs)},
            {"#_TabletID()", TabletID()});
        
        UpdateBackgroundCompactionQueueMetrics();
    } else {
        YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Enqueue] Skipped or already exists shard#  with partCount# , rowCount# , searchHeight# , lastFullCompaction#  at schemeshard ",
            {"shard", shardIdx},
            {"partCount", stats.PartCount},
            {"rowCount", stats.RowCount},
            {"searchHeight", stats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(stats.FullCompactionTs)},
            {"#_TabletID()", TabletID()});
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
            YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Removed shard#  with borrowed parts at schemeshard ",
                {"shard", shardIdx},
                {"#_TabletID()", TabletID()});
        }
        return;
    }

    if (newStats.HasLoanedData) {
        if (RemoveBackgroundCompaction(shardIdx)) {
            YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Removed shard#  with loaned parts at schemeshard ",
                {"shard", shardIdx},
                {"#_TabletID()", TabletID()});
        }
        return;
    }

    TShardCompactionInfo info(shardIdx, newStats);
    if (BackgroundCompactionQueue->Update(info)) {
        YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Updated shard#  with partCount# , rowCount# , searchHeight# , lastFullCompaction#  at schemeshard ",
            {"shard", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"#_TabletID()", TabletID()});
    } else if (BackgroundCompactionQueue->Enqueue(std::move(info))) {
        YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Enqueued shard#  with partCount# , rowCount# , searchHeight# , lastFullCompaction#  at schemeshard ",
            {"shard", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"#_TabletID()", TabletID()});
    } else {
        YDBLOG_CTX_TRACE(ctx, "[BackgroundCompaction] [Update] Skipped shard#  with partCount# , rowCount# , searchHeight# , lastFullCompaction#  at schemeshard ",
            {"shard", shardIdx},
            {"partCount", newStats.PartCount},
            {"rowCount", newStats.RowCount},
            {"searchHeight", newStats.SearchHeight},
            {"lastFullCompaction", TInstant::Seconds(newStats.FullCompactionTs)},
            {"#_TabletID()", TabletID()});
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
