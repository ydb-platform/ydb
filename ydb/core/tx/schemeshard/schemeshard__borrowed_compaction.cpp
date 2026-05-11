#include "schemeshard_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT NKikimrServices::FLAT_TX_SCHEMESHARD

namespace NKikimr::NSchemeShard {

NOperationQueue::EStartStatus TSchemeShard::StartBorrowedCompaction(const TShardIdx& shardIdx) {
    UpdateBorrowedCompactionQueueMetrics();

    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        YDBLOG_CTX_WARN(ctx, " at schemeshard# ",
            {"#_num_0", "Unable to resolve shard info "             "for borrowed compaction# "},
            {"compaction", shardIdx},
            {"schemeshard", TabletID()});

        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    YDBLOG_CTX_INFO(ctx, ", datashard# , next wakeup# , rate# , in queue#  shards, running#  shards at schemeshard ",
        {"#_num_0", "RunBorrowedCompaction "         "for pathId# "},
        {"pathId", pathId},
        {"datashard", datashardId},
        {"wakeup", BorrowedCompactionQueue->GetWakeupDelta()},
        {"rate", BorrowedCompactionQueue->GetRate()},
        {"queue", BorrowedCompactionQueue->Size()},
        {"running", BorrowedCompactionQueue->RunningSize()},
        {"#_TabletID()", TabletID()});

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
        YDBLOG_CTX_WARN(ctx, " at schemeshard# ",
            {"#_num_0", "Unable to resolve shard info "             "for timeout borrowed compaction# "},
            {"compaction", shardIdx},
            {"schemeshard", TabletID()});
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    YDBLOG_CTX_INFO(ctx, ", datashard# , next wakeup# , in queue#  shards, running#  shards at schemeshard ",
        {"#_num_0", "Borrowed compaction timeout "         "for pathId# "},
        {"pathId", pathId},
        {"datashard", datashardId},
        {"wakeup", BorrowedCompactionQueue->GetWakeupDelta()},
        {"queue", BorrowedCompactionQueue->Size()},
        {"running", BorrowedCompactionQueue->RunningSize()},
        {"#_TabletID()", TabletID()});

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
        YDBLOG_CTX_TRACE(ctx, "Borrowed compaction enqueued shard#  at schemeshard ",
            {"shard", shardIdx},
            {"#_TabletID()", TabletID()});
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
        YDBLOG_CTX_WARN(ctx, ", datashard#  in# , next wakeup# , rate# , in queue#  shards, running#  shards at schemeshard ",
            {"#_num_0", "Finished borrowed compaction of unknown shard "             "for pathId# "},
            {"pathId", pathId},
            {"datashard", tabletId},
            {"in", duration.MilliSeconds()},
            {"wakeup", BorrowedCompactionQueue->GetWakeupDelta()},
            {"rate", BorrowedCompactionQueue->GetRate()},
            {"queue", BorrowedCompactionQueue->Size()},
            {"running", BorrowedCompactionQueue->RunningSize()},
            {"#_TabletID()", TabletID()});
    } else {
        YDBLOG_CTX_INFO(ctx, ", datashard# , shardIdx#  in# , next wakeup# , rate# , in queue#  shards, running#  shards at schemeshard ",
            {"#_num_0", "Finished borrowed compaction "             "for pathId# "},
            {"pathId", pathId},
            {"datashard", tabletId},
            {"shardIdx", shardIdx},
            {"in", duration.MilliSeconds()},
            {"wakeup", BorrowedCompactionQueue->GetWakeupDelta()},
            {"rate", BorrowedCompactionQueue->GetRate()},
            {"queue", BorrowedCompactionQueue->Size()},
            {"running", BorrowedCompactionQueue->RunningSize()},
            {"#_TabletID()", TabletID()});
    }

    RunningBorrowedCompactions.erase(shardIdx);

    TabletCounters->Cumulative()[COUNTER_BORROWED_COMPACTION_OK].Increment(1);
    UpdateBorrowedCompactionQueueMetrics();
}

} // NKikimr::NSchemeShard
