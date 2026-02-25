#include "schemeshard_forced_compaction.h"

#include "schemeshard_impl.h"

namespace NKikimr::NSchemeShard {

void TSchemeShard::AddForcedCompaction(
    const TForcedCompactionInfo::TPtr& forcedCompactionInfo)
{
    ForcedCompactions[forcedCompactionInfo->Id] = forcedCompactionInfo;
    ForcedCompactionsByTime.insert(std::make_pair(forcedCompactionInfo->StartTime, forcedCompactionInfo->Id));
    if (forcedCompactionInfo->State == TForcedCompactionInfo::EState::InProgress) {
        InProgressForcedCompactionsByTable[forcedCompactionInfo->TablePathId] = forcedCompactionInfo;
        ForcedCompactionTablesQueue.Enqueue(forcedCompactionInfo->TablePathId);
    } else if (forcedCompactionInfo->State == TForcedCompactionInfo::EState::Cancelling) {
        CancellingForcedCompactions.emplace_back(forcedCompactionInfo);
    }
}

void TSchemeShard::AddForcedCompactionShard(
    const TShardIdx& shardId,
    const TForcedCompactionInfo::TPtr& forcedCompactionInfo)
{
    ForcedCompactionShardsByTable[forcedCompactionInfo->TablePathId].Enqueue(shardId);
    if (forcedCompactionInfo->State == TForcedCompactionInfo::EState::InProgress) {
        InProgressForcedCompactionsByShard[shardId] = forcedCompactionInfo;
    }
}

void TSchemeShard::PersistForcedCompactionState(NIceDb::TNiceDb& db, const TForcedCompactionInfo& info) {
    db.Table<Schema::ForcedCompactions>().Key(info.Id).Update(
        NIceDb::TUpdate<Schema::ForcedCompactions::State>(static_cast<ui8>(info.State)),
        NIceDb::TUpdate<Schema::ForcedCompactions::TableOwnerId>(info.TablePathId.OwnerId),
        NIceDb::TUpdate<Schema::ForcedCompactions::TableLocalId>(info.TablePathId.LocalPathId),
        NIceDb::TUpdate<Schema::ForcedCompactions::Cascade>(info.Cascade),
        NIceDb::TUpdate<Schema::ForcedCompactions::MaxShardsInFlight>(info.MaxShardsInFlight),
        NIceDb::TUpdate<Schema::ForcedCompactions::StartTime>(info.StartTime.Seconds()),
        NIceDb::TUpdate<Schema::ForcedCompactions::EndTime>(info.EndTime.Seconds()),
        NIceDb::TUpdate<Schema::ForcedCompactions::TotalShardCount>(info.TotalShardCount),
        NIceDb::TUpdate<Schema::ForcedCompactions::DoneShardCount>(info.DoneShardCount),
        NIceDb::TUpdate<Schema::ForcedCompactions::SubdomainOwnerId>(info.SubdomainPathId.OwnerId),
        NIceDb::TUpdate<Schema::ForcedCompactions::SubdomainLocalId>(info.SubdomainPathId.LocalPathId)
    );

    if (info.UserSID) {
        db.Table<Schema::ForcedCompactions>().Key(info.Id).Update(
            NIceDb::TUpdate<Schema::ForcedCompactions::UserSID>(*info.UserSID)
        );
    }
}

void TSchemeShard::PersistForcedCompactionForget(NIceDb::TNiceDb& db, const TForcedCompactionInfo& info) {
    db.Table<Schema::ForcedCompactions>().Key(info.Id).Delete();
}

void TSchemeShard::PersistForcedCompactionShards(NIceDb::TNiceDb& db, const TForcedCompactionInfo& info, const TVector<TShardIdx>& shardsToCompact) {
    for (const auto& shardId : shardsToCompact) {
        db.Table<Schema::WaitingForcedCompactionShards>().Key(shardId.GetOwnerId(), shardId.GetLocalId()).Update(
            NIceDb::TUpdate<Schema::WaitingForcedCompactionShards::ForcedCompactionId>(info.Id)
        );
    }
}

void TSchemeShard::PersistForcedCompactionDoneShard(NIceDb::TNiceDb& db, const TShardIdx& shardId) {
    db.Table<Schema::WaitingForcedCompactionShards>().Key(shardId.GetOwnerId(), shardId.GetLocalId()).Delete();
}

void TSchemeShard::FromForcedCompactionInfo(NKikimrForcedCompaction::TForcedCompaction& compaction, const TForcedCompactionInfo& info) {
    compaction.SetId(info.Id);

    if (info.StartTime != TInstant::Zero()) {
        *compaction.MutableStartTime() = SecondsToProtoTimeStamp(info.StartTime.Seconds());
    }
    if (info.EndTime != TInstant::Zero()) {
        *compaction.MutableEndTime() = SecondsToProtoTimeStamp(info.EndTime.Seconds());
    }

    if (info.UserSID) {
        compaction.SetUserSID(*info.UserSID);
    }

    TPath table = TPath::Init(info.TablePathId, this);
    compaction.MutableSettings()->set_source_path(table.PathString());
    compaction.MutableSettings()->set_cascade(info.Cascade);
    compaction.MutableSettings()->set_max_shards_in_flight(info.MaxShardsInFlight);

    switch (info.State) {
        case TForcedCompactionInfo::EState::InProgress:
        case TForcedCompactionInfo::EState::Cancelling:
            compaction.SetState(Ydb::Table::CompactState::STATE_IN_PROGRESS);
            compaction.SetProgress(info.CalcProgress());
            break;
        case TForcedCompactionInfo::EState::Done:
            compaction.SetState(Ydb::Table::CompactState::STATE_DONE);
            compaction.SetProgress(100.0);
            break;
        case TForcedCompactionInfo::EState::Cancelled:
            compaction.SetState(Ydb::Table::CompactState::STATE_CANCELLED);
            compaction.SetProgress(info.CalcProgress());
            break;
        case TForcedCompactionInfo::EState::Invalid:
            compaction.SetState(Ydb::Table::CompactState::STATE_UNSPECIFIED);
            compaction.SetProgress(0.0);
            break;
    }
}

void TSchemeShard::CompleteForcedCompactionForShard(const TShardIdx& shardIdx, const TActorContext &ctx) {
    auto compactionPtr = InProgressForcedCompactionsByShard.FindPtr(shardIdx);
    if (!compactionPtr) {
        return;
    }
    auto compaction = *compactionPtr;
    auto* shardsQueue = ForcedCompactionShardsByTable.FindPtr(compaction->TablePathId); // TODO: check all table when cascade = true

    if (compaction->ShardsInFlight.erase(shardIdx)
        || shardsQueue && shardsQueue->Remove(shardIdx))
    {
        DoneShardsToPersist.emplace_back(shardIdx, compaction);
    }

    const auto now = ctx.Now();
    bool compactionCompleted = (!shardsQueue || shardsQueue->Empty()) && compaction->ShardsInFlight.empty();
    if (compactionCompleted
        || DoneShardsToPersist.size() >= ForcedCompactionPersistBatchSize
        || now - ForcedCompactionProgressStartTime > ForcedCompactionPersistBatchMaxTime)
    {
        ForcedCompactionProgressStartTime = now;
        Execute(CreateTxProgressForcedCompaction());
    }
    ProcessForcedCompactionQueues();
}

void TSchemeShard::ProcessForcedCompactionQueues() {
    // try enqueue shards from multiple tables fairly
    auto initialQueueSize = ForcedCompactionTablesQueue.Size();
    THashSet<TPathId> tablesWithoutCandidates;
    while (!ForcedCompactionTablesQueue.Empty() && tablesWithoutCandidates.size() < initialQueueSize) {
        auto tablePathId = ForcedCompactionTablesQueue.Front();
        auto& compaction = InProgressForcedCompactionsByTable.at(tablePathId);
        auto& shards = ForcedCompactionShardsByTable.at(tablePathId);
        if (!shards.Empty() && compaction->MaxShardsInFlight > compaction->ShardsInFlight.size()) {
            const auto& shardIdx = shards.Front();
            EnqueueForcedCompaction(shards.Front());
            compaction->ShardsInFlight.insert(shardIdx);
            shards.PopFront();
        }
        if (shards.Empty()) {
            tablesWithoutCandidates.insert(tablePathId);
            ForcedCompactionShardsByTable.erase(tablePathId);
            ForcedCompactionTablesQueue.PopFront();
        } else {
            if (compaction->MaxShardsInFlight <= compaction->ShardsInFlight.size()) {
                tablesWithoutCandidates.insert(tablePathId);
            }
            ForcedCompactionTablesQueue.PopFrontToBack();
        }
    }
}

void TSchemeShard::Handle(TEvForcedCompaction::TEvCreateRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCreateForcedCompaction(ev), ctx);
}

void TSchemeShard::Handle(TEvForcedCompaction::TEvGetRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxGetForcedCompaction(ev), ctx);
}

void TSchemeShard::Handle(TEvForcedCompaction::TEvCancelRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxCancelForcedCompaction(ev), ctx);
}

void TSchemeShard::Handle(TEvForcedCompaction::TEvForgetRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxForgetForcedCompaction(ev), ctx);
}

void TSchemeShard::Handle(TEvForcedCompaction::TEvListRequest::TPtr& ev, const TActorContext& ctx) {
    Execute(CreateTxListForcedCompaction(ev), ctx);
}

NOperationQueue::EStartStatus TSchemeShard::StartForcedCompaction(const TShardIdx& shardIdx) {
    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[ForcedCompaction] [Start] Failed to resolve shard info "
            "for forced compaction# " << shardIdx
            << " at schemeshard# " << TabletID());

        CompleteForcedCompactionForShard(shardIdx, ctx);
        return NOperationQueue::EStartStatus::EOperationRemove;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[ForcedCompaction] [Start] Compacting "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup in# " << ForcedCompactionQueue->GetWakeupDelta()
        << ", rate# " << ForcedCompactionQueue->GetRate()
        << ", in queue# " << ForcedCompactionQueue->Size() << " shards"
        << ", running# " << ForcedCompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());

    std::unique_ptr<TEvDataShard::TEvCompactTable> request(
        new TEvDataShard::TEvCompactTable(pathId.OwnerId, pathId.LocalPathId));
    request->Record.SetCompactBorrowed(true);

    PipeClientCache->Send(
        ctx,
        ui64(datashardId),
        request.release(),
        static_cast<ui64>(ECompactionType::Forced));

    return NOperationQueue::EStartStatus::EOperationRunning;
}

void TSchemeShard::HandleForcedCompactionResult(TEvDataShard::TEvCompactTableResult::TPtr &ev, const TActorContext &ctx) {
    const auto& record = ev->Get()->Record;
    // backward compatibility for 0 cookie
    // forced compaction uses CompactBorrowed = true, so this ev definitely not from forced compaction, just ignore it
    if (record.GetStatus() == NKikimrTxDataShard::TEvCompactTableResult::BORROWED) {
        return;
    }

    const TTabletId tabletId(record.GetTabletId());
    const TShardIdx shardIdx = GetShardIdx(tabletId);

    auto pathId = TPathId(
        record.GetPathId().GetOwnerId(),
        record.GetPathId().GetLocalId());

    if (ForcedCompactionQueue) {
        auto duration = ForcedCompactionQueue->OnDone(shardIdx);
        LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[ForcedCompaction] [Finished] Compaction completed "
            "for pathId# " << pathId << ", datashard# " << tabletId
            << ", shardIdx# " << shardIdx
            << " in# " << duration.MilliSeconds() << " ms, with status# " << (int)record.GetStatus()
            << " at schemeshard " << TabletID());
    }

    CompleteForcedCompactionForShard(shardIdx, ctx);
}

void TSchemeShard::EnqueueForcedCompaction(const TShardIdx& shardIdx) {
    if (!ForcedCompactionQueue)
        return;

    auto ctx = ActorContext();

    if (ForcedCompactionQueue->Enqueue(shardIdx)) {
        LOG_TRACE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD,
            "[ForcedCompaction] [Enqueue] Enqueued shard# " << shardIdx << " at schemeshard " << TabletID());
    }
}

void TSchemeShard::OnForcedCompactionTimeout(const TShardIdx& shardIdx) {
    auto ctx = ActorContext();

    auto it = ShardInfos.find(shardIdx);
    if (it == ShardInfos.end()) {
        LOG_WARN_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[ForcedCompaction] [Timeout] Failed to resolve shard info "
            "for timeout forced compaction# " << shardIdx
            << " at schemeshard# " << TabletID());
        return;
    }

    const auto& datashardId = it->second.TabletID;
    const auto& pathId = it->second.PathId;

    LOG_INFO_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[ForcedCompaction] [Timeout] Compaction timeouted "
        "for pathId# " << pathId << ", datashard# " << datashardId
        << ", next wakeup in# " << ForcedCompactionQueue->GetWakeupDelta()
        << ", in queue# " << ForcedCompactionQueue->Size() << " shards"
        << ", running# " << ForcedCompactionQueue->RunningSize() << " shards"
        << " at schemeshard " << TabletID());
    

}

} // namespace NKikimr::NSchemeShard
