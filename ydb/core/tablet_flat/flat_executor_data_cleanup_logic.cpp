#include "flat_executor_data_cleanup_logic.h"

namespace NKikimr::NTabletFlatExecutor {

TDataCleanupLogic::TDataCleanupLogic(IOps* ops, IExecutor* executor, ITablet* owner, NUtil::ILogger* logger)
    : Ops(ops)
    , Executor(executor)
    , Owner(owner)
    , Logger(logger)
{}

bool TDataCleanupLogic::TryStartCleanup(const THashMap<ui32, TGCTime>& commitedGcBarriers) {
    if (State == EDataCleanupState::Idle) {
        if (auto logl = Logger->Log(ELnLev::Info)) {
            logl << "TDataCleanupLogic: Starting DataCleanup for tablet with id " << Owner->TabletID();
        }
        for (const auto& [channel, barrier] : commitedGcBarriers) {
            // init WriteEdge to the latest known commited GC barrier,
            // so we won't wait infinitly if there is no subsequent writes
            ChannelsGCInfo[channel] = {barrier, barrier};
        }
        State = EDataCleanupState::PendingCompaction;
        return true;
    } else {
        if (auto logl = Logger->Log(ELnLev::Info)) {
            logl << "TDataCleanupLogic: schedule next DataCleanup for tablet with id " << Owner->TabletID();
        }
        StartNextCleanup = true;
        return false;
    }
}

void TDataCleanupLogic::OnCompactionPrepared(ui32 tableId, ui64 compactionId) {
    Y_ABORT_UNLESS(State == EDataCleanupState::PendingCompaction);
    CompactingTables[tableId] = {tableId, compactionId};
}

void TDataCleanupLogic::WaitCompaction() {
    Y_ABORT_UNLESS(State == EDataCleanupState::PendingCompaction);
    if (CompactingTables.empty()) {
        State = EDataCleanupState::PendingFirstSnapshot;
    } else {
        State = EDataCleanupState::WaitCompaction;
    }
}

void TDataCleanupLogic::OnCompleteCompaction(
    ui32 generation,
    ui32 step,
    ui32 tableId,
    const TFinishedCompactionInfo& finishedCompactionInfo,
    const TGCBlobDelta& gcDelta)
{
    if (State != EDataCleanupState::WaitCompaction) {
        return;
    }

    if (auto it = CompactingTables.find(tableId); it != CompactingTables.end()) {
        if (finishedCompactionInfo.Edge >= it->second.CompactionId) {
            UpdateWriteEdges(TGCTime(generation, step), gcDelta);
            CompactingTables.erase(it);
        }
    }
    if (CompactingTables.empty()) {
        State = EDataCleanupState::PendingFirstSnapshot;
    }
}

bool TDataCleanupLogic::NeedLogSnaphot() {
    switch (State) {
        case EDataCleanupState::PendingFirstSnapshot:
        case EDataCleanupState::PendingSecondSnapshot:
            return true;
        default:
            return false;
    }
}

void TDataCleanupLogic::OnMakeLogSnapshot(ui32 generation, ui32 step, const TGCBlobDelta& gcDelta) {
    switch (State) {
        case EDataCleanupState::PendingFirstSnapshot: {
            FirstLogSnaphotStep = step;
            UpdateWriteEdges(TGCTime(generation, step), gcDelta);
            State = EDataCleanupState::WaitFirstSnapshot;
            break;
        }
        case EDataCleanupState::PendingSecondSnapshot: {
            SecondLogSnaphotStep = step;
            State = EDataCleanupState::WaitSecondSnapshot;
            break;
        }
        default: {
            break;
        }
    }
}

void TDataCleanupLogic::OnSnapshotCommited(ui32 generation, ui32 step) {
    switch (State) {
        case EDataCleanupState::WaitFirstSnapshot: {
            if (FirstLogSnaphotStep <= step) {
                State = EDataCleanupState::PendingSecondSnapshot;
            }
            break;
        }
        case EDataCleanupState::WaitSecondSnapshot: {
            if (SecondLogSnaphotStep <= step) {
                Ops->Send(Owner->Tablet(), new TEvTablet::TEvGcForStepAckRequest(generation, FirstLogSnaphotStep));
                if (TabletGCCompleted()) {
                    State = EDataCleanupState::WaitLogGC;
                } else {
                    State = EDataCleanupState::WaitAllGCs;
                }
            }
            break;
        }
        default: {
            break;
        }
    }
}

void TDataCleanupLogic::OnCollectedGarbage(ui32 channel, TGCTime commitedGcBarrier, const TActorContext& ctx) {
    switch (State) {
        case EDataCleanupState::WaitAllGCs: {
            UpdateTabletGC(channel, commitedGcBarrier);
            if (TabletGCCompleted()) {
                State = EDataCleanupState::WaitLogGC;
            }
            break;
        }
        case EDataCleanupState::WaitTabletGC: {
            UpdateTabletGC(channel, commitedGcBarrier);
            if (TabletGCCompleted()) {
                CompleteDataCleanup(ctx);
            }
            break;
        }
        default: {
            break;
        }
    }
}

void TDataCleanupLogic::OnGcForStepAckResponse(ui32 step, const TActorContext& ctx) {
    switch (State) {
        case EDataCleanupState::WaitAllGCs: {
            if (FirstLogSnaphotStep <= step) {
                State = EDataCleanupState::WaitTabletGC;
            }
            break;
        }
        case EDataCleanupState::WaitLogGC: {
            if (FirstLogSnaphotStep <= step) {
                CompleteDataCleanup(ctx);
            }
            break;
        }
        default: {
            break;
        }
    }
}

bool TDataCleanupLogic::NeedGC(TGCTime releasedBarrier, TGCTime activeBarrier) {
    switch (State) {
        case EDataCleanupState::PendingSecondSnapshot:
        case EDataCleanupState::WaitSecondSnapshot:
        case EDataCleanupState::WaitAllGCs:
        case EDataCleanupState::WaitTabletGC: {
            bool needGC = false;
            for (const auto& [_, info] : ChannelsGCInfo) {
                needGC = needGC || (
                    info.CommitedGcBarrier < info.WriteEdge &&
                    releasedBarrier <= info.WriteEdge &&
                    info.WriteEdge <= activeBarrier
                );
            }
            return needGC;
        }
        default: {
            return false;
        }
    }
}

void TDataCleanupLogic::CompleteDataCleanup(const TActorContext& ctx) {
    Owner->DataCleanupComplete(ctx);
    if (auto logl = Logger->Log(ELnLev::Info)) {
        logl << "TDataCleanupLogic: DataCleanup finished for tablet with id " << Owner->TabletID();
    }
    State = EDataCleanupState::Idle;
    ChannelsGCInfo.clear();
    if (StartNextCleanup) {
        StartNextCleanup = false;
        Executor->CleanupData();
    }
}

bool TDataCleanupLogic::TabletGCCompleted() {
    for (const auto& [_, info] : ChannelsGCInfo) {
        if (info.CommitedGcBarrier < info.WriteEdge) {
            return false;
        }
    }
    return true;
}

void TDataCleanupLogic::UpdateTabletGC(ui32 channel, TGCTime commitedGcBarrier) {
    if (auto* info = ChannelsGCInfo.FindPtr(channel)) {
        info->CommitedGcBarrier = commitedGcBarrier;
    }
}

void TDataCleanupLogic::UpdateWriteEdges(TGCTime commitTime, const TGCBlobDelta& gcDelta) {
    for (const auto& blobId : gcDelta.Created) {
        auto& info = ChannelsGCInfo[blobId.Channel()];
        info.WriteEdge = Max(info.WriteEdge, TGCTime(blobId.Generation(), blobId.Step()));
    }
    for (const auto& blobId : gcDelta.Deleted) {
        auto& info = ChannelsGCInfo[blobId.Channel()];
        info.WriteEdge = Max(info.WriteEdge, commitTime);
    }
}

} // namespace NKikimr::NTabletFlatExecutor
