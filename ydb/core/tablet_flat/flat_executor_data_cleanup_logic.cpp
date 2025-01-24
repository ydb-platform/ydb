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

void TDataCleanupLogic::OnNoTables(const TActorContext& ctx) {
    Y_ABORT_UNLESS(State == EDataCleanupState::PendingCompaction && CompactingTables.empty());
    CompleteDataCleanup(ctx);
}

void TDataCleanupLogic::OnCompactionPrepared(ui32 tableId, ui64 compactionId) {
    Y_ABORT_UNLESS(State == EDataCleanupState::PendingCompaction);
    CompactingTables[tableId] = {tableId, compactionId};
}

void TDataCleanupLogic::OnCompleteCompaction(
    ui32 generation,
    ui32 step,
    ui32 tableId,
    const TFinishedCompactionInfo& finishedCompactionInfo,
    const TGCBlobDelta& gcDelta)
{
    if (State != EDataCleanupState::PendingCompaction) {
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
    return State == EDataCleanupState::PendingFirstSnapshot && !FirstLogSnaphotStep
        || State == EDataCleanupState::PendingSecondSnapshot && !SecondLogSnaphotStep;
}

void TDataCleanupLogic::OnMakeLogSnapshot(ui32 generation, ui32 step, const TGCBlobDelta& gcDelta) {
    if (State == EDataCleanupState::PendingFirstSnapshot && !FirstLogSnaphotStep) {
        FirstLogSnaphotStep = step;
        UpdateWriteEdges(TGCTime(generation, step), gcDelta);
        if (!GcForStepAckRequested) {
            Ops->Send(Owner->Tablet(), new TEvTablet::TEvGcForStepAckRequest(generation, *FirstLogSnaphotStep));
            GcForStepAckRequested = true;
        }
    } else if (State == EDataCleanupState::PendingSecondSnapshot && !SecondLogSnaphotStep) {
        SecondLogSnaphotStep = step;
    }
}

void TDataCleanupLogic::OnSnapshotCommited(ui32 step, const TActorContext& ctx) {
    if (State == EDataCleanupState::PendingFirstSnapshot && FirstLogSnaphotStep && *FirstLogSnaphotStep <= step) {
        State = EDataCleanupState::PendingSecondSnapshot;
    }
    if (State == EDataCleanupState::PendingSecondSnapshot && SecondLogSnaphotStep && *SecondLogSnaphotStep <= step) {
        State = EDataCleanupState::PendingGCs;
    }
    if (State == EDataCleanupState::PendingGCs && CheckGCsSteps()) {
        CompleteDataCleanup(ctx);
    }
}

void TDataCleanupLogic::OnCollectedGarbage(ui32 channel, TGCTime commitedGcBarrier, const TActorContext& ctx) {
    if (auto* info = ChannelsGCInfo.FindPtr(channel)) {
        info->CommitedGcBarrier = commitedGcBarrier;
    }
    if (State == EDataCleanupState::PendingGCs && CheckGCsSteps()) {
        CompleteDataCleanup(ctx);
    }
}

void TDataCleanupLogic::OnGcForStepAckResponse(ui32 step, const TActorContext& ctx) {
    GcLogSnaphotStep = step;
    if (State == EDataCleanupState::PendingGCs && CheckGCsSteps()) {
        CompleteDataCleanup(ctx);
    }
}

bool TDataCleanupLogic::NeedGC(TGCTime releasedBarrier, TGCTime activeBarrier) {
    if (State == EDataCleanupState::PendingSecondSnapshot || State == EDataCleanupState::PendingGCs) {
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
    return false;
}

void TDataCleanupLogic::CompleteDataCleanup(const TActorContext& ctx) {
    Owner->DataCleanupComplete(ctx);
    if (auto logl = Logger->Log(ELnLev::Info)) {
        logl << "TDataCleanupLogic: DataCleanup finished for tablet with id " << Owner->TabletID();
    }
    State = EDataCleanupState::Idle;
    FirstLogSnaphotStep = Nothing();
    SecondLogSnaphotStep = Nothing();
    ChannelsGCInfo.clear();
    GcLogSnaphotStep = Nothing();
    GcForStepAckRequested = false;
    if (StartNextCleanup) {
        StartNextCleanup = false;
        Executor->CleanupData();
    }
}

bool TDataCleanupLogic::CheckGCsSteps() {
    bool completed = FirstLogSnaphotStep && GcLogSnaphotStep && *FirstLogSnaphotStep <= *GcLogSnaphotStep;
    for (const auto& [_, info] : ChannelsGCInfo) {
        completed = completed && info.WriteEdge <= info.CommitedGcBarrier;
    }
    return completed;
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
