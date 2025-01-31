#include "flat_executor_data_cleanup_logic.h"

namespace NKikimr::NTabletFlatExecutor {

TDataCleanupLogic::TDataCleanupLogic(IOps* ops, IExecutor* executor, ITablet* owner, NUtil::ILogger* logger, TExecutorGCLogic* gcLogic)
    : Ops(ops)
    , Executor(executor)
    , Owner(owner)
    , Logger(logger)
    , GcLogic(gcLogic)
{}

bool TDataCleanupLogic::TryStartCleanup() {
    if (State == EDataCleanupState::Idle) {
        if (auto logl = Logger->Log(ELnLev::Info)) {
            logl << "TDataCleanupLogic: Starting DataCleanup for tablet with id " << Owner->TabletID();
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
    ui32 tableId,
    const TFinishedCompactionInfo& finishedCompactionInfo)
{
    if (State != EDataCleanupState::WaitCompaction) {
        return;
    }

    if (auto it = CompactingTables.find(tableId); it != CompactingTables.end()) {
        if (finishedCompactionInfo.Edge >= it->second.CompactionId) {
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

void TDataCleanupLogic::OnMakeLogSnapshot(ui32 generation, ui32 step) {
    switch (State) {
        case EDataCleanupState::PendingFirstSnapshot: {
            FirstLogSnaphotStep = TGCTime(generation, step);
            State = EDataCleanupState::WaitFirstSnapshot;
            break;
        }
        case EDataCleanupState::PendingSecondSnapshot: {
            SecondLogSnaphotStep = TGCTime(generation, step);
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
            if (FirstLogSnaphotStep <= TGCTime(generation, step)) {
                State = EDataCleanupState::PendingSecondSnapshot;
            }
            break;
        }
        case EDataCleanupState::WaitSecondSnapshot: {
            if (SecondLogSnaphotStep <= TGCTime(generation, step)) {
                Ops->Send(Owner->Tablet(), new TEvTablet::TEvGcForStepAckRequest(FirstLogSnaphotStep.Generation, FirstLogSnaphotStep.Step));
                if (GcLogic->HasGarbageBefore(FirstLogSnaphotStep)) {
                    State = EDataCleanupState::WaitAllGCs;
                } else {
                    State = EDataCleanupState::WaitLogGC;
                }
            }
            break;
        }
        default: {
            break;
        }
    }
}

void TDataCleanupLogic::OnCollectedGarbage(const TActorContext& ctx) {
    switch (State) {
        case EDataCleanupState::WaitAllGCs: {
            if (!GcLogic->HasGarbageBefore(FirstLogSnaphotStep)) {
                State = EDataCleanupState::WaitLogGC;
            }
            break;
        }
        case EDataCleanupState::WaitTabletGC: {
            if (!GcLogic->HasGarbageBefore(FirstLogSnaphotStep)) {
                CompleteDataCleanup(ctx);
            }
            break;
        }
        default: {
            break;
        }
    }
}

void TDataCleanupLogic::OnGcForStepAckResponse(ui32 generation, ui32 step, const TActorContext& ctx) {
    switch (State) {
        case EDataCleanupState::WaitAllGCs: {
            if (FirstLogSnaphotStep <= TGCTime(generation, step)) {
                State = EDataCleanupState::WaitTabletGC;
            }
            break;
        }
        case EDataCleanupState::WaitLogGC: {
            if (FirstLogSnaphotStep <= TGCTime(generation, step)) {
                CompleteDataCleanup(ctx);
            }
            break;
        }
        default: {
            break;
        }
    }
}

bool TDataCleanupLogic::NeedGC() {
    switch (State) {
        case EDataCleanupState::PendingSecondSnapshot:
        case EDataCleanupState::WaitSecondSnapshot:
        case EDataCleanupState::WaitAllGCs:
        case EDataCleanupState::WaitTabletGC: {
            return GcLogic->HasGarbageBefore(FirstLogSnaphotStep);
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
    if (StartNextCleanup) {
        StartNextCleanup = false;
        Executor->CleanupData();
    }
}

} // namespace NKikimr::NTabletFlatExecutor
