#include "flat_executor_vacuum_logic.h"

namespace NKikimr::NTabletFlatExecutor {

TVacuumLogic::TVacuumLogic(IOps* ops, IExecutor* executor, ITablet* owner, NUtil::ILogger* logger, TExecutorGCLogic* gcLogic)
    : Ops(ops)
    , Executor(executor)
    , Owner(owner)
    , Logger(logger)
    , GcLogic(gcLogic)
{}

bool TVacuumLogic::TryStartVacuum(ui64 vacuumGeneration, const TActorContext& ctx) {
    switch (State) {
        case EVacuumState::Idle: {
            if (CurrentVacuumGeneration >= vacuumGeneration) {
                if (auto logl = Logger->Log(ELnLev::Info)) {
                    logl << "TVacuumLogic: Vacuum for tablet with id " << Owner->TabletID()
                        << " had already completed for generation " << vacuumGeneration
                        << ", current Vacuum generation: " << CurrentVacuumGeneration;
                }
                // repeat VacuumComplete callback
                CompleteVacuum(ctx);
                return false;
            } else {
                CurrentVacuumGeneration = vacuumGeneration;
                if (auto logl = Logger->Log(ELnLev::Info)) {
                    logl << "TVacuumLogic: Starting Vacuum for tablet with id " << Owner->TabletID()
                        << ", current Vacuum generation: " << CurrentVacuumGeneration;
                }
                ChangeState(EVacuumState::PendingCompaction);
                return true;
            }
            break;
        }
        default: { // Vacuum in progress
            if (vacuumGeneration > CurrentVacuumGeneration) {
                NextVacuumGeneration = Max(vacuumGeneration, NextVacuumGeneration);
                if (auto logl = Logger->Log(ELnLev::Info)) {
                    logl << "TVacuumLogic: schedule next Vacuum for tablet with id " << Owner->TabletID()
                        << ", current Vacuum generation: " << CurrentVacuumGeneration
                        << ", next Vacuum generation: " << NextVacuumGeneration;
                }
                return false;
            } else {
                // more recent Vacuum in progress, so just ignore osolete generation
                return false;
            }
        }
    }
}

void TVacuumLogic::OnCompactionPrepared(ui32 tableId, ui64 compactionId) {
    if (auto logl = Logger->Log(ELnLev::Dbg03)) {
        logl << "TVacuumLogic: OnCompactionPrepared"
            << " in tablet with id " << Owner->TabletID()
            << ", state: " << State
            << ", current Vacuum generation: " << CurrentVacuumGeneration;
    }
    Y_ENSURE(State == EVacuumState::PendingCompaction);
    CompactingTables[tableId] = {tableId, compactionId};
}

void TVacuumLogic::WaitCompaction() {
    Y_ENSURE(State == EVacuumState::PendingCompaction);
    if (CompactingTables.empty()) {
        ChangeState(EVacuumState::PendingFirstSnapshot);
    } else {
        ChangeState(EVacuumState::WaitCompaction);
    }
}

void TVacuumLogic::OnCompleteCompaction(
    ui32 tableId,
    const TFinishedCompactionInfo& finishedCompactionInfo)
{
    if (auto logl = Logger->Log(ELnLev::Dbg03)) {
        logl << "TVacuumLogic: OnCompleteCompaction"
            << " in tablet with id " << Owner->TabletID()
            << ", state: " << State
            << ", current Vacuum generation: " << CurrentVacuumGeneration;
    }
    if (State != EVacuumState::WaitCompaction) {
        return;
    }

    if (auto it = CompactingTables.find(tableId); it != CompactingTables.end()) {
        if (finishedCompactionInfo.Edge >= it->second.CompactionId) {
            CompactingTables.erase(it);
        }
    }
    if (CompactingTables.empty()) {
        ChangeState(EVacuumState::PendingFirstSnapshot);
    }
}

bool TVacuumLogic::NeedLogSnaphot() {
    switch (State) {
        case EVacuumState::PendingFirstSnapshot:
        case EVacuumState::PendingSecondSnapshot:
            return true;
        default:
            return false;
    }
}

void TVacuumLogic::OnMakeLogSnapshot(ui32 generation, ui32 step) {
    if (auto logl = Logger->Log(ELnLev::Dbg03)) {
        logl << "TVacuumLogic: OnMakeLogSnapshot"
            << " in tablet with id " << Owner->TabletID()
            << ", state: " << State
            << ", current Vacuum generation: " << CurrentVacuumGeneration;
    }
    switch (State) {
        case EVacuumState::PendingFirstSnapshot: {
            FirstLogSnaphotStep = TGCTime(generation, step);
            ChangeState(EVacuumState::WaitFirstSnapshot);
            break;
        }
        case EVacuumState::PendingSecondSnapshot: {
            SecondLogSnaphotStep = TGCTime(generation, step);
            ChangeState(EVacuumState::WaitSecondSnapshot);
            break;
        }
        default: {
            break;
        }
    }
}

void TVacuumLogic::OnSnapshotCommited(ui32 generation, ui32 step) {
    if (auto logl = Logger->Log(ELnLev::Dbg03)) {
        logl << "TVacuumLogic: OnSnapshotCommited"
            << " in tablet with id " << Owner->TabletID()
            << ", state: " << State
            << ", current Vacuum generation: " << CurrentVacuumGeneration;
    }
    switch (State) {
        case EVacuumState::WaitFirstSnapshot: {
            if (FirstLogSnaphotStep <= TGCTime(generation, step)) {
                ChangeState(EVacuumState::PendingSecondSnapshot);
            }
            break;
        }
        case EVacuumState::WaitSecondSnapshot: {
            if (SecondLogSnaphotStep <= TGCTime(generation, step)) {
                Ops->Send(Owner->Tablet(), new TEvTablet::TEvGcForStepAckRequest(FirstLogSnaphotStep.Generation, FirstLogSnaphotStep.Step));
                if (GcLogic->HasGarbageBefore(FirstLogSnaphotStep)) {
                    ChangeState(EVacuumState::WaitAllGCs);
                } else {
                    ChangeState(EVacuumState::WaitLogGC);
                }
            }
            break;
        }
        default: {
            break;
        }
    }
}

void TVacuumLogic::OnCollectedGarbage(const TActorContext& ctx) {
    if (auto logl = Logger->Log(ELnLev::Dbg03)) {
        logl << "TVacuumLogic: OnCollectedGarbage"
            << " in tablet with id " << Owner->TabletID()
            << ", state: " << State
            << ", current Vacuum generation: " << CurrentVacuumGeneration;
    }
    switch (State) {
        case EVacuumState::WaitAllGCs: {
            if (!GcLogic->HasGarbageBefore(FirstLogSnaphotStep)) {
                ChangeState(EVacuumState::WaitLogGC);
            }
            break;
        }
        case EVacuumState::WaitTabletGC: {
            if (!GcLogic->HasGarbageBefore(FirstLogSnaphotStep)) {
                CompleteVacuum(ctx);
            }
            break;
        }
        default: {
            break;
        }
    }
}

void TVacuumLogic::OnGcForStepAckResponse(ui32 generation, ui32 step, const TActorContext& ctx) {
    if (auto logl = Logger->Log(ELnLev::Dbg03)) {
        logl << "TVacuumLogic: OnGcForStepAckResponse"
            << " in tablet with id " << Owner->TabletID()
            << ", state: " << State
            << ", current Vacuum generation: " << CurrentVacuumGeneration;
    }
    switch (State) {
        case EVacuumState::WaitAllGCs: {
            if (FirstLogSnaphotStep <= TGCTime(generation, step)) {
                ChangeState(EVacuumState::WaitTabletGC);
            }
            break;
        }
        case EVacuumState::WaitLogGC: {
            if (FirstLogSnaphotStep <= TGCTime(generation, step)) {
                CompleteVacuum(ctx);
            }
            break;
        }
        default: {
            break;
        }
    }
}

bool TVacuumLogic::NeedGC() {
    switch (State) {
        case EVacuumState::PendingSecondSnapshot:
        case EVacuumState::WaitSecondSnapshot:
        case EVacuumState::WaitAllGCs:
        case EVacuumState::WaitTabletGC: {
            return GcLogic->HasGarbageBefore(FirstLogSnaphotStep);
        }
        default: {
            return false;
        }
    }
}

void TVacuumLogic::CompleteVacuum(const TActorContext& ctx) {
    ChangeState(EVacuumState::Idle);
    if (NextVacuumGeneration) {
        Executor->StartVacuum(std::exchange(NextVacuumGeneration, 0));
    } else {
        // report complete only if all planned cleanups completed
        Owner->VacuumComplete(CurrentVacuumGeneration, ctx);
        if (auto logl = Logger->Log(ELnLev::Info)) {
            logl << "TVacuumLogic: Vacuum finished for tablet with id " << Owner->TabletID()
                << ", current Vacuum generation: " << CurrentVacuumGeneration;
        }
    }
}


void TVacuumLogic::ChangeState(EVacuumState to) {
    if (auto logl = Logger->Log(ELnLev::Debug)) {
        logl << "TVacuumLogic: State transition from " << State << " to " << to
            << " in tablet with id " << Owner->TabletID()
            << ", current Vacuum generation: " << CurrentVacuumGeneration;
    }
    State = to;
}

} // namespace NKikimr::NTabletFlatExecutor
