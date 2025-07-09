#pragma once

#include "tablet_flat_executor.h"
#include "flat_executor_gclogic.h"
#include "util_fmt_logger.h"

namespace NKikimr::NTabletFlatExecutor {

class TVacuumLogic {
    enum class EVacuumState {
        Idle,
        PendingCompaction,
        WaitCompaction,
        PendingFirstSnapshot,
        WaitFirstSnapshot,
        PendingSecondSnapshot,
        WaitSecondSnapshot,
        WaitAllGCs,
        WaitTabletGC,
        WaitLogGC,
    };

    struct TVacuumTableInfo {
        ui32 TableId = Max<ui32>();
        ui64 CompactionId = 0;
    };

public:
    using IOps = NActors::IActorOps;
    using IExecutor = NFlatExecutorSetup::IExecutor;
    using ITablet = NFlatExecutorSetup::ITablet;
    using ELnLev = NUtil::ELnLev;

    TVacuumLogic(IOps* ops, IExecutor* executor, ITablet* owner, NUtil::ILogger* logger, TExecutorGCLogic* gcLogic);

    bool TryStartVacuum(ui64 vacuumGeneration, const TActorContext& ctx);
    void OnCompactionPrepared(ui32 tableId, ui64 compactionId);
    void WaitCompaction();
    void OnCompleteCompaction(ui32 tableId, const TFinishedCompactionInfo& finishedCompactionInfo);
    bool NeedLogSnaphot();
    void OnMakeLogSnapshot(ui32 generation, ui32 step);
    void OnSnapshotCommited(ui32 generation, ui32 step);
    void OnCollectedGarbage(const TActorContext& ctx);
    void OnGcForStepAckResponse(ui32 generation, ui32 step, const TActorContext& ctx);
    bool NeedGC();

private:
    void CompleteVacuum(const TActorContext& ctx);

private:
    IOps* Ops;
    IExecutor* Executor;
    ITablet* Owner;
    NUtil::ILogger* const Logger;
    TExecutorGCLogic* const GcLogic;

    ui64 CurrentVacuumGeneration = 0;
    ui64 NextVacuumGeneration = 0;
    EVacuumState State = EVacuumState::Idle;
    THashMap<ui32, TVacuumTableInfo> CompactingTables; // tracks statuses of compaction

    // two subsequent are snapshots required to force GC
    TGCTime FirstLogSnaphotStep;
    TGCTime SecondLogSnaphotStep;
};

} // NKikimr::NTabletFlatExecutor
