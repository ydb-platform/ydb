#pragma once

#include "tablet_flat_executor.h"
#include "flat_executor_gclogic.h"
#include "util_fmt_logger.h"

namespace NKikimr::NTabletFlatExecutor {

enum class EDataCleanupState {
    Idle,
    PendingCompaction,
    PendingFirstSnapshot,
    PendingSecondSnapshot,
    PendingGCs,
};

struct TCleanupTableInfo {
    ui32 TableId = Max<ui32>();
    ui64 CompactionId = 0;
    bool CompactionCompleted = false;
};

struct TCleanupChannesInfo {
    TGCTime WriteEdge;
    TGCTime CommitedGcBarrier;
};

class TDataCleanupLogic {
public:
    using IOps = NActors::IActorOps;
    using IExecutor = NFlatExecutorSetup::IExecutor;
    using ITablet = NFlatExecutorSetup::ITablet;
    using ELnLev = NUtil::ELnLev;

    TDataCleanupLogic(IOps* ops, IExecutor* executor, ITablet* owner, NUtil::ILogger* Logger);

    bool TryStartCleanup(const THashMap<ui32, TGCTime>& commitedGcBarriers);
    void OnNoTables(const TActorContext& ctx);
    void OnCompactionPrepared(ui32 tableId, ui64 compactionId);
    void OnCompleteCompaction(
        ui32 generation,
        ui32 step,
        ui32 tableId,
        const TFinishedCompactionInfo& finishedCompactionInfo,
        const TGCBlobDelta& gcDelta);
    bool NeedLogSnaphot();
    void OnMakeLogSnapshot(ui32 generation, ui32 step, const TGCBlobDelta& gcDelta);
    void OnSnapshotCommited(ui32 step, const TActorContext& ctx);
    void OnCollectedGarbage(ui32 channel, TGCTime commitedGcBarrier, const TActorContext& ctx);
    void OnGcForStepAckResponse(ui32 step, const TActorContext& ctx);
    bool NeedGC(TGCTime releasedBarrier, TGCTime activeBarrier);

private:
    void CompleteDataCleanup(const TActorContext& ctx);
    bool CheckGCsSteps();
    void UpdateWriteEdges(TGCTime commitTime, const TGCBlobDelta& gcDelta);

private:
    IOps* Ops;
    IExecutor* Executor;
    ITablet* Owner;
    NUtil::ILogger* const Logger;

    EDataCleanupState State = EDataCleanupState::Idle;
    bool StartNextCleanup = false;
    THashMap<ui32, TCleanupTableInfo> Tables; // tracks statuses of compaction

    // tracks commited GC barriers and writes of upcoming compactions and snapshots per channel
    THashMap<ui32, TCleanupChannesInfo> ChannelsGCInfo;

    // two subsequent are snapshots required to force GC
    TMaybe<ui32> FirstLogSnaphotStep = Nothing(); // snapshot requested if not empty
    TMaybe<ui32> SecondLogSnaphotStep = Nothing(); // snapshot requested if not empty
    bool GcForStepAckRequested = false;
    TMaybe<ui32> GcLogSnaphotStep = Nothing();
};

} // NKikimr::NTabletFlatExecutor
