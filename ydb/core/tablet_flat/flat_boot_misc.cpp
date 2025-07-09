#include "flat_executor_bootlogic.h"
#include "flat_boot_stages.h"
#include "flat_boot_back.h"
#include "flat_exec_commit_mgr.h"
#include "logic_snap_main.h"
#include "flat_executor_txloglogic.h"

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBoot {

void TStages::FinalizeLeaderLogics(TResult &result, TSteppedCookieAllocatorFactory &steppedCookieAllocatorFactory)
{
    using EIdx = TCookie::EIdx;

    auto *waste = Back->Waste.Get();

    result.CommitManager = new TCommitManager(steppedCookieAllocatorFactory, waste, result.GcLogic.Get());
    result.Snap = new TLogicSnap(steppedCookieAllocatorFactory.Sys(EIdx::SnapLz4), waste, Back->Snap);
    result.Redo = new TLogicRedo(steppedCookieAllocatorFactory.Sys(EIdx::RedoLz4), result.CommitManager.Get(), Back->Redo);
}

}
}
}
