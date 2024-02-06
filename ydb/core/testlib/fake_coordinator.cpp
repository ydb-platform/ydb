#include "fake_coordinator.h"
#include "tablet_helpers.h"
#include <ydb/library/dbgtrace/debug_trace.h>

namespace NKikimr {

    void BootFakeCoordinator(TTestActorRuntime& runtime, ui64 coordinatorId, TFakeCoordinator::TState::TPtr state) {
        DBGTRACE("BootFakeCoordinator");
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(coordinatorId, TTabletTypes::Coordinator),
            [=](const TActorId & tablet, TTabletStorageInfo* info) {
                return new TFakeCoordinator(tablet, info, state);
        });

        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot, 1));
            runtime.DispatchEvents(options);
        }
    }
}
