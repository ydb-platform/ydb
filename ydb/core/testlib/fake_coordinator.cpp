#include "fake_coordinator.h"
#include "tablet_helpers.h"

namespace NKikimr {

    void BootFakeCoordinator(TTestActorRuntime& runtime, ui64 coordinatorId, TFakeCoordinator::TState::TPtr state) {
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
