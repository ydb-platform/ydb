#include "setup.h"

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {

void ConfigureBlobStorage(NActors::TTestActorRuntime& runtime, TBlobStorageSubsystemFactory factory) {
    Y_ABORT_UNLESS(factory);
    auto previous = std::move(runtime.SetupNodeSubSystems);
    runtime.SetupNodeSubSystems = [factory = std::move(factory), previous = std::move(previous)](
            ui32 nodeIndex, NActors::TActorSystemSetup* setup) {
        if (previous) {
            previous(nodeIndex, setup);
        }
        InstallBlobStorageSubsystem(*setup, factory(nodeIndex));
    };
}

}
