#include "subsystem.h"

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {

void InstallBlobStorageSubsystem(NActors::TActorSystemSetup& setup,
        std::unique_ptr<IBlobStorageSubsystem> subsystem) {
    Y_ABORT_UNLESS(subsystem);
    Y_ABORT_UNLESS(!NActors::GetSubSystem<IBlobStorageSubsystem>(setup.SubSystems),
        "BlobStorage subsystem is already installed");
    subsystem->Prepare(setup);
    setup.RegisterSubSystem<IBlobStorageSubsystem>(std::move(subsystem));
}

} // namespace NKikimr
