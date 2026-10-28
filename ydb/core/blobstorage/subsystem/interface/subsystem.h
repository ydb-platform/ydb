#pragma once

#include <ydb/library/actors/core/subsystem.h>

namespace NActors {
    struct TActorSystemSetup;
}

namespace NKikimr {

// Selects the node's BlobStorage implementation. Registered actors belong to
// the actor system; externally supplied storage models may outlive that system.
class IBlobStorageSubsystem : public NActors::ISubSystem {
public:
    // Populate LocalServices before the actor system is constructed, so storage
    // services are available when tablet actors start.
    virtual void Prepare(NActors::TActorSystemSetup& setup) = 0;
};

// Installs exactly one implementation under its interface type. Replacing an
// already prepared implementation would leave its actors in LocalServices.
void InstallBlobStorageSubsystem(NActors::TActorSystemSetup& setup,
    std::unique_ptr<IBlobStorageSubsystem> subsystem);

} // namespace NKikimr
