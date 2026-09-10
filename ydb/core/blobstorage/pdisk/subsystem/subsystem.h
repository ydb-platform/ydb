#pragma once

#include <ydb/library/actors/core/subsystem.h>
#include <util/generic/ptr.h>
#include <util/system/types.h>

namespace NActors {
    struct TActorContext;
}

namespace NKikimr {
    struct TPDiskConfig;

    namespace NPDisk {
        struct TMainKey;
    }

    class IPDiskSubsystem : public NActors::ISubSystem {
    public:
        // Called from the owning NodeWarden actor. Registers the PDisk local service;
        // initialization and restart completion use the existing actor protocol.
        virtual void Start(const NActors::TActorContext& ctx, ui32 pdiskId,
            const TIntrusivePtr<TPDiskConfig>& config, const NPDisk::TMainKey& mainKey,
            ui32 poolId, ui32 nodeId) = 0;
    };
}
