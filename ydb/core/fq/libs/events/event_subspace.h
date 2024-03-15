#pragma once

#include <ydb/library/actors/core/events.h>

namespace NFq {

constexpr ui32 ES_YQ = 4213; // Must be compatible with the values defined in ydb/core/base/events.h.

// Declares YQ event subspace that can contain up to 512 events.
constexpr ui32 YqEventSubspaceBegin(ui32 subspace) {
    return EventSpaceBegin(ES_YQ) + 512 * subspace;
}

constexpr ui32 YqEventSubspaceEnd(ui32 subspace) {
    return EventSpaceBegin(ES_YQ) + 512 * (subspace + 1);
}

struct TYqEventSubspace {
    enum : ui32 {
        ControlPlane,
        CheckpointCoordinator,
        CheckpointStorage,
        MetastorageProxy,
        ConfigUpdater,
        ControlPlaneStorage,
        ControlPlaneProxy,
        AuditService,
        TestConnection,
        InternalService,
        QuotaService,
        RateLimiter,
        ControlPlaneConfig,
        YdbCompute,
        TableOverFq,

        SubspacesEnd,
    };

    static_assert(YqEventSubspaceBegin(SubspacesEnd) <= EventSpaceEnd(ES_YQ), "All YQ events must be in YQ event space");
};

} // namespace NFq
