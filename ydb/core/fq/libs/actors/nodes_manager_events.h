#pragma once

#include <ydb/core/fq/libs/events/event_subspace.h>

#include <ydb/library/actors/core/events.h>

namespace NFq {

struct TEvNodesManager {
    // Event ids.
    enum EEv : ui32 {
        EvGetNodesRequest = YqEventSubspaceBegin(TYqEventSubspace::NodesManager),
        EvGetNodesResponse,
        EvEnd,
    };

    static_assert(EvEnd <= YqEventSubspaceEnd(TYqEventSubspace::NodesManager), "All events must be in their subspace");

    struct TEvGetNodesRequest : NActors::TEventLocal<TEvGetNodesRequest, EvGetNodesRequest> {
    };

    struct TEvGetNodesResponse : NActors::TEventLocal<TEvGetNodesResponse, EvGetNodesResponse> {
        std::vector<ui32> NodeIds;
    };
};

} // namespace NFq
