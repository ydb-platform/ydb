#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/base/events.h>

namespace NKikimr::NIcNodeCache {

NActors::IActor* CreateICNodesInfoCacheService(const ::NMonitoring::TDynamicCounterPtr& counters,
                                               const TDuration& cacheUpdateTimeout = TDuration::Seconds(10));

inline NActors::TActorId CreateICNodesInfoCacheServiceId() {
    return NActors::TActorId(0, "ICNodeCache");
}

using TNodeInfoVector = TVector<NActors::TEvInterconnect::TNodeInfo>;
using TNodeInfoVectorPtr = std::shared_ptr<TNodeInfoVector>;

struct TEvICNodesInfoCache {
    enum EEv {
        EvWakeup = EventSpaceBegin(TKikimrEvents::ES_IC_NODE_CACHE),
        EvGetAllNodesInfoRequest,
        EvGetAllNodesInfoResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_IC_NODE_CACHE),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_IC_NODE_CACHE)");


    struct TEvGetAllNodesInfoRequest : public NActors::TEventLocal<TEvGetAllNodesInfoRequest, EvGetAllNodesInfoRequest> {
    };

    struct TEvGetAllNodesInfoResponse : public NActors::TEventLocal<TEvGetAllNodesInfoResponse, EvGetAllNodesInfoResponse> {
        TNodeInfoVectorPtr Nodes;
        std::shared_ptr<THashMap<ui64, ui64>> NodeIdsMapping;

        
        TEvGetAllNodesInfoResponse(const TNodeInfoVectorPtr& nodesInfo, const std::shared_ptr<THashMap<ui64, ui64>>& nodeIdsMapping)
            : Nodes(nodesInfo)
            , NodeIdsMapping(nodeIdsMapping)
        {}
    };
};
} // NKikimr::NIcNodeCache