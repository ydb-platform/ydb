#include "ic_nodes_cache_service.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>


namespace NKikimr::NIcNodeCache {
using namespace NActors;

class TIcNodeCacheServiceActor : public TActorBootstrapped<TIcNodeCacheServiceActor> {
    using TBase = TActorBootstrapped<TIcNodeCacheServiceActor>;
public:
    TIcNodeCacheServiceActor(TIcNodeCacheServiceActor&&) = default;
    TIcNodeCacheServiceActor& operator=(TIcNodeCacheServiceActor&&) = default;

    TIcNodeCacheServiceActor(const ::NMonitoring::TDynamicCounterPtr& counters,
                             const TDuration& cacheUpdateInterval)
        : Counters(counters)
        , CacheUpdateInterval(cacheUpdateInterval)
    {}


private:
   void RequestNodes() {
        if (InfoRequested)
            return;

        const static TActorId nameserviceId = GetNameserviceActorId();
        InfoRequested = true;
        this->ActorContext().Send(nameserviceId, new TEvInterconnect::TEvListNodes());
    }
 
    void HandleNodesInfo(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        Y_ABORT_UNLESS(InfoRequested);
        InfoRequested = false;
        NodesCache.reset(new TNodeInfoVector(ev->Get()->Nodes));
        NextUpdate = TActivationContext::Now() + CacheUpdateInterval;
        NodeIdsMapping.reset(new THashMap<ui64, ui64>());

        RespondToAllWaiters();
    }

    void RespondToAllWaiters() {
        while (!WaitersOnInit.empty()) {
            RespondNodesInfo(WaitersOnInit.front());
            WaitersOnInit.pop_front();
        }
    }

    void BuildNodesMap() {
        Y_DEBUG_ABORT_UNLESS(NodeIdsMapping->empty());
        for (auto i = 0u; i < NodesCache->size(); i++) {
            auto res = NodeIdsMapping->insert(
                std::make_pair((*NodesCache)[i].NodeId, i)
            );
            Y_DEBUG_ABORT_UNLESS(res.second);
        }
    }

    void HandleNodesRequest(TEvICNodesInfoCache::TEvGetAllNodesInfoRequest::TPtr& ev) {
        if (ActorContext().Now() > NextUpdate) {
            RequestNodes();
        }

        if(!NodesCache) {
            WaitersOnInit.emplace_back(ev->Sender);
            return;
        } else {
            RespondNodesInfo(ev->Sender);
        }
    }

    void RespondNodesInfo(const TActorId& recipient) {
        if (NodeIdsMapping->empty()) {
            BuildNodesMap();
        }
        auto* response = new TEvICNodesInfoCache::TEvGetAllNodesInfoResponse(NodesCache, NodeIdsMapping);
        ActorContext().Send(recipient, response);
    }

    void HandleWakeup() {
        if (TActivationContext::Now() > NextUpdate) {
            RequestNodes();
        }
        ActorContext().Schedule(CacheUpdateInterval / 2, new TEvents::TEvWakeup());
    }

public:
    void Bootstrap(const TActorContext&) {
        Become(&TIcNodeCacheServiceActor::StateFunc);
        HandleWakeup();
    }
    
    STRICT_STFUNC(StateFunc,
          hFunc(TEvInterconnect::TEvNodesInfo, HandleNodesInfo)
          sFunc(TEvents::TEvWakeup, HandleWakeup)
          hFunc(TEvICNodesInfoCache::TEvGetAllNodesInfoRequest, HandleNodesRequest)
    )

private:
    TNodeInfoVectorPtr NodesCache;
    std::shared_ptr<THashMap<ui64, ui64>> NodeIdsMapping;
    bool InfoRequested = false;
    ::NMonitoring::TDynamicCounterPtr Counters;
    TDuration CacheUpdateInterval;
    TInstant NextUpdate = TInstant::Zero();

    TDeque<TActorId> WaitersOnInit;

};

NActors::IActor* CreateICNodesInfoCacheService(
    const ::NMonitoring::TDynamicCounterPtr& counters, const TDuration& cacheUpdateTimeout
) {
    return new TIcNodeCacheServiceActor(counters, cacheUpdateTimeout);
}
} // NKikimr::NIcNodeCache