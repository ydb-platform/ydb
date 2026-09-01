#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NMsgBusProxy {

static const ui32 PQ_METACACHE_TIMEOUT_SECONDS = 120;
static const ui32 PQ_METACACHE_REFRESH_INTERVAL_SECONDS = 10;


inline TActorId CreatePersQueueMetaCacheV2Id() {
    return TActorId(0, "PQMetaCache");
}

namespace NPqMetaCacheV2 {

enum class EQueryType {
    ECheckVersion,
    EGetTopics,
};

struct TTopicMetaRequest {
    TString Path;
    THolder<NSchemeCache::TSchemeCacheNavigate> Response;
    bool Success = false;
};

using TMetaCacheRequest = TVector<TTopicMetaRequest>;

struct TEvPqNewMetaCache {
    enum EEv {
        EvWakeup = EventSpaceBegin(TKikimrEvents::ES_PQ_META_CACHE),
        EvGetNodesMappingRequest,
        EvGetNodesMappingResponse,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_META_CACHE),
                  "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_META_CACHE)");


    struct TEvGetNodesMappingRequest : public TEventLocal<TEvGetNodesMappingRequest, EvGetNodesMappingRequest> {
    };

    struct TEvGetNodesMappingResponse : public TEventLocal<TEvGetNodesMappingResponse, EvGetNodesMappingResponse> {
        std::shared_ptr<THashMap<ui32, ui32>> NodesMapping;
        bool Status;

        TEvGetNodesMappingResponse(const std::shared_ptr<THashMap<ui32, ui32>>& nodesMapping, bool status)
            : NodesMapping(std::move(nodesMapping))
            , Status(status)
        {}

    };

};
IActor* CreatePQMetaCache(const ::NMonitoring::TDynamicCounterPtr& counters);

IActor* CreatePQMetaCache(const NActors::TActorId& schemeBoardCacheId);


} // namespace NPqMetaCacheV2

} //namespace NKikimr::NMsgBusProxy
