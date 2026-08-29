#pragma once
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>

#include <ydb/library/actors/core/actor.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/ptr.h>

namespace NKikimr {

using TEvPqMetaCache = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache;

// Nodes-mapping only. Topic describe goes through NDescriber + SchemeCache.
class TMockPQMetaCache: public TActor<TMockPQMetaCache> {
public:
    TMockPQMetaCache()
        : TActor<TMockPQMetaCache>(&TMockPQMetaCache::StateFunc)
    {
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPqMetaCache::TEvGetNodesMappingRequest, HandleGetNodesMapping);
            default:
                UNIT_FAIL_NONFATAL("Unexpected event to PQ metacache: " << ev->GetTypeRewrite());
        }
    }

    void HandleGetNodesMapping(TEvPqMetaCache::TEvGetNodesMappingRequest::TPtr& ev, const TActorContext& ctx) {
        auto mapping = std::make_shared<THashMap<ui32, ui32>>();
        ctx.Send(ev->Sender, new TEvPqMetaCache::TEvGetNodesMappingResponse(mapping, true));
    }
};

} // namespace NKikimr
