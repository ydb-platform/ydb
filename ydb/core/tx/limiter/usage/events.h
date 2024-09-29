#pragma once
#include "abstract.h"
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/conclusion/result.h>
#include <ydb/core/base/events.h>

namespace NKikimr::NLimiter {

struct TEvExternal {
    enum EEv {
        EvAskResource = EventSpaceBegin(TKikimrEvents::ES_LIMITER),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_LIMITER), "expected EvEnd < EventSpaceEnd");

    class TEvAskResource: public NActors::TEventLocal<TEvAskResource, EvAskResource> {
    private:
        YDB_READONLY_DEF(std::shared_ptr<IResourceRequest>, Request);
    public:
        TEvAskResource() = default;

        explicit TEvAskResource(const std::shared_ptr<IResourceRequest>& request)
            : Request(request) {
        }
    };
};

}
