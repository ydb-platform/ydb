#pragma once

#include <library/cpp/threading/future/future.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <util/generic/fwd.h>

namespace NYql {
    struct TEvIamAuthCredentialsProviderService {
        // Event ids
        enum EEv : ui32 {
            EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
            EvGetAuthInfoRequest = EvBegin,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");

        // Events
        struct TEvGetAuthInfoRequest : public NActors::TEventLocal<TEvGetAuthInfoRequest, EvGetAuthInfoRequest> {
            TString ServiceAccountId;
            TString ResourceId;
            NThreading::TPromise<std::string> Promise;

            explicit TEvGetAuthInfoRequest(const TString& serviceAccountId, const TString& resourceId, NThreading::TPromise<std::string>& promise)
               : ServiceAccountId(serviceAccountId)
               , ResourceId(resourceId)
               , Promise(promise)
            {}
        };
    };

    NActors::TActorId MakeCachingIamServiceCredentialsProviderServiceId();
}
