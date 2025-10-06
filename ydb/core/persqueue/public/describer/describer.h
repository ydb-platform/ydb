#pragma once

#include <ydb/core/persqueue/events/events.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr::NPQ::NDescriber {

enum EEv : ui32 {
    EvDescribeTopicsResponse = InternalEventSpaceBegin(NPQ::NEvents::EServices::DESCRIBER_SERVICE),
    EvEnd
};

struct TEvDescribeTopicsResponse : public NActors::TEventLocal<TEvDescribeTopicsResponse, EEv::EvDescribeTopicsResponse> {

    enum class EStatus {
        SUCCESS,
        NOT_FOUND,
        NOT_TOPIC,
        UNKNOWN_ERROR
    };

    struct TTopicInfo {
        EStatus Status = EStatus::NOT_FOUND;

        // Topic path from request
        TString OriginalPath;
        // Real topic path. If original topic path is CDC than real path is different.
        TString RealPath;

        TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> Info;
    };

    TEvDescribeTopicsResponse(std::vector<TTopicInfo>&& topics)
        : Topics(std::move(topics))
    {
    }

    // The order is the same as in the request.
    std::vector<TTopicInfo> Topics;
};

NActors::IActor* CreateDescriberActor(const NActors::TActorId& parent, const TString& databasePath, const std::vector<TString>&& topicPaths);

}
