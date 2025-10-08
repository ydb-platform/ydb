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

enum class EStatus {
    SUCCESS,
    NOT_FOUND,
    NOT_TOPIC,
    UNKNOWN_ERROR
};

struct TTopicInfo {
    EStatus Status = EStatus::NOT_FOUND;

    // Real topic path. If original topic path is CDC than real path is different.
    TString RealPath;

    TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TPQGroupInfo> Info;
    TIntrusivePtr<TSecurityObject> SecurityObject;
};

struct TEvDescribeTopicsResponse : public NActors::TEventLocal<TEvDescribeTopicsResponse, EEv::EvDescribeTopicsResponse> {

    TEvDescribeTopicsResponse(std::unordered_map<TString, TTopicInfo>&& topics)
        : Topics(std::move(topics))
    {
    }

    // The original topic path (from request) -> TopicInfo
    std::unordered_map<TString, TTopicInfo> Topics;
};

NActors::IActor* CreateDescriberActor(const NActors::TActorId& parent, const TString& databasePath, const std::unordered_set<TString>&& topicPaths);
TString Description(const TString& topicPath, const EStatus status);

}
