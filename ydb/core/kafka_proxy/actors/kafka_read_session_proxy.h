#pragma once

#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>

namespace NKafka {

using namespace NKikimr;

class KafkaReadSessionProxyActor : public TActorBootstrapped<KafkaReadSessionProxyActor> {
    using TBase = TActorBootstrapped<KafkaReadSessionProxyActor>;

public:
    KafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie);

    void Bootstrap();

    void Handle(TEvKafka::TEvJoinGroupRequest::TPtr&);
    void Handle(TEvKafka::TEvSyncGroupRequest::TPtr&);
    void Handle(TEvKafka::TEvHeartbeatRequest::TPtr&);
    void Handle(TEvKafka::TEvLeaveGroupRequest::TPtr&);
    void Handle(TEvKafka::TEvFetchRequest::TPtr&);

    template<bool handlePending, typename TRequest>
    void DoHandle(TRequest&, const TString& event);

    void Handle(NPQ::NDescriber::TEvDescribeTopicsResponse::TPtr&);
    void Handle(TEvPersQueue::TEvBalancingSubscribeNotify::TPtr&);

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev);

    STFUNC(StateWork);

    TSyncGroupResponseData::TPtr CreateChangeResponse(TSyncGroupRequestData&);
    THeartbeatResponseData::TPtr CreateChangeResponse(THeartbeatRequestData&);
    TFetchResponseData::TPtr CreateChangeResponse(TFetchRequestData&);

    void EnsureReadSessionActor();
    void ProcessPendingRequestIfPossible();
    void Reconnect(ui64 tabletId);
    void Subscribe(const TString& topic, ui64 tabletId, const ui64 cookie);
    void Unsubscribe(const TString& topic, ui64 tabletId);
    void PassAway();

private:
    const TContext::TPtr Context;
    const ui64 Cookie;

    TActorId ReadSessionActorId;

    struct TTopicInfo {
        bool Initialized;
        ui64 ReadBalancerTabletId;

        ui64 ReadBalancerGeneration = 0;
        ui64 ReadBalancerNotifyCookie = 0;
        std::optional<bool> UsedServerBalancing;

        ui64 SubscribeCookie = 1;
    };
    std::unordered_map<TString, TTopicInfo> Topics;

    std::optional<TEvKafka::TEvFetchRequest::TPtr> PendingRequest;
};

}
