#pragma once

#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/kafka_proxy/kafka_events.h>
#include <ydb/core/persqueue/events/internal.h>

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

    template<typename TRequest>
    void DoHandle(TRequest&);

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr&);
    void Handle(TEvPersQueue::TEvBalancingSubscribeNotify::TPtr&);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev);

    STFUNC(StateWork);

    void EnsureReadSessionActor();
    void ProcessPendingRequestIfPossible();
    void Reconnect(ui64 tabletId);
    void PassAway();
    TActorId CreatePipe(ui64 tabletId);

private:
    const TContext::TPtr Context;
    const ui64 Cookie;

    TActorId ReadSessionActorId;

    struct TTopicInfo {
        ui64 ReadBalancerTabletId;

        ui64 ReadBalancerGeneration = 0;
        ui64 ReadBalancerNotifyCookie = 0;
        std::optional<bool> UsedServerBalancing;
    };
    std::unordered_map<TString, TTopicInfo> Topics;
    std::vector<TString> NewTopics;

    std::optional<TEvKafka::TEvFetchRequest::TPtr> PendingRequest;
};

}
