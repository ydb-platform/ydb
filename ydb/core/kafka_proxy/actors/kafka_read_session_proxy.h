#pragma once

#include "actors.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/kafka_proxy/kafka_events.h>

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

    STFUNC(StateWork);

    TSyncGroupResponseData::TPtr CreateChangeResponse(TSyncGroupRequestData&);
    THeartbeatResponseData::TPtr CreateChangeResponse(THeartbeatRequestData&);

    void EnsureReadSessionActor();
    void PassAway();

private:
    const TContext::TPtr Context;
    const ui64 Cookie;

    TActorId ReadSessionActorId;
};

}
