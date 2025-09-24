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

    enum class EMode {
        NativeBalancing,
        ServerBalancing
    };

public:
    KafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie);

    void Bootstrap();

    template<typename TRequest>
    void HandleOnInit(TRequest&);

    void Handle(TEvKafka::TEvJoinGroupRequest::TPtr&);
    template<typename TRequest>
    void HandleOnWork(TRequest&);

    STFUNC(StateInit);
    STFUNC(StateWork);

    void OnInitComplete();
    void PassAway();

private:
    const TContext::TPtr Context;
    const ui64 Cookie;

    EMode Mode;

    TActorId ReadSessionActorId;

    using TMessage = std::variant<
        TEvKafka::TEvJoinGroupRequest::TPtr,
        TEvKafka::TEvSyncGroupRequest::TPtr,
        TEvKafka::TEvHeartbeatRequest::TPtr,
        TEvKafka::TEvLeaveGroupRequest::TPtr
    >;

    std::optional<TMessage> PendingRequest;
};

}
