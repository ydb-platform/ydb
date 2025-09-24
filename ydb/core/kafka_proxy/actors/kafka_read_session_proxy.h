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

    template<typename TRequest>
    void HandleOnWork(TRequest&);

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr&);

    STFUNC(StateWork);

    void PassAway();
    TActorId CreatePipe(ui64 tabletId);

private:
    const TContext::TPtr Context;
    const ui64 Cookie;

    TActorId ReadSessionActorId;

    struct TTopicInfo {
        ui64 ReadBalancerTabletId;
        TActorId PipeClient;
    };
    std::unordered_map<TString, TTopicInfo> Topics;
    std::vector<TString> NewTopics;

};

}
