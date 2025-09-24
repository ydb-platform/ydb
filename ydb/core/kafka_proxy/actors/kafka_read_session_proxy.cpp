#include "kafka_read_session_proxy.h"
#include "kafka_balancer_actor.h"


namespace NKafka {

KafkaReadSessionProxyActor::KafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie)
    : Context(context)
    , Cookie(cookie)
{
}

void KafkaReadSessionProxyActor::Bootstrap() {
    Become(&KafkaReadSessionProxyActor::StateInit);
}

template<typename TRequest>
void KafkaReadSessionProxyActor::HandleOnInit(TRequest& ev) {
    Y_ENSURE(!PendingRequest.has_value());
    PendingRequest = std::move(ev);
}

STFUNC(KafkaReadSessionProxyActor::StateInit) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvKafka::TEvJoinGroupRequest, HandleOnInit);
        hFunc(TEvKafka::TEvSyncGroupRequest, HandleOnInit);
        hFunc(TEvKafka::TEvHeartbeatRequest, HandleOnInit);
        hFunc(TEvKafka::TEvLeaveGroupRequest, HandleOnInit);
    }
}

template<typename TRequest>
void KafkaReadSessionProxyActor::HandleOnWork(TRequest& ev) {
    switch (Mode) {
        case EMode::NativeBalancing:
            Register(new TKafkaBalancerActor(Context, 0, ev->Get()->CorrelationId, ev->Get()->Request));
            break;

        case EMode::ServerBalancing:
            Forward(ev, ReadSessionActorId);
            break;
    }
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvJoinGroupRequest::TPtr& ev) {
    if (NKikimr::AppData()->FeatureFlags.GetEnableKafkaNativeBalancing()) {
        Mode = AnyOf(ev->Get()->Request->Protocols, [](const auto& p) {
            return p.Name == "ydb";
        }) ? EMode::ServerBalancing : EMode::NativeBalancing;
    } else {
        Mode = EMode::ServerBalancing;
    }

    HandleOnWork(ev);
}

STFUNC(KafkaReadSessionProxyActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvKafka::TEvJoinGroupRequest, Handle);
        hFunc(TEvKafka::TEvSyncGroupRequest, HandleOnWork);
        hFunc(TEvKafka::TEvHeartbeatRequest, HandleOnWork);
        hFunc(TEvKafka::TEvLeaveGroupRequest, HandleOnWork);
    }
}


void KafkaReadSessionProxyActor::OnInitComplete() {
    if (PendingRequest.has_value()) {
        std::visit([this](auto& request) {
            HandleOnWork(request);
        }, PendingRequest.value());

        PendingRequest.reset();
    }
}

void KafkaReadSessionProxyActor::PassAway() {
    if (ReadSessionActorId) {
        Send(ReadSessionActorId, new NActors::TEvents::TEvPoison());
    }
    TBase::PassAway();
}

IActor* CreateKafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie) {
    return new KafkaReadSessionProxyActor(context, cookie);
}

}