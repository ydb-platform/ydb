#include "kafka_read_session_proxy.h"
#include "kafka_read_session_utils.h"
#include "kafka_balancer_actor.h"

namespace NKafka {

KafkaReadSessionProxyActor::KafkaReadSessionProxyActor(const TContext::TPtr context, ui64 cookie)
    : Context(context)
    , Cookie(cookie)
{
}

void KafkaReadSessionProxyActor::Bootstrap() {
    Become(&KafkaReadSessionProxyActor::StateWork);
    Y_UNUSED(Cookie);
}

template<bool handlePending, typename TRequest>
void KafkaReadSessionProxyActor::DoHandle(TRequest& ev, const TString& event) {
    if constexpr (handlePending) {
        if (Context->ReadSession.PendingBalancingMode.has_value()) {
            KAFKA_LOG_D("DoHandle " << event << " with pending balance mode");
            auto response = CreateChangeResponse(*ev->Get()->Request);
            Send(Context->ConnectionId, new TEvKafka::TEvResponse(ev->Get()->CorrelationId, response, EKafkaErrors::REBALANCE_IN_PROGRESS));
            return;
        }
    }

    KAFKA_LOG_D("DoHandle " << event);
    switch (Context->ReadSession.BalancingMode) {
        case EBalancingMode::Native:
            Register(new TKafkaBalancerActor(Context, 0, ev->Get()->CorrelationId, ev->Get()->Request));
            break;

        case EBalancingMode::Server:
            EnsureReadSessionActor();
            Forward(ev, ReadSessionActorId);
            break;
    }
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvJoinGroupRequest::TPtr& ev) {
    KAFKA_LOG_D("Handle TEvKafka::TEvJoinGroupRequest");
    Context->ReadSession.BalancingMode = Context->ReadSession.PendingBalancingMode.value_or(GetBalancingMode(*ev->Get()->Request));
    Context->ReadSession.PendingBalancingMode.reset();
    KAFKA_LOG_D("Balancing mode: " << Context->ReadSession.BalancingMode);

    DoHandle<false>(ev, "TEvKafka::TEvJoinGroupRequest");
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvSyncGroupRequest::TPtr& ev) {
    DoHandle<true>(ev, "TEvKafka::TEvSyncGroupRequest");
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvHeartbeatRequest::TPtr& ev) {
    DoHandle<true>(ev, "TEvKafka::TEvHeartbeatRequest");
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvLeaveGroupRequest::TPtr& ev) {
    DoHandle<false>(ev, "TEvKafka::TEvLeaveGroupRequest");
}

void KafkaReadSessionProxyActor::Handle(TEvKafka::TEvFetchRequest::TPtr& ev) {
    KAFKA_LOG_D("Handle TEvKafka::TEvFetchRequest");
    Register(CreateKafkaFetchActor(Context, ev->Get()->CorrelationId, ev->Get()->Request));
}

TSyncGroupResponseData::TPtr KafkaReadSessionProxyActor::CreateChangeResponse(TSyncGroupRequestData&) {
    TSyncGroupResponseData::TPtr response = std::make_shared<TSyncGroupResponseData>();
    response->ErrorCode = EKafkaErrors::ILLEGAL_GENERATION;
    response->Assignment = "";

    return response;
}

THeartbeatResponseData::TPtr KafkaReadSessionProxyActor::CreateChangeResponse(THeartbeatRequestData&) {
    THeartbeatResponseData::TPtr response = std::make_shared<THeartbeatResponseData>();
    response->ErrorCode = EKafkaErrors::REBALANCE_IN_PROGRESS;

    return response;
}

STFUNC(KafkaReadSessionProxyActor::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvKafka::TEvJoinGroupRequest, Handle);
        hFunc(TEvKafka::TEvSyncGroupRequest, Handle);
        hFunc(TEvKafka::TEvHeartbeatRequest, Handle);
        hFunc(TEvKafka::TEvLeaveGroupRequest, Handle);
        hFunc(TEvKafka::TEvFetchRequest, Handle);

        sFunc(TEvents::TEvPoison, PassAway);
    }
}

void KafkaReadSessionProxyActor::EnsureReadSessionActor() {
    if (!ReadSessionActorId) {
        ReadSessionActorId = Register(CreateKafkaReadSessionActor(Context, Cookie));
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
