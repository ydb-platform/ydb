#pragma once

#include "events.h"

#include <ydb/core/grpc_services/rpc_deferrable.h>

#include <ydb/core/persqueue/events/global.h>


namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;

class TReadInfoActor : public TRpcOperationRequestActor<TReadInfoActor, TEvPQReadInfoRequest> {
using TBase = TRpcOperationRequestActor<TReadInfoActor, TEvPQReadInfoRequest>;
public:
     TReadInfoActor(
             NKikimr::NGRpcService::TEvPQReadInfoRequest* request, const NPersQueue::TTopicsListController& topicsHandler,
             const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
             TIntrusivePtr<::NMonitoring::TDynamicCounters> counters
     );
    ~TReadInfoActor();

    void Bootstrap(const NActors::TActorContext& ctx);



    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::PQ_META_REQUEST_PROCESSOR; }

    bool HasCancelOperation() {
        return false;
    }

private:

    void Die(const NActors::TActorContext& ctx) override;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQProxy::TEvAuthResultOk, Handle); // form auth actor
            HFunc(TEvPQProxy::TEvCloseSession, Handle) //from auth actor

            HFunc(TEvPersQueue::TEvResponse, Handle);
        default:
            break;
        };
    }

    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvPersQueue::TEvResponse::TPtr& ev, const TActorContext& ctx);

    void AnswerError(const TString& errorReason, const PersQueue::ErrorCode::ErrorCode errorCode, const NActors::TActorContext& ctx);
    void ProcessAnswers(const TActorContext& ctx);

private:
    TActorId SchemeCache;
    TActorId NewSchemeCache;

    TActorId AuthInitActor;

    TTopicInitInfoMap TopicAndTablets;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    TString ClientId;
    NPersQueue::TTopicsListController TopicsHandler;
};

}
