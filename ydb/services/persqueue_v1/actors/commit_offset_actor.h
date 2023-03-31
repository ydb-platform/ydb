#pragma once

#include "events.h"

#include <ydb/core/grpc_services/rpc_deferrable.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>

#include <ydb/core/persqueue/events/global.h>


namespace NKikimr::NGRpcProxy::V1 {

using namespace NKikimr::NGRpcService;

class TCommitOffsetActor : public TRpcOperationRequestActor<TCommitOffsetActor, TEvCommitOffsetRequest> {

    using TBase = TRpcOperationRequestActor<TCommitOffsetActor, TEvCommitOffsetRequest>;

    using TEvDescribeTopicsResponse = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsResponse;
    using TEvDescribeTopicsRequest = NMsgBusProxy::NPqMetaCacheV2::TEvPqNewMetaCache::TEvDescribeTopicsRequest;

public:
    static constexpr ui32 MAX_PIPE_RESTARTS = 100; //after 100 restarts without progress kill session

public:
     TCommitOffsetActor(
             NKikimr::NGRpcService::TEvCommitOffsetRequest* request, const NPersQueue::TTopicsListController& topicsHandler,
             const NActors::TActorId& schemeCache, const NActors::TActorId& newSchemeCache,
             TIntrusivePtr<::NMonitoring::TDynamicCounters> counters
     );
    ~TCommitOffsetActor();

    void Bootstrap(const NActors::TActorContext& ctx);


    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::FRONT_PQ_COMMIT; }

    bool HasCancelOperation() {
        return false;
    }

private:

    void Die(const NActors::TActorContext& ctx) override;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPQProxy::TEvAuthResultOk, Handle); // from auth actor
            HFunc(TEvPQProxy::TEvCloseSession, Handle); // from auth actor

            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);

            HFunc(TEvPersQueue::TEvResponse, Handle);
        default:
            break;
        };
    }

    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const NActors::TActorContext& ctx);

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const NActors::TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const NActors::TActorContext& ctx);

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
    ui64 PartitionId;

    TActorId PipeClient;

    NPersQueue::TTopicsListController TopicsHandler;
};

}
