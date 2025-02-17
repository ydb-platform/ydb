#pragma once

#include "events.h"
#include "persqueue_utils.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/persqueue/events/global.h>

#include <ydb/core/persqueue/pq_rl_helpers.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NGRpcProxy::V1 {

struct TFormedDirectReadResponse: public TSimpleRefCount<TFormedDirectReadResponse> {
    using TPtr = TIntrusivePtr<TFormedDirectReadResponse>;

    TFormedDirectReadResponse() = default;

    TFormedDirectReadResponse(TInstant start)
        : Start(start)
    {
    }

    std::shared_ptr<Topic::StreamDirectReadMessage::FromServer> Response;

    TInstant Start;
    TDuration WaitQuotaTime;

    ui64 RequiredQuota = 0;
};



class TDirectReadSessionActor
    : public TActorBootstrapped<TDirectReadSessionActor>
    , private NPQ::TRlHelpers
{
    using TClientMessage = Topic::StreamDirectReadMessage::FromClient;

    using TServerMessage = Topic::StreamDirectReadMessage::FromServer;

    using TEvStreamReadRequest = NGRpcService::TEvStreamTopicDirectReadRequest;

    using IContext = NGRpcServer::IGRpcStreamingContext<TClientMessage, TServerMessage>;

public:
    TDirectReadSessionActor(TEvStreamReadRequest* request, const ui64 cookie,
        const TActorId& schemeCache, const TActorId& newSchemeCache,
        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
        const TMaybe<TString> clientDC,
        const NPersQueue::TTopicsListController& topicsHandler);

    void Bootstrap(const TActorContext& ctx);

    void Die(const TActorContext& ctx) override;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::FRONT_PQ_READ;
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            // grpc events
            HFunc(IContext::TEvReadFinished, Handle);
            HFunc(IContext::TEvWriteFinished, Handle);
            HFunc(IContext::TEvNotifiedWhenDone, Handle)
            HFunc(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse, Handle);

            // proxy events
            HFunc(TEvPQProxy::TEvAuthResultOk, Handle); // form auth actor
            HFunc(TEvPQProxy::TEvInitDirectRead,  Handle); // from gRPC
            HFunc(TEvPQProxy::TEvDone, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvCloseSession, Handle); // from auth actor
            HFunc(TEvPQProxy::TEvStartDirectRead, Handle); // from gRPC
            HFunc(TEvPQProxy::TEvAuth, Handle); // from gRPC

            HFunc(TEvPQProxy::TEvDirectReadDataSessionConnectedResponse, Handle); // from CachingService
            hFunc(TEvPQProxy::TEvDirectReadCloseSession, HandleSessionKilled) // from CachingService
            hFunc(TEvPQProxy::TEvDirectReadDestroyPartitionSession, HandleDestroyPartitionSession) // from CachingService
            hFunc(TEvPQProxy::TEvDirectReadSendClientData, HandleGotData) // from CachingService
            // system events
            HFunc(TEvents::TEvWakeup, Handle);

        default:
            break;
        }
    }

    bool ReadFromStreamOrDie(const TActorContext& ctx);
    bool WriteToStreamOrDie(const TActorContext& ctx, TServerMessage&& response, bool finish = false);

    void InitSession(const TActorContext& ctx);

    // grpc events
    void Handle(typename IContext::TEvReadFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(typename IContext::TEvWriteFinished::TPtr& ev, const TActorContext &ctx);
    void Handle(typename IContext::TEvNotifiedWhenDone::TPtr& ev, const TActorContext &ctx);
    void Handle(NGRpcService::TGRpcRequestProxy::TEvRefreshTokenResponse::TPtr& ev, const TActorContext &ctx);

    // proxy events
    void Handle(TEvPQProxy::TEvAuthResultOk::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvInitDirectRead::TPtr& ev,  const TActorContext& ctx);
    //void Handle(typename TEvReadResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDone::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvCloseSession::TPtr& ev, const TActorContext& ctx);
    //void Handle(TEvPQProxy::TEvDieCommand::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvStartDirectRead::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvDirectReadDataSessionConnectedResponse::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvPQProxy::TEvAuth::TPtr& ev, const TActorContext& ctx);

    // Caching service events
    void HandleSessionKilled(TEvPQProxy::TEvDirectReadCloseSession::TPtr& ev);
    void HandleDestroyPartitionSession(TEvPQProxy::TEvDirectReadDestroyPartitionSession::TPtr& ev);

    void HandleGotData(TEvPQProxy::TEvDirectReadSendClientData::TPtr& ev);

    // system events
    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx);

    void RunAuthActor(const TActorContext& ctx);
    void RecheckACL(const TActorContext& ctx);

    void CloseSession(PersQueue::ErrorCode::ErrorCode code, const TString& reason);

    void SetupCounters();
    void SetupCounters(const TString& cloudId, const TString& dbId, const TString& dbPath, const bool isServerless, const TString& folderId);

    void ProcessAnswer(typename TFormedDirectReadResponse::TPtr response, const TActorContext& ctx);

private:
    std::unique_ptr<TEvStreamReadRequest> Request;
    ui64 Cookie;
    const TString ClientDC;
    const TInstant StartTimestamp;

    TActorId SchemeCache;
    TActorId NewSchemeCache;

    TActorId AuthInitActor;
    TIntrusiveConstPtr<NACLib::TUserToken> Token;

    TString ClientId;
    TString ClientPath;
    TString Session;
    TString PeerName;

    bool InitDone;

    TString Auth;

    bool ForceACLCheck;
    TInstant LastACLCheckTimestamp;

    //THashMap<TString, TTopicHolder> Topics; // topic -> info
    THashMap<TString, NPersQueue::TTopicConverterPtr> FullPathToConverter; // PrimaryFullPath -> Converter, for balancer replies matching
    THashSet<TString> TopicsToResolve;

   // Response that currenly pending quota
    TFormedDirectReadResponse::TPtr PendingQuota;

   // Responses that will be quoted next
   std::deque<TFormedDirectReadResponse::TPtr> WaitingQuota;

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    ::NMonitoring::TDynamicCounters::TCounterPtr DirectSessionsCreated;
    ::NMonitoring::TDynamicCounters::TCounterPtr DirectSessionsActive;

    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;

    TInstant StartTime;

    NPersQueue::TTopicsListController TopicsHandler;
    NPersQueue::TTopicsToConverter TopicsList;

    bool Initing = false;
};

}
