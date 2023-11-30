#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/grpc_helper.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/actors/core/hfunc.h>
#include <library/cpp/uri/uri.h>

#include <util/generic/guid.h>

namespace NKikimr::NHttpProxy {


using namespace NGRpcService;



class TGRpcRequestActor : public NActors::TActorBootstrapped<TGRpcRequestActor> {
public:
    using TBase = NActors::TActorBootstrapped<TGRpcRequestActor>;


    TStringBuilder LogPrefix() const {
        return TStringBuilder() << "[GrpcProxy] ListEndpointRequest requestId : " << RequestId;
        // << ReqCtx->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER)
        //                << " trace: " << ReqCtx->GetPeerMetaValues(NYdb::YDB_TRACE_ID_HEADER) << " ";
    }

    TGRpcRequestActor(NYdbGrpc::IRequestContextBase *ctx)
        : ReqCtx(ctx)
        , RequestId(CreateGuidAsString())
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        LOG_SP_INFO_S(ctx, NKikimrServices::GRPC_PROXY, "got new request from " << ReqCtx->GetPeer());
        SendYdbDriverRequest(ctx);
        Become(&TGRpcRequestActor::StateWork);
    }

private:

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult, Handle);
            HFunc(TEvServerlessProxy::TEvListEndpointsResponse, Handle);
//            HFunc(TEvents::TEvWakeup, HandleTimeout); //TODO timeout
            break;
        }
    }

    void SendYdbDriverRequest(const TActorContext& ctx) {
        auto request = MakeHolder<TEvServerlessProxy::TEvDiscoverDatabaseEndpointRequest>();
        const auto& res = ReqCtx->GetPeerMetaValues(NYdb::YDB_DATABASE_HEADER);
        TString database;
        if (!res.empty()) {
            database = CGIUnescapeRet(res[0]);
        } else {
            database = dynamic_cast<const Ydb::Discovery::ListEndpointsRequest*>(ReqCtx->GetRequest())->database();
        }
        request->DatabasePath = database;
        LOG_SP_DEBUG_S(ctx, NKikimrServices::GRPC_PROXY, "Database discovery request sent");

        ctx.Send(MakeTenantDiscoveryID(), std::move(request));
    }

    void Handle(TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult::TPtr ev, const TActorContext& ctx) {

        if (ev->Get()->DatabaseInfo) {
            auto& db = ev->Get()->DatabaseInfo;
            LOG_SP_DEBUG_S(ctx, NKikimrServices::GRPC_PROXY, "Database discovery result " << db->Path);
            SendGrpcRequest(ctx, db->Endpoint, db->Path);
        } else {
            return ReplyWithError(ctx, ev->Get()->Status, ev->Get()->Message);
        }
    }

    void Handle(TEvServerlessProxy::TEvListEndpointsResponse::TPtr ev, const TActorContext& ctx) {
        if (ev->Get()->Record) {
            LOG_SP_INFO_S(ctx, NKikimrServices::GRPC_PROXY, "Replying ok");

            Ydb::Discovery::ListEndpointsResponse * resp = CreateResponseMessage();
            resp->CopyFrom(*(ev->Get()->Record.get()));

            ReqCtx->Reply(resp, grpc::StatusCode::OK);
            TBase::Die(ctx);
            return;
        } else if (ev->Get()->Status) {
            LOG_SP_INFO_S(ctx, NKikimrServices::GRPC_PROXY, "Replying " << ev->Get()->Status->GRpcStatusCode << " error " << ev->Get()->Status->Msg);
            if (ev->Get()->Status->GRpcStatusCode == grpc::StatusCode::NOT_FOUND)
                return ReplyWithError(ctx, NYdb::EStatus::NOT_FOUND, ev->Get()->Status->Msg);
            return ReplyWithError(ctx, NYdb::EStatus::INTERNAL_ERROR, ev->Get()->Status->Msg);
        } else {
            LOG_SP_INFO_S(ctx, NKikimrServices::GRPC_PROXY, "Replying INTERNAL ERROR");
            return ReplyWithError(ctx, NYdb::EStatus::INTERNAL_ERROR, "Error happened while discovering database endpoint");
        }
    }


    void SendGrpcRequest(const TActorContext& ctx, const TString& endpoint, const TString& database) {
        LOG_SP_DEBUG_S(ctx, NKikimrServices::GRPC_PROXY, "Send grpc request to " << database << " endpoint " << endpoint);
        ctx.Send(MakeDiscoveryProxyID(), new TEvServerlessProxy::TEvListEndpointsRequest(endpoint, database));
    }

    void ReplyWithError(const TActorContext& ctx, NYdb::EStatus status, const TString& errorText) {
        Ydb::Discovery::ListEndpointsResponse * resp = CreateResponseMessage();
        auto deferred = resp->mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(Ydb::StatusIds::StatusCode(status));
        deferred->add_issues()->set_message(errorText);
        ReqCtx->Reply(resp, Ydb::StatusIds::StatusCode(status));
        TBase::Die(ctx);
    }

    Ydb::Discovery::ListEndpointsResponse* CreateResponseMessage() {
        return google::protobuf::Arena::CreateMessage<Ydb::Discovery::ListEndpointsResponse>(ReqCtx->GetArena());
    }

    TIntrusivePtr<NYdbGrpc::IRequestContextBase> ReqCtx;
    TString RequestId;
};


static TString GetSdkBuildInfo(NYdbGrpc::IRequestContextBase* reqCtx) {
    const auto& res = reqCtx->GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    if (res.empty()) {
        return {};
    }
    return TString{res[0]};
}

TGRpcDiscoveryService::TGRpcDiscoveryService(NActors::TActorSystem *system, std::shared_ptr<NYdb::ICredentialsProvider> credentialsProvider,
                                 TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
    : ActorSystem_(system)
    , CredentialsProvider_(credentialsProvider)
    , Counters_(counters)
{

}

void TGRpcDiscoveryService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcDiscoveryService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter *limiter) {
    Limiter_ = limiter;
}

bool TGRpcDiscoveryService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcDiscoveryService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcDiscoveryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = NGRpcService::CreateCounterCb(Counters_, ActorSystem_);
#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
     MakeIntrusive<TGRpcRequest<Ydb::Discovery::IN, Ydb::Discovery::OUT, TGRpcDiscoveryService>>(this, &Service_, CQ_, \
         [this](NYdbGrpc::IRequestContextBase *reqCtx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer(), GetSdkBuildInfo(reqCtx)); \
            ACTION; \
         }, &Ydb::Discovery::V1::DiscoveryService::AsyncService::Request ## NAME, \
         #NAME, logger, getCounterBlock("discovery", #NAME))->Run();

    ADD_REQUEST(ListEndpoints, ListEndpointsRequest, ListEndpointsResponse, {
        ActorSystem_->Register(new TGRpcRequestActor(reqCtx));
    })
#undef ADD_REQUEST
}

} // namespace NKikimr
