#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

#include <library/cpp/uri/uri.h>

#include <util/generic/guid.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::GRPC_PROXY

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
        YDB_LOG_INFO_CTX(ctx, "Got new request",
            {"logPrefix", LogPrefix()},
            {"peer", ReqCtx->GetPeer()});
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
        YDB_LOG_DEBUG_CTX(ctx, "Database discovery request sent",
            {"logPrefix", LogPrefix()});

        ctx.Send(MakeTenantDiscoveryID(), std::move(request));
    }

    void Handle(TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult::TPtr ev, const TActorContext& ctx) {

        if (ev->Get()->DatabaseInfo) {
            auto& db = ev->Get()->DatabaseInfo;
            YDB_LOG_DEBUG_CTX(ctx, "Database discovery result",
                {"logPrefix", LogPrefix()},
                {"dbPath", db->Path});
            SendGrpcRequest(ctx, db->Endpoint, db->Path);
        } else {
            return ReplyWithError(ctx, ev->Get()->Status, ev->Get()->Message);
        }
    }

    void Handle(TEvServerlessProxy::TEvListEndpointsResponse::TPtr ev, const TActorContext& ctx) {
        if (ev->Get()->Record) {
            YDB_LOG_INFO_CTX(ctx, "Replying ok",
                {"logPrefix", LogPrefix()});

            Ydb::Discovery::ListEndpointsResponse * resp = CreateResponseMessage();
            resp->CopyFrom(*(ev->Get()->Record.get()));

            ReqCtx->Reply(resp, grpc::StatusCode::OK);
            TBase::Die(ctx);
            return;
        } else if (ev->Get()->Status) {
            YDB_LOG_INFO_CTX(ctx, "Replying error",
                {"logPrefix", LogPrefix()},
                {"grpcStatusCode", ev->Get()->Status->GRpcStatusCode},
                {"statusMsg", ev->Get()->Status->Msg});
            if (ev->Get()->Status->GRpcStatusCode == grpc::StatusCode::NOT_FOUND)
                return ReplyWithError(ctx, NYdb::EStatus::NOT_FOUND, TString{ev->Get()->Status->Msg});
            return ReplyWithError(ctx, NYdb::EStatus::INTERNAL_ERROR, TString{ev->Get()->Status->Msg});
        } else {
            YDB_LOG_INFO_CTX(ctx, "Replying INTERNAL ERROR",
                {"logPrefix", LogPrefix()});
            return ReplyWithError(ctx, NYdb::EStatus::INTERNAL_ERROR, "Error happened while discovering database endpoint");
        }
    }


    void SendGrpcRequest(const TActorContext& ctx, const TString& endpoint, const TString& database) {
        YDB_LOG_DEBUG_CTX(ctx, "Send grpc request to endpoint",
            {"logPrefix", LogPrefix()},
            {"database", database},
            {"endpoint", endpoint});
        ctx.Send(MakeDiscoveryProxyID(), new TEvServerlessProxy::TEvListEndpointsRequest(endpoint, database));
    }

    void ReplyWithError(const TActorContext& ctx, NYdb::EStatus status, const TString& errorText) {
        Ydb::Discovery::ListEndpointsResponse * resp = CreateResponseMessage();
        auto deferred = resp->mutable_operation();
        deferred->set_ready(true);
        deferred->set_status(Ydb::StatusIds::StatusCode(status));
        deferred->add_issues()->set_message(TString{errorText});
        ReqCtx->Reply(resp, Ydb::StatusIds::StatusCode(status));
        TBase::Die(ctx);
    }

    Ydb::Discovery::ListEndpointsResponse* CreateResponseMessage() {
        return google::protobuf::Arena::CreateMessage<Ydb::Discovery::ListEndpointsResponse>(ReqCtx->GetArena());
    }

    TIntrusivePtr<NYdbGrpc::IRequestContextBase> ReqCtx;
    TString RequestId;
};

TGRpcDiscoveryService::TGRpcDiscoveryService(NActors::TActorSystem *system,
                                 TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
    : ActorSystem_(system)
    , Counters_(counters)
{

}

void TGRpcDiscoveryService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcDiscoveryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::Discovery;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    ReportSdkBuildInfo();

#ifdef SETUP_LEGACY_EVENT_METHOD
#error SETUP_LEGACY_EVENT_METHOD macro already defined
#endif

#define SETUP_LEGACY_EVENT_METHOD(methodName, inputType, outputType, action)                                          \
    MakeIntrusive<TGRpcRequest<inputType, outputType, TGRpcDiscoveryService>>(this, &Service_, CQ_,                   \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) {                                                               \
           NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer(), GetSdkBuildInfoIfNeeded(reqCtx));       \
           action;                                                                                                    \
        }, &TGrpcAsyncService::Y_CAT(Request, methodName),                                                            \
        Y_STRINGIZE(methodName), logger, YDB_API_DEFAULT_COUNTER_BLOCK(discovery, methodName))->Run();

    SETUP_LEGACY_EVENT_METHOD(ListEndpoints, ListEndpointsRequest, ListEndpointsResponse, {
        ActorSystem_->Register(new TGRpcRequestActor(reqCtx));
    });

#undef SETUP_LEGACY_EVENT_METHOD
}

} // namespace NKikimr::NHttpProxy
