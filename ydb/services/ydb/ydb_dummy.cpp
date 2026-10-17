#include "ydb_dummy.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/rpc_deferrable.h>

#include <ydb/core/grpc_streaming/grpc_streaming.h>
#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/public/api/grpc/draft/dummy.grpc.pb.h>

namespace NKikimr {
namespace NGRpcService {

using namespace Draft::Dummy;

using TEvInfiniteRequest = TGRpcRequestWrapper<0, InfiniteRequest, InfiniteResponse, true>;

static void HandlePing(NYdbGrpc::IRequestContextBase* ctx) {
    auto req = static_cast<const PingRequest*>(ctx->GetRequest());
    auto resp = google::protobuf::Arena::CreateMessage<PingResponse>(ctx->GetArena());
    if (req->copy()) {
        resp->set_payload(req->payload());
    }
    ctx->Reply(resp, Ydb::StatusIds::SUCCESS);
}

class TInfiniteRpc : public TRpcOperationRequestActor<TInfiniteRpc, TEvInfiniteRequest> {
    using TBase = TRpcOperationRequestActor<TInfiniteRpc, TEvInfiniteRequest>;

public:
    TInfiniteRpc(TEvInfiniteRequest* request)
        : TBase(request) {}

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);

        Become(&TInfiniteRpc::StateWork);
    }

    bool HasCancelOperation() {
        return true;
    }

    void OnCancelOperation(const TActorContext& ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Operation cancelled."));
        Reply(Ydb::StatusIds::CANCELLED, issues, ctx);
    }

private:
    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            default: TBase::StateFuncBase(ev);
        }
    }
};

TGRpcYdbDummyService::TGRpcYdbDummyService(NActors::TActorSystem* system, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId proxyActorId)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(proxyActorId)
{ }

void TGRpcYdbDummyService::InitService(grpc::ServerCompletionQueue* cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcYdbDummyService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Draft::Dummy::IN, Draft::Dummy::OUT, TGRpcYdbDummyService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Draft::Dummy::DummyService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("dummy", #NAME))->Run();

    ADD_REQUEST(Ping, PingRequest, PingResponse, {
        HandlePing(ctx);
    })

    ADD_REQUEST(Infinite, InfiniteRequest, InfiniteResponse, {
        ActorSystem_->Register(new TInfiniteRpc(new TEvInfiniteRequest(ctx)));
    })
#undef ADD_REQUEST

    // Example of using biStreamingRequest with GRpcRequestProxy
    {
        using TBiRequest = Draft::Dummy::PingRequest;
        using TBiResponse = Draft::Dummy::PingResponse;
        using TBiStreamGRpcRequest = NGRpcServer::TGRpcStreamingRequest<
            TBiRequest,
            TBiResponse,
            TGRpcYdbDummyService,
            413>;

        TBiStreamGRpcRequest::Start(
            this,
            this->GetService(),
            CQ_,
            &Draft::Dummy::DummyService::AsyncService::RequestBiStreamPing,
            [this](TIntrusivePtr<TBiStreamGRpcRequest::IContext> context) {
                ActorSystem_->Send(
                    GRpcRequestProxyId_,
                    new TEvBiStreamPingRequest(
                        context,
                        NGRpcService::TRequestAuxSettings{
                            .EmptyDatabaseMode = EEmptyDatabaseMode::EmptyDatabaseForbidden
                        }
                    )
                );
            },
            *ActorSystem_, "BiStreamPing",
            getCounterBlock("dummy", "biStreamPing", true), nullptr);
    }
}

} // namespace NGRpcService
} // namespace NKikimr
