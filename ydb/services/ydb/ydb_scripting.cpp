#include "ydb_scripting.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_yql_scripting.h>

namespace NKikimr {
namespace NGRpcService {

TGRpcYdbScriptingService::TGRpcYdbScriptingService(NActors::TActorSystem *system,
    TIntrusivePtr<::NMonitoring::TDynamicCounters> counters, NActors::TActorId id)
    : ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id) {}

void TGRpcYdbScriptingService::InitService(grpc::ServerCompletionQueue *cq, NGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcYdbScriptingService::SetGlobalLimiterHandle(NGrpc::TGlobalLimiter* limiter) {
    Limiter_ = limiter;
}

bool TGRpcYdbScriptingService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcYdbScriptingService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcYdbScriptingService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    using Ydb::Scripting::ExecuteYqlRequest;
    using Ydb::Scripting::ExecuteYqlResponse;
    using Ydb::Scripting::ExecuteYqlPartialResponse;
    using Ydb::Scripting::ExplainYqlRequest;
    using Ydb::Scripting::ExplainYqlResponse;

    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Scripting::IN, Ydb::Scripting::OUT, TGRpcYdbScriptingService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Scripting::V1::ScriptingService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("scripting", #NAME))->Run();

    ADD_REQUEST(ExecuteYql, ExecuteYqlRequest, ExecuteYqlResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestOperationCall<ExecuteYqlRequest, ExecuteYqlResponse>
                (ctx, &DoExecuteYqlScript, TRequestAuxSettings{TRateLimiterMode::Ru, nullptr}));
    })

    ADD_REQUEST(StreamExecuteYql, ExecuteYqlRequest, ExecuteYqlPartialResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<ExecuteYqlRequest, ExecuteYqlPartialResponse>
                (ctx, &DoStreamExecuteYqlScript, TRequestAuxSettings{TRateLimiterMode::Rps, nullptr}));
    })

    ADD_REQUEST(ExplainYql, ExplainYqlRequest, ExplainYqlResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestOperationCall<ExplainYqlRequest, ExplainYqlResponse>
                (ctx, &DoExplainYqlScript, TRequestAuxSettings{TRateLimiterMode::Rps, nullptr}));
    })
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
