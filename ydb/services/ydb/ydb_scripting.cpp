#include "ydb_scripting.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/core/grpc_services/service_yql_scripting.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbScriptingService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
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
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Scripting::V1::ScriptingService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("scripting", #NAME))->Run();

    ADD_REQUEST(ExecuteYql, ExecuteYqlRequest, ExecuteYqlResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestOperationCall<ExecuteYqlRequest, ExecuteYqlResponse>
                (ctx, &DoExecuteYqlScript, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Ru), nullptr, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml)}));
    })

    ADD_REQUEST(StreamExecuteYql, ExecuteYqlRequest, ExecuteYqlPartialResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<ExecuteYqlRequest, ExecuteYqlPartialResponse>
                (ctx, &DoStreamExecuteYqlScript, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr, TAuditMode::Modifying(TAuditMode::TLogClassConfig::Dml)}));
    })

    ADD_REQUEST(ExplainYql, ExplainYqlRequest, ExplainYqlResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestOperationCall<ExplainYqlRequest, ExplainYqlResponse>
                (ctx, &DoExplainYqlScript, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr, TAuditMode::NonModifying()}));
    })
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
