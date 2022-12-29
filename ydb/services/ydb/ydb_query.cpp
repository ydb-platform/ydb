#include "ydb_query.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/counters/counters.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/query/service_query.h>

namespace NKikimr::NGRpcService {

void TGRpcYdbQueryService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    using Ydb::Query::ExecuteQueryRequest;
    using Ydb::Query::ExecuteQueryResponsePart;

    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Query::IN, Ydb::Query::OUT, TGRpcYdbQueryService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Query::V1::QueryService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("query", #NAME))->Run();

    ADD_REQUEST(ExecuteQuery, ExecuteQueryRequest, ExecuteQueryResponsePart, {
        ActorSystem_->Send(GRpcRequestProxyId_,
            new TGrpcRequestNoOperationCall<ExecuteQueryRequest, ExecuteQueryResponsePart>
                (ctx, &DoExecuteQueryRequest, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));
    })
#undef ADD_REQUEST
}

} // namespace NKikimr::NGRpcService
