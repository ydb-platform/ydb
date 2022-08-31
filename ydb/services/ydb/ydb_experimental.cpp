#include "ydb_experimental.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbExperimentalService::SetupIncomingRequests(NGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Experimental::IN, Ydb::Experimental::OUT, TGRpcYdbExperimentalService>>(this, &Service_, CQ_, \
        [this](NGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ACTION; \
        }, &Ydb::Experimental::V1::ExperimentalService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("experimental", #NAME))->Run();

    ADD_REQUEST(ExecuteStreamQuery, ExecuteStreamQueryRequest, ExecuteStreamQueryResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvExperimentalStreamQueryRequest(ctx));
    })
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
