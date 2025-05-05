#include "ydb_bscontroller.h"

#include <ydb/core/grpc_services/service_bscontroller.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>

namespace NKikimr {
namespace NGRpcService {

void TGRpcYdbBsControllerService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, IN, OUT, CB) \
    MakeIntrusive<TGRpcRequest<Ydb::BsController::IN, Ydb::BsController::OUT, TGRpcYdbBsControllerService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *ctx) { \
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer()); \
            ActorSystem_->Send(GRpcRequestProxyId_, \
                new NGRpcService::TGrpcRequestNoOperationCall<Ydb::BsController::IN, Ydb::BsController::OUT> \
                    (ctx, &CB, NGRpcService::TRequestAuxSettings{NGRpcService::TRateLimiterMode::Off, nullptr})); \
        }, &Ydb::BsController::V1::BsControllerService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("bscontroller-describe", #NAME))->Run();

    ADD_REQUEST(Describe, DescribeRequest, DescribeResponse, DoBsControllerDescribeRequest);
#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
