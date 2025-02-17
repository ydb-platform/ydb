#include "grpc_service.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_view.h>

namespace NKikimr::NGRpcService {

void TGRpcViewService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace Ydb::View;

    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);

    MakeIntrusive<TGRpcRequest<DescribeViewRequest,
                               DescribeViewResponse,
                               TGRpcViewService>>(
        this, &Service_, CQ_,
        [this](NYdbGrpc::IRequestContextBase* ctx) {
            NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer());
            ActorSystem_->Send(
                GRpcRequestProxyId_,
                new TGrpcRequestOperationCall<DescribeViewRequest,
                                              DescribeViewResponse>(
                    ctx, &DoDescribeView,
                    TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}
                )
            );
        },
        &V1::ViewService::AsyncService::RequestDescribeView,
        "DescribeView", logger, getCounterBlock("view", "DescribeView")
    )->Run();
}

}
