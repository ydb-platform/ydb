#include "grpc_service.h"

#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/service_monitoring.h>
#include <ydb/core/grpc_services/base/base.h>

#include <ydb/core/grpc_services/rpc_calls.h>

namespace NKikimr {
namespace NGRpcService {

static TString GetSdkBuildInfo(NYdbGrpc::IRequestContextBase* reqCtx) {
    const auto& res = reqCtx->GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    if (res.empty()) {
        return {};
    }
    return TString{res[0]};
}

void TGRpcMonitoringService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;

#ifdef ADD_REQUEST
#error ADD_REQUEST macro already defined
#endif
#define ADD_REQUEST(NAME, CB, AUDIT_MODE, TGrpcRequestOperationCallType) \
     MakeIntrusive<TGRpcRequest<Monitoring::NAME##Request, Monitoring::NAME##Response, TGRpcMonitoringService>> \
         (this, &Service_, CQ_,                                                                                 \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                        \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer(), GetSdkBuildInfo(ctx));          \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TGrpcRequestOperationCallType<Monitoring::NAME##Request, Monitoring::NAME##Response>        \
                         (ctx, &CB, TRequestAuxSettings{TRateLimiterMode::Off, nullptr, AUDIT_MODE}));          \
            }, &Ydb::Monitoring::V1::MonitoringService::AsyncService::Request ## NAME,                          \
            #NAME, logger, getCounterBlock("monitoring", #NAME))->Run();

    ADD_REQUEST(SelfCheck, DoSelfCheckRequest, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    ADD_REQUEST(ClusterState, DoClusterStateRequest, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    ADD_REQUEST(NodeCheck, DoNodeCheckRequest, TAuditMode::NonModifying(), TGrpcRequestOperationCallNoAuth);


#undef ADD_REQUEST
}

} // namespace NGRpcService
} // namespace NKikimr
