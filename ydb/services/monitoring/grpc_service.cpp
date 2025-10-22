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
#define ADD_REQUEST_NEW(NAME, CB, AUDIT_MODE) \
     MakeIntrusive<TGRpcRequest<Monitoring::NAME##Request, Monitoring::NAME##Response, TGRpcMonitoringService>> \
         (this, &Service_, CQ_,                                                                                 \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                        \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer(), GetSdkBuildInfo(ctx));          \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                         \
                    new TGrpcRequestOperationCall<Monitoring::NAME##Request, Monitoring::NAME##Response>        \
                         (ctx, &CB, TRequestAuxSettings{TRateLimiterMode::Off, nullptr, AUDIT_MODE}));          \
            }, &Ydb::Monitoring::V1::MonitoringService::AsyncService::Request ## NAME,                          \
            #NAME, logger, getCounterBlock("monitoring", #NAME))->Run();

    ADD_REQUEST_NEW(SelfCheck, DoSelfCheckRequest, TAuditMode::NonModifying());

#define ADD_REQUEST_OLD(NAME, IN, OUT, ACTION) \
    MakeIntrusive<TGRpcRequest<Ydb::Monitoring::IN, Ydb::Monitoring::OUT, TGRpcMonitoringService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase* reqCtx) { \
           NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer(), GetSdkBuildInfo(reqCtx)); \
           ACTION; \
        }, &Ydb::Monitoring::V1::MonitoringService::AsyncService::Request ## NAME, \
        #NAME, logger, getCounterBlock("monitoring", #NAME))->Run();

    ADD_REQUEST_OLD(NodeCheck, NodeCheckRequest, NodeCheckResponse, {
        ActorSystem_->Send(GRpcRequestProxyId_, new TEvNodeCheckRequest(reqCtx));
    });


#undef ADD_REQUEST_NEW
#undef ADD_REQUEST_OLD
}

} // namespace NGRpcService
} // namespace NKikimr
