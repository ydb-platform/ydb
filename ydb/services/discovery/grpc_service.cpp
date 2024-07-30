#include "grpc_service.h"
#include <ydb/core/grpc_services/service_discovery.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
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

void TGRpcDiscoveryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
     auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
     using namespace Ydb;
#ifdef ADD_REQUEST
#error macro already defined
#endif
#define ADD_REQUEST(NAME, CB) \
    MakeIntrusive<TGRpcRequest<Discovery::NAME##Request, Discovery::NAME##Response, TGRpcDiscoveryService>>   \
        (this, &Service_, CQ_,                                                                                \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                         \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer(), GetSdkBuildInfo(ctx));        \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                       \
                    new TGrpcRequestOperationCall<Discovery::NAME##Request, Discovery::NAME##Response>        \
                        (ctx, CB, TRequestAuxSettings{RLSWITCH(TRateLimiterMode::Rps), nullptr}));                     \
            }, &Ydb::Discovery::V1::DiscoveryService::AsyncService::Request ## NAME,                          \
            #NAME, logger, getCounterBlock("discovery", #NAME))->Run();

    ADD_REQUEST(WhoAmI, &DoWhoAmIRequest)
    ADD_REQUEST(NodeRegistration, &DoNodeRegistrationRequest)

#ifdef ADD_LEGACY_REQUEST
#error macro already defined
#endif
#define ADD_LEGACY_REQUEST(NAME, IN, OUT, ACTION)                                                                     \
    MakeIntrusive<TGRpcRequest<Ydb::Discovery::IN, Ydb::Discovery::OUT, TGRpcDiscoveryService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *reqCtx) {                                                                  \
           NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer(), GetSdkBuildInfo(reqCtx));               \
           ACTION;                                                                                                    \
        }, &Ydb::Discovery::V1::DiscoveryService::AsyncService::Request ## NAME,                                      \
        #NAME, logger, getCounterBlock("discovery", #NAME))->Run();

     ADD_LEGACY_REQUEST(ListEndpoints, ListEndpointsRequest, ListEndpointsResponse, {
         ActorSystem_->Send(GRpcRequestProxyId_, new TEvListEndpointsRequest(reqCtx));
     })

#undef ADD_REQUEST
#undef ADD_LEGACY_REQUEST
 }

} // namespace NGRpcService
} // namespace NKikimr
