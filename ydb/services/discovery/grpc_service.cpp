#include "grpc_service.h"
#include <ydb/core/grpc_services/service_discovery.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

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
    using namespace Ydb::Discovery;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    ReportSdkBuildInfo();

#ifdef SETUP_DISCOVERY_METHOD
#error SETUP_DISCOVERY_METHOD macro already defined
#endif

#define SETUP_DISCOVERY_METHOD(methodName, methodCallback, rlMode, requestType, auditMode) \
    SETUP_METHOD(methodName, methodCallback, rlMode, requestType, discovery, auditMode)

    SETUP_DISCOVERY_METHOD(WhoAmI, DoWhoAmIRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::NonModifying());
    SETUP_DISCOVERY_METHOD(NodeRegistration, DoNodeRegistrationRequest, RLSWITCH(Rps), UNSPECIFIED, TAuditMode::Modifying(TAuditMode::TLogClassConfig::NodeRegistration));

#ifdef SETUP_LEGACY_EVENT_METHOD
#error SETUP_LEGACY_EVENT_METHOD macro already defined
#endif

#define SETUP_LEGACY_EVENT_METHOD(NAME, IN, OUT, ACTION)                                                              \
    MakeIntrusive<TGRpcRequest<Ydb::Discovery::IN, Ydb::Discovery::OUT, TGRpcDiscoveryService>>(this, &Service_, CQ_, \
        [this](NYdbGrpc::IRequestContextBase *reqCtx) {                                                               \
           NGRpcService::ReportGrpcReqToMon(*ActorSystem_, reqCtx->GetPeer(), GetSdkBuildInfo(reqCtx));               \
           ACTION;                                                                                                    \
        }, &Ydb::Discovery::V1::DiscoveryService::AsyncService::Request ## NAME,                                      \
        #NAME, logger, getCounterBlock("discovery", #NAME))->Run();

     SETUP_LEGACY_EVENT_METHOD(ListEndpoints, ListEndpointsRequest, ListEndpointsResponse, {
         ActorSystem_->Send(GRpcRequestProxyId_, new TEvListEndpointsRequest(reqCtx));
     });

#undef SETUP_DISCOVERY_METHOD
#undef SETUP_LEGACY_EVENT_METHOD
 }

} // namespace NGRpcService
} // namespace NKikimr
