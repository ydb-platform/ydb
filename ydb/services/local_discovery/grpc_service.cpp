#include "grpc_func_call.h"
#include "grpc_service.h"

#include <ydb/core/grpc_services/service_discovery.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>
#include <ydb/library/grpc/server/grpc_method_setup.h>

#include <util/system/hostname.h>

namespace {

void FillEnpointInfo(const TString& host, ui32 port, const TString& publicHost, ui32 publicPort, bool ssl, Ydb::Discovery::EndpointInfo& info) {
    auto effectivePublicHost = publicHost ? publicHost : host;
    auto effectivePublicPort = publicPort ? publicPort : port;
    info.set_address(effectivePublicHost);
    info.set_port(effectivePublicPort);
    info.set_ssl(ssl);
}

TString InferPublicHostFromServerHost(const TString& serverHost) {
    return serverHost && serverHost != "[::]" ? serverHost : FQDNHostName();
}

void AddEndpointsForGrpcConfig(const NKikimrConfig::TGRpcConfig& grpcConfig, Ydb::Discovery::ListEndpointsResult& result) {
    const TString& address = InferPublicHostFromServerHost(grpcConfig.GetHost());
    if (const ui32 port = grpcConfig.GetPort()) {
        FillEnpointInfo(address, port, grpcConfig.GetPublicHost(), grpcConfig.GetPublicPort(), false, *result.add_endpoints());
    }

    if (const ui32 sslPort = grpcConfig.GetSslPort()) {
        FillEnpointInfo(address, sslPort, grpcConfig.GetPublicHost(), grpcConfig.GetPublicSslPort(), true, *result.add_endpoints());
    }
}

}

namespace NKikimr {
namespace NGRpcService {

TGRpcLocalDiscoveryService::TGRpcLocalDiscoveryService(const NKikimrConfig::TGRpcConfig& grpcConfig,
                                NActors::TActorSystem *system,
                                TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                                NActors::TActorId id)
    : GrpcConfig(grpcConfig)
    , ActorSystem_(system)
    , Counters_(counters)
    , GRpcRequestProxyId_(id)
{
}

void TGRpcLocalDiscoveryService::InitService(grpc::ServerCompletionQueue *cq, NYdbGrpc::TLoggerPtr logger) {
    CQ_ = cq;
    SetupIncomingRequests(std::move(logger));
}

void TGRpcLocalDiscoveryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    using namespace std::placeholders;
    using namespace Ydb::Discovery;
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    ReportSdkBuildInfo();

#ifdef SETUP_LOCAL_DISCOVERY_METHOD
#error SETUP_LOCAL_DISCOVERY_METHOD macro already defined
#endif

#define SETUP_LOCAL_DISCOVERY_METHOD(methodName, methodCallback, rlMode, requestType, auditMode, operationCallClass) \
    SETUP_RUNTIME_EVENT_METHOD(methodName,                    \
        YDB_API_DEFAULT_REQUEST_TYPE(methodName),             \
        YDB_API_DEFAULT_RESPONSE_TYPE(methodName),            \
        methodCallback,                                       \
        rlMode,                                               \
        requestType,                                          \
        YDB_API_DEFAULT_COUNTER_BLOCK(discovery, methodName), \
        auditMode,                                            \
        COMMON,                                               \
        operationCallClass,                                   \
        GRpcRequestProxyId_,                                  \
        CQ_,                                                  \
        nullptr,                                              \
        nullptr)

    SETUP_LOCAL_DISCOVERY_METHOD(WhoAmI, DoWhoAmIRequest, RLMODE(Rps), DISCOVERY_WHOAMI, TAuditMode::NonModifying(), TGrpcRequestOperationCall);
    SETUP_LOCAL_DISCOVERY_METHOD(NodeRegistration, DoNodeRegistrationRequest, RLMODE(Rps), DISCOVERY_NODEREGISTRATION, TAuditMode::Modifying(TAuditMode::TLogClassConfig::NodeRegistration), TGrpcRequestOperationCall);
    SETUP_LOCAL_DISCOVERY_METHOD(ListEndpoints, std::bind(&TGRpcLocalDiscoveryService::DoListEndpointsRequest, this, _1, _2), RLMODE(Rps), DISCOVERY_LISTENDPOINTS, TAuditMode::NonModifying(), TGrpcRequestFunctionCall);

#undef SETUP_LOCAL_DISCOVERY_METHOD
}

void TGRpcLocalDiscoveryService::DoListEndpointsRequest(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
    auto *response = TEvListEndpointsRequest::AllocateResult<Ydb::Discovery::ListEndpointsResult>(request);
    AddEndpointsForGrpcConfig(GrpcConfig, *response);
    request->SendResult(*response, Ydb::StatusIds::SUCCESS);
}

} // namespace NGRpcService
} // namespace NKikimr
