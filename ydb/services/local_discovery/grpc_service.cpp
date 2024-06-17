#include "grpc_func_call.h"
#include "grpc_service.h"

#include <ydb/core/grpc_services/service_discovery.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/rpc_calls.h>

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

static TString GetSdkBuildInfo(NYdbGrpc::IRequestContextBase* reqCtx) {
    const auto& res = reqCtx->GetPeerMetaValues(NYdb::YDB_SDK_BUILD_INFO_HEADER);
    if (res.empty()) {
        return {};
    }
    return TString{res[0]};
}

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

void TGRpcLocalDiscoveryService::SetGlobalLimiterHandle(NYdbGrpc::TGlobalLimiter *limiter) {
    Limiter_ = limiter;
}

bool TGRpcLocalDiscoveryService::IncRequest() {
    return Limiter_->Inc();
}

void TGRpcLocalDiscoveryService::DecRequest() {
    Limiter_->Dec();
    Y_ASSERT(Limiter_->GetCurrentInFlight() >= 0);
}

void TGRpcLocalDiscoveryService::SetupIncomingRequests(NYdbGrpc::TLoggerPtr logger) {
    auto getCounterBlock = CreateCounterCb(Counters_, ActorSystem_);
    using namespace Ydb;
#ifdef ADD_REQUEST
#error macro already defined
#endif

#define ADD_REQUEST(NAME, CB, REQUEST_TYPE) \
    MakeIntrusive<TGRpcRequest<Discovery::NAME##Request, Discovery::NAME##Response, TGRpcLocalDiscoveryService>>   \
        (this, &Service_, CQ_,                                                                                \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                      \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer(), GetSdkBuildInfo(ctx));        \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                       \
                    new TGrpcRequestOperationCall<Discovery::NAME##Request, Discovery::NAME##Response>        \
                        (ctx, CB, TRequestAuxSettings {                                                       \
                            .RlMode = TRateLimiterMode::Rps,                                                  \
                            .RequestType = NJaegerTracing::ERequestType::DISCOVERY_##REQUEST_TYPE,            \
                        }));                                                                                  \
            }, &Ydb::Discovery::V1::DiscoveryService::AsyncService::Request ## NAME,                          \
            #NAME, logger, getCounterBlock("discovery", #NAME))->Run();

    ADD_REQUEST(WhoAmI, &DoWhoAmIRequest, WHOAMI)
    ADD_REQUEST(NodeRegistration, &DoNodeRegistrationRequest, NODEREGISTRATION)
#undef ADD_REQUEST

using namespace std::placeholders;

#ifdef ADD_METHOD
#error macro already defined
#endif

#define ADD_METHOD(NAME, METHOD, REQUEST_TYPE) \
    MakeIntrusive<TGRpcRequest<Discovery::NAME##Request, Discovery::NAME##Response, TGRpcLocalDiscoveryService>>   \
        (this, &Service_, CQ_,                                                                                \
            [this](NYdbGrpc::IRequestContextBase *ctx) {                                                      \
                NGRpcService::ReportGrpcReqToMon(*ActorSystem_, ctx->GetPeer(), GetSdkBuildInfo(ctx));        \
                TFuncCallback cb = std::bind(&TGRpcLocalDiscoveryService::METHOD, this, _1, _2);              \
                ActorSystem_->Send(GRpcRequestProxyId_,                                                       \
                    new TGrpcRequestFunctionCall<Discovery::NAME##Request, Discovery::NAME##Response>         \
                        (ctx, cb, TRequestAuxSettings {                                                       \
                            .RlMode = TRateLimiterMode::Rps,                                                  \
                            .RequestType = NJaegerTracing::ERequestType::DISCOVERY_##REQUEST_TYPE,            \
                        }));                                                                                  \
            }, &Ydb::Discovery::V1::DiscoveryService::AsyncService::Request ## NAME,                          \
            #NAME, logger, getCounterBlock("discovery", #NAME))->Run();

    ADD_METHOD(ListEndpoints, DoListEndpointsRequest, LISTENDPOINTS)
#undef ADD_METHOD

}

void TGRpcLocalDiscoveryService::DoListEndpointsRequest(std::unique_ptr<IRequestOpCtx> request, const IFacilityProvider&) {
    auto *response = TEvListEndpointsRequest::AllocateResult<Ydb::Discovery::ListEndpointsResult>(request);
    AddEndpointsForGrpcConfig(GrpcConfig, *response);
    request->SendResult(*response, Ydb::StatusIds::SUCCESS);
}

} // namespace NGRpcService
} // namespace NKikimr
