#include "grpc_request_check_actor.h"
#include "grpc_request_proxy.h"
#include "operation_helpers.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>

#include <util/system/hostname.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;

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

class TGRpcRequestProxySimple
    : public TActorBootstrapped<TGRpcRequestProxySimple>
    , public IFacilityProvider
{
    using TBase = TActorBootstrapped<TGRpcRequestProxySimple>;
public:
    explicit TGRpcRequestProxySimple(const NKikimrConfig::TAppConfig& appConfig)
        : AppConfig(appConfig)
        , ChannelBufferSize(appConfig.GetTableServiceConfig().GetResourceManager().GetChannelBufferSize())
    {
    }

    void Bootstrap(const TActorContext& ctx);
    void StateFunc(TAutoPtr<IEventHandle>& ev);

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_PROXY;
    }

private:
    void HandleUndelivery(TEvents::TEvUndelivered::TPtr& ev);

    static bool IsAuthStateOK(const IRequestProxyCtx& ctx);

    void Handle(TEvListEndpointsRequest::TPtr& event, const TActorContext&) {
        IRequestProxyCtx* requestBaseCtx = event->Get();
        TString validationError;
        if (!requestBaseCtx->Validate(validationError)) {
            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_API_VALIDATION_ERROR, validationError);
            requestBaseCtx->RaiseIssue(issue);
            requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::BAD_REQUEST);
            return;
        }

        THolder<TEvListEndpointsRequest> request(event->Release().Release());
        auto *result = TEvListEndpointsRequest::AllocateResult<Ydb::Discovery::ListEndpointsResult>(request);
        const auto& grpcConfig = AppConfig.GetGRpcConfig();
        AddEndpointsForGrpcConfig(grpcConfig, *result);

        for (const auto& externalEndpoint : grpcConfig.GetExtEndpoints()) {
            AddEndpointsForGrpcConfig(externalEndpoint, *result);
        }

        request->SendResult(*result, Ydb::StatusIds::SUCCESS);
    }

    void Handle(TEvProxyRuntimeEvent::TPtr& event, const TActorContext&) {
        IRequestProxyCtx* requestBaseCtx = event->Get();
        TString validationError;
        if (!requestBaseCtx->Validate(validationError)) {
            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_API_VALIDATION_ERROR, validationError);
            requestBaseCtx->RaiseIssue(issue);
            requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::BAD_REQUEST);
        } else {
            event->Release().Release()->Pass(*this);
        }
    }

    template <typename TEvent>
    void PreHandle(TAutoPtr<TEventHandle<TEvent>>& event, const TActorContext& ctx) {
        IRequestProxyCtx* requestBaseCtx = event->Get();

        LogRequest(event);

        if (IsAuthStateOK(*requestBaseCtx)) {
            Handle(event, ctx);
            return;
        }

        auto state = requestBaseCtx->GetAuthState();

        if (state.State == NYdbGrpc::TAuthState::AS_FAIL) {
            requestBaseCtx->ReplyUnauthenticated();
            return;
        }

        if (state.State == NYdbGrpc::TAuthState::AS_UNAVAILABLE) {
            Counters->IncDatabaseUnavailableCounter();
            const TString error = "Unable to resolve token";
            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_AUTH_UNAVAILABLE, error);
            requestBaseCtx->RaiseIssue(issue);
            requestBaseCtx->ReplyUnavaliable();
            return;
        }

        {
            TSchemeBoardEvents::TDescribeSchemeResult schemeData;
            Register(CreateGrpcRequestCheckActor<TEvent>(SelfId(), schemeData, nullptr, event.Release(), Counters, false, this));
            return;
        }

        // in case we somehow skipped all auth checks
        const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, "Can't authenticate request");
        requestBaseCtx->RaiseIssue(issue);
        requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::BAD_REQUEST);
    }

    ui64 GetChannelBufferSize() const override {
        return ChannelBufferSize.load();
    }

    TActorId RegisterActor(IActor* actor) const override {
        return TActivationContext::AsActorContext().Register(actor);
    }

    const NKikimrConfig::TAppConfig AppConfig;
    std::atomic<ui64> ChannelBufferSize;
    IGRpcProxyCounters::TPtr Counters;
};

void TGRpcRequestProxySimple::Bootstrap(const TActorContext& ctx) {
    auto nodeID = SelfId().NodeId();

    LOG_NOTICE(ctx, NKikimrServices::GRPC_SERVER, "Grpc simple request proxy started, nodeid# %d", nodeID);

    Counters = CreateGRpcProxyCounters(AppData()->Counters);
    InitializeGRpcProxyDbCountersRegistry(ctx.ActorSystem());

    Become(&TThis::StateFunc);
}

void TGRpcRequestProxySimple::HandleUndelivery(TEvents::TEvUndelivered::TPtr& ev) {
    switch (ev->Get()->SourceType) {
        default:
            LOG_ERROR(*TlsActivationContext, NKikimrServices::GRPC_SERVER,
                "Undelivered event with unexpected source type: %d", ev->Get()->SourceType);
            break;
    }
}

bool TGRpcRequestProxySimple::IsAuthStateOK(const IRequestProxyCtx& ctx) {
    const auto& state = ctx.GetAuthState();
    if (state.State == NYdbGrpc::TAuthState::AS_OK) {
        return true;
    }

    const bool authorizationParamsAreSet = ctx.GetYdbToken() || !ctx.FindClientCertPropertyValues().empty();
    if (!state.NeedAuth && !authorizationParamsAreSet) {
        return true;
    }

    if (!state.NeedAuth && state.State == NYdbGrpc::TAuthState::AS_FAIL) {
        if (AppData()->EnforceUserTokenCheckRequirement && authorizationParamsAreSet) {
            return false;
        }
        return true;
    }
    return false;
}

template<typename TEvent>
void LogRequest(const TEvent& event) {
    auto getDebugString = [&event]()->TString {
        TStringStream ss;
        ss << "Got grpc request# " << event->Get()->GetRequestName();
        ss << ", traceId# " << event->Get()->GetTraceId().GetOrElse("undef");
        ss << ", sdkBuildInfo# " << event->Get()->GetSdkBuildInfo().GetOrElse("undef");
        ss << ", state# " << event->Get()->GetAuthState().State;
        ss << ", database# " << event->Get()->GetDatabaseName().GetOrElse("undef");
        ss << ", grpcInfo# " << event->Get()->GetGrpcUserAgent().GetOrElse("undef");
        if (event->Get()->GetDeadline() == TInstant::Max()) {
            ss << ", timeout# undef";
        } else {
            ss << ", timeout# " << event->Get()->GetDeadline() - TInstant::Now();
        }
        return ss.Str();
    };

    if constexpr (std::is_same_v<TEvListEndpointsRequest::TPtr, TEvent>) {
        LOG_INFO(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "%s", getDebugString().c_str());
    }
    else {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "%s", getDebugString().c_str());
    }
}

void TGRpcRequestProxySimple::StateFunc(TAutoPtr<IEventHandle>& ev) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvents::TEvUndelivered, HandleUndelivery);
        HFunc(TEvListEndpointsRequest, PreHandle);
        HFunc(TEvProxyRuntimeEvent, PreHandle);
        default:
            Y_ABORT("Unknown request: %u\n", ev->GetTypeRewrite());
        break;
    }
}

IActor* CreateGRpcRequestProxySimple(const NKikimrConfig::TAppConfig& appConfig) {
    return new TGRpcRequestProxySimple(appConfig);
}

} // namespace NGRpcService
} // namespace NKikimr
