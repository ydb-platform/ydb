#include "grpc_proxy_counters.h"
#include "grpc_request_check_actor.h"
#include "grpc_request_proxy.h"
#include "operation_helpers.h"

#include <ydb/core/base/appdata.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;


class TGRpcRequestProxySimple
    : public TActorBootstrapped<TGRpcRequestProxySimple>
    , public IFacilityProvider
{
    using TBase = TActorBootstrapped<TGRpcRequestProxySimple>;
public:
    explicit TGRpcRequestProxySimple(const NKikimrConfig::TAppConfig& appConfig)
        : AppConfig(appConfig)
    {
    }

    void Bootstrap(const TActorContext& ctx);
    void StateFunc(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx);

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
        } else {
            THolder<TEvListEndpointsRequest> Request(event->Release().Release());
            auto *result = TEvListEndpointsRequest::AllocateResult<Ydb::Discovery::ListEndpointsResult>(Request);
            auto xres = result->add_endpoints();
            auto publicHost = AppConfig.GetGRpcConfig().GetPublicHost();
            xres->set_address(publicHost ? publicHost : AppConfig.GetGRpcConfig().GetHost()); //TODO: use cfg
            auto publicPort = AppConfig.GetGRpcConfig().GetPublicPort();
            xres->set_port(publicPort ? publicPort : AppConfig.GetGRpcConfig().GetPort());
            Request->SendResult(*result, Ydb::StatusIds::SUCCESS);
        }
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

        if (state.State == NGrpc::TAuthState::AS_FAIL) {
            requestBaseCtx->ReplyUnauthenticated();
            return;
        }

        if (state.State == NGrpc::TAuthState::AS_UNAVAILABLE) {
            Counters->IncDatabaseUnavailableCounter();
            const TString error = "Unable to resolve token";
            const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::YDB_AUTH_UNAVAILABLE, error);
            requestBaseCtx->RaiseIssue(issue);
            requestBaseCtx->ReplyUnavaliable();
            return;
        }

        {
            TSchemeBoardEvents::TDescribeSchemeResult schemeData;
            Register(CreateGrpcRequestCheckActor<TEvent>(SelfId(), schemeData, nullptr, event.Release(), Counters, false));
            return;
        }

        // in case we somehow skipped all auth checks
        const auto issue = MakeIssue(NKikimrIssues::TIssuesIds::GENERIC_TXPROXY_ERROR, "Can't authenticate request");
        requestBaseCtx->RaiseIssue(issue);
        requestBaseCtx->ReplyWithYdbStatus(Ydb::StatusIds::BAD_REQUEST);
    }

    const NKikimrConfig::TAppConfig& GetAppConfig() const override {
        return AppConfig;
    }

    NKikimrConfig::TAppConfig AppConfig;
    TIntrusivePtr<TGrpcProxyCounters> Counters;
};

void TGRpcRequestProxySimple::Bootstrap(const TActorContext& ctx) {
    auto nodeID = SelfId().NodeId();

    LOG_NOTICE(ctx, NKikimrServices::GRPC_SERVER, "Grpc simple request proxy started, nodeid# %d", nodeID);

    Counters = MakeIntrusive<TGrpcProxyCounters>(AppData()->Counters);

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
    return state.State == NGrpc::TAuthState::AS_OK ||
           state.State == NGrpc::TAuthState::AS_FAIL && state.NeedAuth == false ||
           state.NeedAuth == false && !ctx.GetYdbToken();
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
        LOG_NOTICE(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "%s", getDebugString().c_str());
    }
    else {
        LOG_DEBUG(*TlsActivationContext, NKikimrServices::GRPC_SERVER, "%s", getDebugString().c_str());
    }
}

void TGRpcRequestProxySimple::StateFunc(TAutoPtr<IEventHandle>& ev, const TActorContext& ctx) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvents::TEvUndelivered, HandleUndelivery);
        HFunc(TEvListEndpointsRequest, PreHandle);
        HFunc(TEvProxyRuntimeEvent, PreHandle);
        default:
            Y_FAIL("Unknown request: %u\n", ev->GetTypeRewrite());
        break;
    }
}

IActor* CreateGRpcRequestProxySimple(const NKikimrConfig::TAppConfig& appConfig) {
    return new TGRpcRequestProxySimple(appConfig);
}

} // namespace NGRpcService
} // namespace NKikimr
