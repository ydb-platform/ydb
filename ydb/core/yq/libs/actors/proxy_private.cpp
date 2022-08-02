#include "proxy_private.h"
#include <ydb/core/yq/libs/config/protos/yq_config.pb.h>

#include <ydb/core/yq/libs/events/events.h>
#include <ydb/core/yq/libs/shared_resources/db_pool.h>

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/yq/libs/config/yq_issue.h>

#include <library/cpp/actors/core/log.h>

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/system/hostname.h>

#define LOG_E(stream) \
    LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, stream)
#define LOG_I(stream) \
    LOG_INFO_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, stream)
#define LOG_D(stream) \
    LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, stream)

namespace NYq {

using namespace NActors;
using namespace NMonitoring;

class TYqlAnalyticsPrivateProxy : public NActors::TActorBootstrapped<TYqlAnalyticsPrivateProxy> {
public:
    TYqlAnalyticsPrivateProxy(
        const NConfig::TPrivateProxyConfig& privateProxyConfig,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider> randomProvider,
        ::NMonitoring::TDynamicCounterPtr counters,
        const NConfig::TTokenAccessorConfig& tokenAccessorConfig)
        : PrivateProxyConfig(privateProxyConfig)
        , TokenAccessorConfig(tokenAccessorConfig)
        , TimeProvider(timeProvider)
        , RandomProvider(randomProvider)
        , Counters(counters->GetSubgroup("subsystem", "private_api"))
    { }

    static constexpr char ActorName[] = "YQ_PRIVATE_PROXY";

    void PassAway() final {
        NActors::IActor::PassAway();
    }

    void Bootstrap(const TActorContext&) {
        Become(&TYqlAnalyticsPrivateProxy::StateFunc);
        Counters->GetCounter("EvBootstrap", true)->Inc();
    }

private:
    template<typename T>
    NYql::TIssues ValidatePermissions(T& ev) {
        NYql::TIssues issues;
        if (!PrivateProxyConfig.GetEnablePermissions()) {
            return issues;
        }

        TString user = ev->Get()->User;
        auto it = Find(PrivateProxyConfig.GetGrantedUsers(), user);
        if (it == PrivateProxyConfig.GetGrantedUsers().end()) {
            issues.AddIssue(MakeErrorIssue(NYq::TIssuesIds::UNAUTHORIZED, "Authentication error for the user " + user));
        }
        return issues;
    }

    void Handle(TEvents::TEvPingTaskRequest::TPtr& ev, const TActorContext& ctx) {
        Counters->GetCounter("EvPingTaskRequest", true)->Inc();

        NYql::TIssues issues = ValidatePermissions(ev);
        if (issues) {
            auto response = std::make_unique<TEvents::TEvPingTaskResponse>();
            response->Issues = issues;
            response->Status = Ydb::StatusIds::UNAUTHORIZED;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        Register(
            CreatePingTaskRequestActor(ev->Sender, TimeProvider, ev->Release(), Counters),
            NActors::TMailboxType::HTSwap, ctx.SelfID.PoolID());
    }

    void Handle(TEvents::TEvGetTaskRequest::TPtr& ev, const TActorContext& ctx) {
        Counters->GetCounter("EvGetTaskRequest", true)->Inc();

        NYql::TIssues issues = ValidatePermissions(ev);
        if (issues) {
            auto response = std::make_unique<TEvents::TEvGetTaskResponse>();
            response->Issues = issues;
            response->Status = Ydb::StatusIds::UNAUTHORIZED;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        Register(
            CreateGetTaskRequestActor(ev->Sender, TokenAccessorConfig, TimeProvider, ev->Release(), Counters),
            NActors::TMailboxType::HTSwap, ctx.SelfID.PoolID());
    }

    void Handle(TEvents::TEvWriteTaskResultRequest::TPtr& ev, const TActorContext& ctx) {
        Counters->GetCounter("EvWriteTaskResultRequest", true)->Inc();

        NYql::TIssues issues = ValidatePermissions(ev);
        if (issues) {
            auto response = std::make_unique<TEvents::TEvWriteTaskResultResponse>();
            response->Issues = issues;
            response->Status = Ydb::StatusIds::UNAUTHORIZED;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        Register(
            CreateWriteTaskResultRequestActor(ev->Sender, TimeProvider, ev->Release(), Counters),
            NActors::TMailboxType::HTSwap, ctx.SelfID.PoolID());
    }

    void Handle(TEvents::TEvNodesHealthCheckRequest::TPtr& ev, const TActorContext& ctx) {
        Counters->GetCounter("EvNodesHealthCheckRequest", true)->Inc();

        NYql::TIssues issues = ValidatePermissions(ev);
        if (issues) {
            auto response = std::make_unique<TEvents::TEvNodesHealthCheckResponse>();
            response->Issues = issues;
            response->Status = Ydb::StatusIds::UNAUTHORIZED;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        Register(
            CreateNodesHealthCheckActor(ev->Sender, TimeProvider, ev->Release(), Counters),
            NActors::TMailboxType::HTSwap, ctx.SelfID.PoolID());
    }

    void Handle(TEvents::TEvCreateRateLimiterResourceRequest::TPtr& ev) {
        Counters->GetCounter("EvCreateRateLimiterResourceRequest", true)->Inc();

        NYql::TIssues issues = ValidatePermissions(ev);
        if (issues) {
            auto response = std::make_unique<TEvents::TEvCreateRateLimiterResourceResponse>();
            response->Issues = issues;
            response->Status = Ydb::StatusIds::UNAUTHORIZED;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        auto sender = ev->Sender;
        Register(
            CreateCreateRateLimiterResourceRequestActor(sender, TimeProvider, ev->Release(), Counters),
            NActors::TMailboxType::HTSwap, SelfId().PoolID());
    }

    void Handle(TEvents::TEvDeleteRateLimiterResourceRequest::TPtr& ev) {
        Counters->GetCounter("EvDeleteRateLimiterResourceRequest", true)->Inc();

        NYql::TIssues issues = ValidatePermissions(ev);
        if (issues) {
            auto response = std::make_unique<TEvents::TEvDeleteRateLimiterResourceResponse>();
            response->Issues = issues;
            response->Status = Ydb::StatusIds::UNAUTHORIZED;
            Send(ev->Sender, response.release(), 0, ev->Cookie);
            return;
        }

        auto sender = ev->Sender;
        Register(
            CreateDeleteRateLimiterResourceRequestActor(sender, TimeProvider, ev->Release(), Counters),
            NActors::TMailboxType::HTSwap, SelfId().PoolID());
    }

    STRICT_STFUNC(
        StateFunc,
        HFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
        HFunc(TEvents::TEvPingTaskRequest, Handle)
        HFunc(TEvents::TEvGetTaskRequest, Handle)
        HFunc(TEvents::TEvWriteTaskResultRequest, Handle)
        HFunc(TEvents::TEvNodesHealthCheckRequest, Handle)
        hFunc(TEvents::TEvCreateRateLimiterResourceRequest, Handle)
        hFunc(TEvents::TEvDeleteRateLimiterResourceRequest, Handle)
        )

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&, const NActors::TActorContext&) {
        LOG_E("TYqlAnalyticsPrivateProxy::OnUndelivered");
        Counters->GetCounter("OnUndelivered", true)->Inc();
    }

private:
    const NConfig::TPrivateProxyConfig PrivateProxyConfig;
    const NConfig::TTokenAccessorConfig TokenAccessorConfig;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    ::NMonitoring::TDynamicCounterPtr Counters;
};

TActorId MakeYqPrivateProxyId() {
    constexpr TStringBuf name = "YQPRIVPROXY";
    return NActors::TActorId(0, name);
}

IActor* CreateYqlAnalyticsPrivateProxy(
    const NConfig::TPrivateProxyConfig& privateProxyConfig,
    TIntrusivePtr<ITimeProvider> timeProvider,
    TIntrusivePtr<IRandomProvider> randomProvider,
    ::NMonitoring::TDynamicCounterPtr counters,
    const NConfig::TTokenAccessorConfig& tokenAccessorConfig) {
    return new TYqlAnalyticsPrivateProxy(privateProxyConfig, timeProvider, randomProvider, counters, tokenAccessorConfig);
}

} // namespace NYq
