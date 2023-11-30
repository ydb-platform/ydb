#include "proxy_private.h"
#include <ydb/core/fq/libs/config/protos/fq_config.pb.h>

#include <ydb/core/fq/libs/events/events.h>

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/actors/core/log.h>

#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/system/hostname.h>

#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, stream)
#define LOG_I(stream) LOG_INFO_S (*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::YQL_PRIVATE_PROXY, stream)

namespace NFq {

using namespace NActors;
using namespace NMonitoring;

class TYqlAnalyticsPrivateProxy : public NActors::TActorBootstrapped<TYqlAnalyticsPrivateProxy> {
public:
    TYqlAnalyticsPrivateProxy(
        const NConfig::TPrivateProxyConfig& privateProxyConfig,
        TIntrusivePtr<ITimeProvider> timeProvider,
        TIntrusivePtr<IRandomProvider> randomProvider,
        ::NMonitoring::TDynamicCounterPtr counters,
        const ::NFq::TSigner::TPtr& signer)
        : PrivateProxyConfig(privateProxyConfig)
        , Signer(signer)
        , TimeProvider(timeProvider)
        , RandomProvider(randomProvider)
        , Counters(counters->GetSubgroup("subsystem", "private_api"))
    { }

    static constexpr char ActorName[] = "YQ_PRIVATE_PROXY";

    void PassAway() final {
        NActors::IActor::PassAway();
    }

    void Bootstrap() {
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
            issues.AddIssue(MakeErrorIssue(NFq::TIssuesIds::UNAUTHORIZED, "Authentication error for the user " + user));
        }
        return issues;
    }

    void Handle(TEvents::TEvPingTaskRequest::TPtr& ev) {
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
            NActors::TMailboxType::HTSwap, SelfId().PoolID());
    }

    void Handle(TEvents::TEvGetTaskRequest::TPtr& ev) {
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
            CreateGetTaskRequestActor(ev->Sender, Signer, TimeProvider, ev->Release(), Counters),
            NActors::TMailboxType::HTSwap, SelfId().PoolID());
    }

    void Handle(TEvents::TEvWriteTaskResultRequest::TPtr& ev) {
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
            NActors::TMailboxType::HTSwap, SelfId().PoolID());
    }

    void Handle(TEvents::TEvNodesHealthCheckRequest::TPtr& ev) {
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
            NActors::TMailboxType::HTSwap, SelfId().PoolID());
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
        hFunc(NActors::TEvents::TEvUndelivered, OnUndelivered)
        hFunc(TEvents::TEvPingTaskRequest, Handle)
        hFunc(TEvents::TEvGetTaskRequest, Handle)
        hFunc(TEvents::TEvWriteTaskResultRequest, Handle)
        hFunc(TEvents::TEvNodesHealthCheckRequest, Handle)
        hFunc(TEvents::TEvCreateRateLimiterResourceRequest, Handle)
        hFunc(TEvents::TEvDeleteRateLimiterResourceRequest, Handle)
        )

    void OnUndelivered(NActors::TEvents::TEvUndelivered::TPtr&) {
        LOG_E("TYqlAnalyticsPrivateProxy::OnUndelivered");
        Counters->GetCounter("OnUndelivered", true)->Inc();
    }

private:
    const NConfig::TPrivateProxyConfig PrivateProxyConfig;
    const ::NFq::TSigner::TPtr Signer;
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
    const ::NFq::TSigner::TPtr& signer) {
    return new TYqlAnalyticsPrivateProxy(privateProxyConfig, timeProvider, randomProvider, counters, signer);
}

} // namespace NFq
