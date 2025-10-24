#pragma once

#include "counters.h"
#include "utils.h"

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/config/protos/issue_id.pb.h>
#include <ydb/core/fq/libs/config/yq_issue.h>
#include <ydb/core/fq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/fq/libs/control_plane_proxy/utils/config.h>
#include <ydb/core/fq/libs/control_plane_proxy/utils/logs.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/rate_limiter/events/control_plane_events.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/protobuf_printer/security_printer.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/maybe.h>

#include <contrib/libs/fmt/include/fmt/format.h>

namespace NFq::NPrivate {

template <class TRequest, class TResponse, class TRequestProxy, class TResponseProxy>
class TRequestActor : public NActors::TActorBootstrapped<TRequestActor<TRequest, TResponse, TRequestProxy, TResponseProxy>> {
protected:
    using TBase = NActors::TActorBootstrapped<TRequestActor>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;
    using TBase::Schedule;

    typename TRequestProxy::TPtr RequestProxy;
    TControlPlaneProxyConfig Config;
    TActorId ServiceId;
    TRequestCounters Counters;
    TInstant StartTime;
    std::function<void(const TDuration&, bool /* isSuccess */, bool /* isTimeout */)> Probe;
    TPermissions Permissions;
    ui32 RetryCount = 0;
    bool ReplyWithResponseOnSuccess = true;

public:
    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_REQUEST_ACTOR";

    TRequestActor(typename TRequestProxy::TPtr requestProxy,
                  const TControlPlaneProxyConfig& config,
                  const TActorId& serviceId,
                  const TRequestCounters& counters,
                  const std::function<void(const TDuration&, bool, bool)>& probe,
                  const TPermissions& availablePermissions,
                  bool replyWithResponseOnSuccess = true)
        : RequestProxy(requestProxy)
        , Config(config)
        , ServiceId(serviceId)
        , Counters(counters)
        , StartTime(TInstant::Now())
        , Probe(probe)
        , Permissions(ExtractPermissions(RequestProxy, availablePermissions))
        , ReplyWithResponseOnSuccess(replyWithResponseOnSuccess) {
        Counters.IncInFly();
    }

public:
    void Bootstrap() {
        CPP_LOG_T("Request actor. Actor id: " << SelfId());
        Become(&TRequestActor::StateFunc,
               Config.RequestTimeout,
               new NActors::TEvents::TEvWakeup());
        Send(ControlPlaneConfigActorId(),
             new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
        OnBootstrap();
    }

    virtual void OnBootstrap() {}

    STRICT_STFUNC(StateFunc, cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
                  hFunc(TResponse, Handle);
                  cFunc(TEvControlPlaneConfig::EvGetTenantInfoRequest, HandleRetry);
                  hFunc(TEvControlPlaneConfig::TEvGetTenantInfoResponse, Handle);)

    void HandleRetry() {
        Send(ControlPlaneConfigActorId(),
             new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
    }

    void Handle(TEvControlPlaneConfig::TEvGetTenantInfoResponse::TPtr& ev) {
        RequestProxy->Get()->TenantInfo = std::move(ev->Get()->TenantInfo);
        if (RequestProxy->Get()->TenantInfo) {
            SendRequestIfCan();
        } else {
            RetryCount++;
            Schedule(Now() + Config.ConfigRetryPeriod * (1 << RetryCount),
                     new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
        }
    }

    void HandleTimeout() {
        CPP_LOG_D("Request timeout. " << NKikimr::SecureDebugString(RequestProxy->Get()->Request));
        NYql::TIssues issues;
        NYql::TIssue issue =
            MakeErrorIssue(TIssuesIds::TIMEOUT,
                           "Request timeout. Try repeating the request later");
        issues.AddIssue(issue);
        Counters.IncTimeout();
        ReplyWithError(issues, true);
    }

    void Handle(typename TResponse::TPtr& ev) {
        auto& response = *ev->Get();
        ProcessResponse(response);
    }

    template<typename T>
    void ProcessResponse(const T& response) {
        if (response.Issues) {
            ReplyWithError(response.Issues);
        } else {
            ReplyWithSuccess(response.Result);
        }
    }

    template<typename T>
        requires requires(T t) { t.AuditDetails; }
    void ProcessResponse(const T& response) {
        if (response.Issues) {
            ReplyWithError(response.Issues);
        } else {
            ReplyWithSuccess(response.Result, response.AuditDetails);
        }
    }

    void ReplyWithError(const NYql::TIssues& issues, bool isTimeout = false) {
        const TDuration delta = TInstant::Now() - StartTime;
        Counters.IncError();
        Probe(delta, false, isTimeout);
        Send(RequestProxy->Sender, new TResponseProxy(issues, RequestProxy->Get()->SubjectType), 0, RequestProxy->Cookie);
        PassAway();
    }

    template<class... TArgs>
    void ReplyWithSuccess(TArgs&&... args) {
        const TDuration delta = TInstant::Now() - StartTime;
        Counters.IncOk();
        Probe(delta, true, false);
        if (ReplyWithResponseOnSuccess) {
            Send(RequestProxy->Sender,
                 new TResponseProxy(std::forward<TArgs>(args)..., RequestProxy->Get()->SubjectType),
                 0,
                 RequestProxy->Cookie);
        } else {
            RequestProxy->Get()->Response =
                std::make_unique<TResponseProxy>(std::forward<TArgs>(args)..., RequestProxy->Get()->SubjectType);
            RequestProxy->Get()->ControlPlaneYDBOperationWasPerformed = true;
            Send(RequestProxy->Forward(ControlPlaneProxyActorId()));
        }
        PassAway();
    }

    virtual bool CanSendRequest() const { return bool(RequestProxy->Get()->TenantInfo); }

    void SendRequestIfCan() {
        if (CanSendRequest()) {
            Send(ServiceId,
                 new TRequest(RequestProxy->Get()->Scope,
                              RequestProxy->Get()->Request,
                              RequestProxy->Get()->User,
                              RequestProxy->Get()->Token,
                              RequestProxy->Get()->CloudId,
                              Permissions,
                              RequestProxy->Get()->Quotas,
                              RequestProxy->Get()->TenantInfo,
                              RequestProxy->Get()->ComputeDatabase.GetOrElse({})),
                 0,
                 RequestProxy->Cookie);
        }
    }

    virtual ~TRequestActor() {
        Counters.DecInFly();
        Counters.Common->LatencyMs->Collect((TInstant::Now() - StartTime).MilliSeconds());
    }
};

class TCreateQueryRequestActor :
    public TRequestActor<TEvControlPlaneStorage::TEvCreateQueryRequest,
                         TEvControlPlaneStorage::TEvCreateQueryResponse,
                         TEvControlPlaneProxy::TEvCreateQueryRequest,
                         TEvControlPlaneProxy::TEvCreateQueryResponse> {

    bool QuoterResourceCreated = false;

public:
    using TBaseRequestActor = TRequestActor<TEvControlPlaneStorage::TEvCreateQueryRequest,
                                            TEvControlPlaneStorage::TEvCreateQueryResponse,
                                            TEvControlPlaneProxy::TEvCreateQueryRequest,
                                            TEvControlPlaneProxy::TEvCreateQueryResponse>;

    TCreateQueryRequestActor(typename TEvControlPlaneProxy::TEvCreateQueryRequest::TPtr requestProxy,
                            const TControlPlaneProxyConfig& config,
                            const TActorId& serviceId,
                            const TRequestCounters& counters,
                            const TRequestCommonCountersPtr& rateLimiterCounters,
                            const std::function<void(const TDuration&, bool, bool)>& probe,
                            const TPermissions& availablePermissions,
                            bool replyWithResponseOnSuccess = true)
        : TBaseRequestActor(requestProxy, config, serviceId, counters, probe, availablePermissions, replyWithResponseOnSuccess)
        , RateLimiterCounters(rateLimiterCounters) {
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRateLimiter::TEvCreateResourceResponse, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
            default:
                return TBaseRequestActor::StateFunc(ev);
        }
    }

    bool ShouldCreateRateLimiter() const {
        return RequestProxy->Get()->Quotas
                && (RequestProxy->Get()->Request.content().type() == FederatedQuery::QueryContent::STREAMING
                    || !Config.ComputeConfig.YdbComputeControlPlaneEnabled(RequestProxy->Get()->Scope));
    }

    void HandleTimeout() {
        // Don't need to set the RateLimiterCreationInProgress = false
        // because of the PassAway will be called in this callback
        if (RateLimiterCreationInProgress) {
            RateLimiterCounters->Timeout->Inc();
            RateLimiterCounters->InFly->Dec();
        }
        TBaseRequestActor::HandleTimeout();
    }

    void OnBootstrap() override {
        this->UnsafeBecome(&TCreateQueryRequestActor::StateFunc);
        if (ShouldCreateRateLimiter()) {
            SendCreateRateLimiterResourceRequest();
        } else {
            SendRequestIfCan();
        }
    }

    void SendCreateRateLimiterResourceRequest() {
        if (auto quotaIt = RequestProxy->Get()->Quotas->find(QUOTA_CPU_PERCENT_LIMIT); quotaIt != RequestProxy->Get()->Quotas->end()) {
            const double cloudLimit = static_cast<double>(quotaIt->second.Limit.Value *
                                                          10); // percent -> milliseconds
            CPP_LOG_T("Create rate limiter resource for cloud with limit " << cloudLimit
                                                                           << "ms");
            RateLimiterCreationInProgress = true;
            RateLimiterCounters->InFly->Inc();
            StartRateLimiterCreation = TInstant::Now();
            Send(RateLimiterControlPlaneServiceId(),
                 new TEvRateLimiter::TEvCreateResource(RequestProxy->Get()->CloudId, cloudLimit));
        } else {
            RateLimiterCounters->Error->Inc();
            NYql::TIssues issues;
            NYql::TIssue issue =
                MakeErrorIssue(TIssuesIds::INTERNAL_ERROR,
                               TStringBuilder() << "CPU quota for cloud \"" << RequestProxy->Get()->CloudId
                                                << "\" was not found");
            issues.AddIssue(issue);
            CPP_LOG_W("Failed to get cpu quota for cloud " << RequestProxy->Get()->CloudId);
            ReplyWithError(issues);
        }
    }

    void Handle(TEvRateLimiter::TEvCreateResourceResponse::TPtr& ev) {
        RateLimiterCreationInProgress = false;
        RateLimiterCounters->InFly->Dec();
        RateLimiterCounters->LatencyMs->Collect((TInstant::Now() - StartRateLimiterCreation).MilliSeconds());
        CPP_LOG_D(
            "Create response from rate limiter service. Success: " << ev->Get()->Success);
        if (ev->Get()->Success) {
            RateLimiterCounters->Ok->Inc();
            QuoterResourceCreated = true;
            SendRequestIfCan();
        } else {
            RateLimiterCounters->Error->Inc();
            NYql::TIssue issue("Failed to create rate limiter resource");
            for (const NYql::TIssue& i : ev->Get()->Issues) {
                issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
            }
            NYql::TIssues issues;
            issues.AddIssue(issue);
            ReplyWithError(issues);
        }
    }

    bool CanSendRequest() const override {
        return (QuoterResourceCreated || !ShouldCreateRateLimiter()) && TBaseRequestActor::CanSendRequest();
    }

private:
    TInstant StartRateLimiterCreation;
    bool RateLimiterCreationInProgress = false;
    TRequestCommonCountersPtr RateLimiterCounters;
};

} // namespace NFq::NPrivate
