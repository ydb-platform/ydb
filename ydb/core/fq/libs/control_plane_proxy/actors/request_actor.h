#pragma once

#include "counters.h"
#include "utils.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <ydb/library/actors/core/event.h>
#include <util/generic/maybe.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NFq::NPrivate {

template<class TRequestProto, class TRequest, class TResponse, class TRequestProxy, class TResponseProxy>
class TRequestActor :
    public TActorBootstrapped<
        TRequestActor<TRequestProto, TRequest, TResponse, TRequestProxy, TResponseProxy>> {
protected:
    using TBase = TActorBootstrapped<TRequestActor>;
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
    ui32 RetryCount                 = 0;
    bool ReplyWithResponseOnSuccess = true;

public:
    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_REQUEST_ACTOR";

    explicit TRequestActor(typename TRequestProxy::TPtr requestProxy,
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

    virtual void OnBootstrap() { }

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
        CPP_LOG_D("Request timeout. " << RequestProxy->Get()->Request.DebugString());
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
    public TRequestActor<FederatedQuery::CreateQueryRequest,
                         TEvControlPlaneStorage::TEvCreateQueryRequest,
                         TEvControlPlaneStorage::TEvCreateQueryResponse,
                         TEvControlPlaneProxy::TEvCreateQueryRequest,
                         TEvControlPlaneProxy::TEvCreateQueryResponse> {
    bool QuoterResourceCreated = false;

public:
    using TBaseRequestActor = TRequestActor<FederatedQuery::CreateQueryRequest,
                                            TEvControlPlaneStorage::TEvCreateQueryRequest,
                                            TEvControlPlaneStorage::TEvCreateQueryResponse,
                                            TEvControlPlaneProxy::TEvCreateQueryRequest,
                                            TEvControlPlaneProxy::TEvCreateQueryResponse>;
    using TBaseRequestActor::TBaseRequestActor;

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvRateLimiter::TEvCreateResourceResponse, Handle);
            default:
                return TBaseRequestActor::StateFunc(ev);
        }
    }

    void OnBootstrap() override {
        this->UnsafeBecome(&TCreateQueryRequestActor::StateFunc);
        if (RequestProxy->Get()->Quotas) {
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
            Send(RateLimiterControlPlaneServiceId(),
                 new TEvRateLimiter::TEvCreateResource(RequestProxy->Get()->CloudId, cloudLimit));
        } else {
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
        CPP_LOG_D(
            "Create response from rate limiter service. Success: " << ev->Get()->Success);
        if (ev->Get()->Success) {
            QuoterResourceCreated = true;
            SendRequestIfCan();
        } else {
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
        return (QuoterResourceCreated || !RequestProxy->Get()->Quotas) && TBaseRequestActor::CanSendRequest();
    }
};

} // namespace NFq::NPrivate
