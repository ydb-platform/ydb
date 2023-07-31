#pragma once

#include "counters.h"

#include <contrib/libs/fmt/include/fmt/format.h>
#include <library/cpp/actors/core/event.h>
#include <util/generic/maybe.h>

#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NFq {
namespace NPrivate {

template<class TRequestProto, class TRequest, class TResponse, class TRequestProxy, class TResponseProxy>
class TRequestActor :
    public NActors::TActorBootstrapped<
        TRequestActor<TRequestProto, TRequest, TResponse, TRequestProxy, TResponseProxy>> {
protected:
    using TBase = NActors::TActorBootstrapped<
        TRequestActor<TRequestProto, TRequest, TResponse, TRequestProxy, TResponseProxy>>;
    using TBase::SelfId;
    using TBase::Send;
    using TBase::PassAway;
    using TBase::Become;
    using TBase::Schedule;

    typename TRequestProxy::TPtr RequestProxy;
    ::NFq::TControlPlaneProxyConfig Config;
    TRequestProto RequestProto;
    TString Scope;
    TString FolderId;
    TString User;
    TString Token;
    TActorId Sender;
    ui32 Cookie;
    TActorId ServiceId;
    TRequestCounters Counters;
    TInstant StartTime;
    std::function<void(const TDuration&, bool /* isSuccess */, bool /* isTimeout */)> Probe;
    TPermissions Permissions;
    TString CloudId;
    TString SubjectType;
    const TMaybe<TQuotaMap> Quotas;
    TTenantInfo::TPtr TenantInfo;
    TMaybe<FederatedQuery::Internal::ComputeDatabaseInternal> ComputeDatabase;
    ui32 RetryCount                 = 0;
    bool ReplyWithResponseOnSuccess = true;

public:
    static constexpr char ActorName[] = "YQ_CONTROL_PLANE_PROXY_REQUEST_ACTOR";

    explicit TRequestActor(typename TRequestProxy::TPtr requestProxy,
                           const ::NFq::TControlPlaneProxyConfig& config,
                           TActorId sender,
                           ui32 cookie,
                           const TString& scope,
                           const TString& folderId,
                           TRequestProto&& requestProto,
                           TString&& user,
                           TString&& token,
                           const TActorId& serviceId,
                           const TRequestCounters& counters,
                           const std::function<void(const TDuration&, bool, bool)>& probe,
                           TPermissions permissions,
                           const TString& cloudId,
                           const TString& subjectType,
                           TMaybe<TQuotaMap>&& quotas = Nothing(),
                           TMaybe<FederatedQuery::Internal::ComputeDatabaseInternal>&&
                               computeDatabase             = Nothing(),
                           bool replyWithResponseOnSuccess = true)
        : RequestProxy(requestProxy)
        , Config(config)
        , RequestProto(std::forward<TRequestProto>(requestProto))
        , Scope(scope)
        , FolderId(folderId)
        , User(std::move(user))
        , Token(std::move(token))
        , Sender(sender)
        , Cookie(cookie)
        , ServiceId(serviceId)
        , Counters(counters)
        , StartTime(TInstant::Now())
        , Probe(probe)
        , Permissions(permissions)
        , CloudId(cloudId)
        , SubjectType(subjectType)
        , Quotas(std::move(quotas))
        , ComputeDatabase(std::move(computeDatabase))
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
        TenantInfo = std::move(ev->Get()->TenantInfo);
        if (TenantInfo) {
            SendRequestIfCan();
        } else {
            RetryCount++;
            Schedule(Now() + Config.ConfigRetryPeriod * (1 << RetryCount),
                     new TEvControlPlaneConfig::TEvGetTenantInfoRequest());
        }
    }

    void HandleTimeout() {
        CPP_LOG_D("Request timeout. " << RequestProto.DebugString());
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
        Send(Sender, new TResponseProxy(issues, SubjectType), 0, Cookie);
        PassAway();
    }

    template<class... TArgs>
    void ReplyWithSuccess(TArgs&&... args) {
        const TDuration delta = TInstant::Now() - StartTime;
        Counters.IncOk();
        Probe(delta, true, false);
        if (ReplyWithResponseOnSuccess) {
            Send(Sender,
                 new TResponseProxy(std::forward<TArgs>(args)..., SubjectType),
                 0,
                 Cookie);
        } else {
            RequestProxy->Get()->Response =
                std::make_unique<TResponseProxy>(std::forward<TArgs>(args)..., SubjectType);
            RequestProxy->Get()->ControlPlaneYDBOperationWasPerformed = true;
            Send(RequestProxy->Forward(ControlPlaneProxyActorId()));
        }
        PassAway();
    }

    virtual bool CanSendRequest() const { return bool(TenantInfo); }

    void SendRequestIfCan() {
        if (CanSendRequest()) {
            Send(ServiceId,
                 new TRequest(Scope,
                              RequestProto,
                              User,
                              Token,
                              CloudId,
                              Permissions,
                              Quotas,
                              TenantInfo,
                              ComputeDatabase.GetOrElse({})),
                 0,
                 Cookie);
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
        Become(&TCreateQueryRequestActor::StateFunc);
        if (Quotas) {
            SendCreateRateLimiterResourceRequest();
        } else {
            SendRequestIfCan();
        }
    }

    void SendCreateRateLimiterResourceRequest() {
        if (auto quotaIt = Quotas->find(QUOTA_CPU_PERCENT_LIMIT); quotaIt != Quotas->end()) {
            const double cloudLimit = static_cast<double>(quotaIt->second.Limit.Value *
                                                          10); // percent -> milliseconds
            CPP_LOG_T("Create rate limiter resource for cloud with limit " << cloudLimit
                                                                           << "ms");
            Send(RateLimiterControlPlaneServiceId(),
                 new TEvRateLimiter::TEvCreateResource(CloudId, cloudLimit));
        } else {
            NYql::TIssues issues;
            NYql::TIssue issue =
                MakeErrorIssue(TIssuesIds::INTERNAL_ERROR,
                               TStringBuilder() << "CPU quota for cloud \"" << CloudId
                                                << "\" was not found");
            issues.AddIssue(issue);
            CPP_LOG_W("Failed to get cpu quota for cloud " << CloudId);
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
        return (QuoterResourceCreated || !Quotas) && TBaseRequestActor::CanSendRequest();
    }
};

}
}
