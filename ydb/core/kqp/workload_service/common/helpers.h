#pragma once

#include <library/cpp/retry/retry_policy.h>

#include <ydb/core/resource_pools/resource_pool_settings.h>

#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>


namespace NKikimr::NKqp::NWorkload {

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << LogPrefix() << stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << LogPrefix() << stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << LogPrefix() << stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << LogPrefix() << stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << LogPrefix() << stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << LogPrefix() << stream)
#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_WORKLOAD_SERVICE, "[WorkloadService] " << LogPrefix() << stream)


template <typename TDerived>
class TSchemeActorBase : public NActors::TActorBootstrapped<TDerived> {
    using TRetryPolicy = IRetryPolicy<bool>;

protected:
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;

public:
    TSchemeActorBase() {
    }

    void Bootstrap() {
        static_cast<TDerived*>(this)->DoBootstrap();
        StartRequest();
    }

    void Handle(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->Reason == NActors::TEvents::TEvUndelivered::ReasonActorUnknown && ScheduleRetry("Scheme service not found")) {
            return;
        }

        LOG_E("Scheme service is unavailable");
        OnFatalError(Ydb::StatusIds::UNAVAILABLE, NYql::TIssue("Scheme service is unavailable"));
    }

    STRICT_STFUNC(StateFuncBase,
        sFunc(TEvents::TEvWakeup, StartRequest);
        hFunc(TEvents::TEvUndelivered, Handle);
    )

protected:
    virtual void StartRequest() = 0;
    virtual void OnFatalError(Ydb::StatusIds::StatusCode status, NYql::TIssue issue) = 0;

    virtual TString LogPrefix() const = 0;

protected:
    bool ScheduleRetry(NYql::TIssues issues, bool longDelay = false) {
        if (!RetryState) {
            RetryState = CreateRetryState();
        }

        if (const auto delay = RetryState->GetNextRetryDelay(longDelay)) {
            Issues.AddIssues(issues);
            this->Schedule(*delay, new TEvents::TEvWakeup());
            LOG_W("Scheduled retry for error: " << issues.ToOneLineString());
            return true;
        }

        return false;
    }

    bool ScheduleRetry(const TString& message, bool longDelay = false) {
        return ScheduleRetry({NYql::TIssue(message)}, longDelay);
    }

private:
    static TRetryPolicy::IRetryState::TPtr CreateRetryState() {
        return TRetryPolicy::GetFixedIntervalPolicy(
                  [](bool longDelay){return longDelay ? ERetryErrorClass::LongRetry : ERetryErrorClass::ShortRetry;}
                , TDuration::MilliSeconds(100)
                , TDuration::MilliSeconds(500)
                , 100
            )->CreateRetryState();
    }

protected:
    NYql::TIssues Issues;

private:
    TRetryPolicy::IRetryState::TPtr RetryState;
};


NYql::TIssues GroupIssues(const NYql::TIssues& issues, const TString& message);

void ParsePoolSettings(const NKikimrSchemeOp::TResourcePoolDescription& description, NResourcePool::TPoolSettings& poolConfig);

ui64 SaturationSub(ui64 x, ui64 y);

}  // NKikimr::NKqp::NWorkload
