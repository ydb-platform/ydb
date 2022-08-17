#pragma once

#include <ydb/core/kqp/kqp.h>
#include <ydb/core/ymq/base/counters.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/log.h>


namespace NKikimr::NSQS {

class TMonitoringActor : public TActorBootstrapped<TMonitoringActor> {
private:
    enum class EState {
        LockQueue,
        GetQueue,
        RemoveData,
        Finish
    };

public:
    TMonitoringActor(TIntrusivePtr<TMonitoringCounters> counters);
    
    void Bootstrap(const TActorContext& ctx);

    STRICT_STFUNC(StateFunc,
        HFunc(NKqp::TEvKqp::TEvQueryResponse, HandleQueryResponse);
        HFunc(NKqp::TEvKqp::TEvProcessResponse, HandleProcessResponse);
        IgnoreFunc(NKqp::TEvKqp::TEvCloseSessionResponse);
    )

    void RequestMetrics(TDuration runAfter, const TActorContext& ctx);
    
    void HandleError(const TString& error, const TActorContext& ctx);

    void HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx);
    void HandleProcessResponse(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev, const TActorContext& ctx);

    

private:
    TIntrusivePtr<TMonitoringCounters> Counters;
    TDuration RetryPeriod;

    TString RemovedQueuesQuery;
};

} // namespace NKikimr::NSQS
