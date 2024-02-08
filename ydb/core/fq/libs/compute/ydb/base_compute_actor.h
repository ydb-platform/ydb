#pragma once

#include <ydb/core/fq/libs/compute/common/run_actor_params.h>

#include <ydb/core/fq/libs/compute/common/metrics.h>

#include <ydb/library/yql/providers/common/metrics/service_counters.h>

#include <ydb/public/sdk/cpp/client/ydb_types/status_codes.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/retry/retry_policy.h>

namespace NFq {

template<typename TDerived>
class TBaseComputeActor : public NActors::TActorBootstrapped<TDerived> {
public:
    using TBase = NActors::TActorBootstrapped<TDerived>;
    using TBase::PassAway;

    TBaseComputeActor(const ::NYql::NCommon::TServiceCounters& queryCounters, const TString& stepName)
        : PublicCounters(queryCounters.PublicCounters)
        , BaseCounters(queryCounters.Counters)
        , Counters(MakeIntrusive<TComputeRequestCounters>("Total", queryCounters.Counters->GetSubgroup("step", stepName)))
        , TotalStartTime(TInstant::Now())
    {}

    TBaseComputeActor(const ::NMonitoring::TDynamicCounterPtr& baseCounters, const TString& stepName)
        : PublicCounters(MakeIntrusive<::NMonitoring::TDynamicCounters>())
        , BaseCounters(baseCounters)
        , Counters(MakeIntrusive<TComputeRequestCounters>("Total", baseCounters->GetSubgroup("step", stepName)))
        , TotalStartTime(TInstant::Now())
    {}

    void Bootstrap() {
        Counters->Register();
        Counters->InFly->Inc();
        AsDerived()->Start();
    }

    TDerived* AsDerived() {
        return static_cast<TDerived*>(this);
    }

    void CompleteAndPassAway() {
        Counters->Ok->Inc();
        PassAway();
    }

    void FailedAndPassAway() {
        Counters->Error->Inc();
        PassAway();
    }

    virtual ~TBaseComputeActor() {
        Counters->InFly->Dec();
        Counters->LatencyMs->Collect((TInstant::Now() - TotalStartTime).MilliSeconds());
    }

    ::NMonitoring::TDynamicCounterPtr GetStepCountersSubgroup() const {
        return Counters->Counters;
    }

    ::NMonitoring::TDynamicCounterPtr GetBaseCounters() const {
        return BaseCounters;
    }

    ::NMonitoring::TDynamicCounterPtr GetPublicCounters() const {
        return PublicCounters;
    }

private:
    ::NMonitoring::TDynamicCounterPtr PublicCounters;
    ::NMonitoring::TDynamicCounterPtr BaseCounters;
    TComputeRequestCountersPtr Counters;
    TInstant TotalStartTime;
};

} /* NFq */
