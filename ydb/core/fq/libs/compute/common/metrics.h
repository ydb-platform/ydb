#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

struct TComputeRequestCounters: public virtual TThrRefBase {
    const TString Name;

    ::NMonitoring::TDynamicCounterPtr Counters;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retry;
    ::NMonitoring::THistogramPtr LatencyMs;

    explicit TComputeRequestCounters(const TString& name, const ::NMonitoring::TDynamicCounterPtr& counters = nullptr)
        : Name(name)
        , Counters(counters)
    { }

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
        Counters = counters;
        Register();
    }

    void Register() {
        ::NMonitoring::TDynamicCounterPtr subgroup = Counters->GetSubgroup("request", Name);
        InFly = subgroup->GetCounter("InFly", false);
        Ok = subgroup->GetCounter("Ok", true);
        Error = subgroup->GetCounter("Error", true);
        Retry = subgroup->GetCounter("Retry", true);
        LatencyMs = subgroup->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
    }

private:
    static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
        return ::NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
    }
};

using TComputeRequestCountersPtr = TIntrusivePtr<TComputeRequestCounters>;

} /* NFq */
