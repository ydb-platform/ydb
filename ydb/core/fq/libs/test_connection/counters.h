#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NFq {

class TTestConnectionRequestCounters: public virtual TThrRefBase {
public:
    const TString Name;
    ::NMonitoring::TDynamicCounters::TCounterPtr InFly;
    ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
    ::NMonitoring::TDynamicCounters::TCounterPtr Error;
    NMonitoring::THistogramPtr LatencyMs;

    explicit TTestConnectionRequestCounters(const TString& name);

    void Register(const ::NMonitoring::TDynamicCounterPtr& counters);

private:
    static NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets();
};

using TTestConnectionRequestCountersPtr = TIntrusivePtr<TTestConnectionRequestCounters>;

} // NFq
