#include "counters.h"

namespace NFq {

TTestConnectionRequestCounters::TTestConnectionRequestCounters(const TString& name)
    : Name(name)
{}

void TTestConnectionRequestCounters::Register(const ::NMonitoring::TDynamicCounterPtr& counters) {
    auto requestCounters = counters->GetSubgroup("request", Name);
    InFly = requestCounters->GetCounter("InFly", false);
    Ok = requestCounters->GetCounter("Ok", true);
    Error = requestCounters->GetCounter("Error", true);
    LatencyMs = requestCounters->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
}

NMonitoring::IHistogramCollectorPtr TTestConnectionRequestCounters::GetLatencyHistogramBuckets() {
    return NMonitoring::ExplicitHistogram({0, 10, 100, 1000, 10000});
}

} // NFq
