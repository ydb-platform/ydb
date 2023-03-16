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
    return NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
}

} // NFq
