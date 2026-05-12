#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/future/core/future.h>

namespace NKikimr::NOlap::NCounters {

// Can be used if required functional:
// 1. Do ->Set on counter to update value
// 2. Report aggregated metric
class TIntCounter final : public TMoveOnly {
public:
    explicit TIntCounter(NMonitoring::TDynamicCounters::TCounterPtr counter, i64 defaultValue = 0);

    ~TIntCounter();

    void Set(i64 value);

private:
    const NMonitoring::TDynamicCounters::TCounterPtr Counter;
    i64 CurrentValue = 0;
};

class TSignalWrapper {
public:
    TSignalWrapper(NMonitoring::TDynamicCounterPtr counters, const TString& name);

    NThreading::TFuture<void> GetFuture();

    void Signal();

private:
    std::optional<NThreading::TPromise<void>> Promise;
    const NMonitoring::TDynamicCounters::TCounterPtr WaitCounter;
    const NMonitoring::TDynamicCounters::TCounterPtr SignalRate;
};

} // namespace NKikimr::NOlap::NCounters
