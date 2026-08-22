#include "signal_utils.h"

#include <util/string/builder.h>

namespace NKikimr::NOlap::NCounters {

TIntCounter::TIntCounter(NMonitoring::TDynamicCounters::TCounterPtr counter, i64 defaultValue)
    : Counter(std::move(counter))
    , CurrentValue(defaultValue)
{
    Counter->Add(CurrentValue);
}

TIntCounter::~TIntCounter() {
    Counter->Sub(CurrentValue);
}

void TIntCounter::Set(i64 value) {
    Counter->Add(value - CurrentValue);
    CurrentValue = value;
}

TSignalWrapper::TSignalWrapper(NMonitoring::TDynamicCounterPtr counters, const TString& name)
    : WaitCounter(counters->GetCounter(TStringBuilder() << name << "Wait"))
    , SignalRate(counters->GetCounter(TStringBuilder() << name << "SignalRate", /* derivative */ true))
{}

NThreading::TFuture<void> TSignalWrapper::GetFuture() {
    if (!Promise) {
        Promise = NThreading::NewPromise<void>();
    }

    WaitCounter->Inc();
    return Promise->GetFuture().Subscribe([counter = WaitCounter](const NThreading::TFuture<void>&) {
        counter->Dec();
    });
}

void TSignalWrapper::Signal() {
    if (Promise) {
        SignalRate->Inc();
        Promise->SetValue();
        Promise.reset();
    }
}

} // namespace NKikimr::NOlap::NCounters
