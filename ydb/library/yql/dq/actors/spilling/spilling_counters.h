#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>

namespace NYql::NDq {

struct TSpillingCounters : public TThrRefBase {

    TSpillingCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingWriteBlobs;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingReadBlobs;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingStoredBlobs;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingTotalSpaceUsed;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingTooBigFileErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingNoSpaceErrors;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingIoErrors;
};

struct TSpillingTaskCounters : public TThrRefBase {
    // Maybe change to std::atomic<ui64>
    TAtomicCounter SpillingReadBytes;
    TAtomicCounter SpillingWriteBytes;
};

} // namespace NYql::NDq
