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
    std::atomic<ui64> ComputeWriteBytes = 0;
    std::atomic<ui64> ChannelWriteBytes = 0;

    std::atomic<ui64> ComputeReadTime = 0;
    std::atomic<ui64> ComputeWriteTime = 0;
    std::atomic<ui64> ChannelReadTime = 0;
    std::atomic<ui64> ChannelWriteTime = 0;
};

} // namespace NYql::NDq
