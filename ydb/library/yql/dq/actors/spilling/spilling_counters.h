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
    // std::atomic<ui64> ComputeReadBytes;
    std::atomic<ui64> ComputeWriteBytes;
    // std::atomic<ui64> ChannelReadBytes;
    std::atomic<ui64> ChannelWriteBytes;

    // TDuration is not atomic coutable
    std::atomic<ui64> ComputeReadTime;
    std::atomic<ui64> ComputeWriteTime;
    std::atomic<ui64> ChannelReadTime;
    std::atomic<ui64> ChannelWriteTime;
};

} // namespace NYql::NDq
