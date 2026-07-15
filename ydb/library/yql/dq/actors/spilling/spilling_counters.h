#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>

namespace NYql::NDq {

enum class ESpillingType {
    Compute,
    Channel,
};

struct TSpillingCounters : public TThrRefBase {

    struct TTypeCounters {
        ::NMonitoring::TDynamicCounters::TCounterPtr WriteBlobs;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReadBlobs;
        ::NMonitoring::TDynamicCounters::TCounterPtr StoredBlobs;
        ::NMonitoring::TDynamicCounters::TCounterPtr TotalSpaceUsed;
        ::NMonitoring::TDynamicCounters::TCounterPtr TooBigFileErrors;
        ::NMonitoring::TDynamicCounters::TCounterPtr NoSpaceErrors;
        ::NMonitoring::TDynamicCounters::TCounterPtr IoErrors;
        ::NMonitoring::TDynamicCounters::TCounterPtr FileDescriptors;
    };

    TSpillingCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    TTypeCounters& GetTypeCounters(ESpillingType type) {
        return type == ESpillingType::Compute ? ComputeSpilling : ChannelSpilling;
    }

    TTypeCounters ComputeSpilling;
    TTypeCounters ChannelSpilling;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingIOQueueSize;
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
