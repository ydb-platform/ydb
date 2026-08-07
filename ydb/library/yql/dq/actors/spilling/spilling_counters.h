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

    // Backend-specific counters for DDisk PersistentBuffer spilling.
    struct TDDiskCounters {
        ::NMonitoring::TDynamicCounters::TCounterPtr ActiveSessions;
        ::NMonitoring::TDynamicCounters::TCounterPtr Discoveries;
        ::NMonitoring::TDynamicCounters::TCounterPtr DiscoveryErrors;
        ::NMonitoring::TDynamicCounters::TCounterPtr Connects;
        ::NMonitoring::TDynamicCounters::TCounterPtr ConnectErrors;
        ::NMonitoring::TDynamicCounters::TCounterPtr WriteBytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReadBytes;
        ::NMonitoring::TDynamicCounters::TCounterPtr WriteParts;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReadParts;
        ::NMonitoring::TDynamicCounters::TCounterPtr Erases;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlightWrites;
        ::NMonitoring::TDynamicCounters::TCounterPtr InFlightReads;
    };

    TSpillingCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    TTypeCounters& GetTypeCounters(ESpillingType type) {
        return type == ESpillingType::Compute ? ComputeSpilling : ChannelSpilling;
    }

    TTypeCounters ComputeSpilling;
    TTypeCounters ChannelSpilling;
    TDDiskCounters DDisk;
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
