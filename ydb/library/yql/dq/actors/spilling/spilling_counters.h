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

struct TSpillingCountersPerTaskRunner : public TThrRefBase {

    TSpillingCountersPerTaskRunner(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters, ui64 taskId);

    ~TSpillingCountersPerTaskRunner();

    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    ui64 TaskId;

    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingWriteBytes;
    ::NMonitoring::TDynamicCounters::TCounterPtr SpillingReadBytes;
};

} // namespace NYql::NDq
