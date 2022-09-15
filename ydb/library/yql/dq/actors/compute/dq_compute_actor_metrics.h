#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>

namespace NYql::NDq {

struct TDqComputeActorMetrics {
public:
    TDqComputeActorMetrics(const NMonitoring::TDynamicCounterPtr& counters);

    void ReportAsyncInputData(ui32 id, ui64 dataSize, TMaybe<TInstant> watermark);
    void ReportInputChannelWatermark(ui32 id, ui64 dataSize, TMaybe<TInstant> watermark);
    void ReportInjectedToTaskRunnerWatermark(TInstant watermark);
    void ReportInjectedToOutputsWatermark(TInstant watermark);

private:
    NMonitoring::TDynamicCounterPtr GetAsyncInputCounters(ui32 id);
    NMonitoring::TDynamicCounterPtr GetInputChannelCounters(ui32 id);
    void ReportInputWatermarkMetrics(NMonitoring::TDynamicCounterPtr& counters, TInstant watermark);

private:
    bool Enable = false;
    NMonitoring::TDynamicCounterPtr ComputeActorSubgroup;
    THashMap<ui32, NMonitoring::TDynamicCounterPtr> AsyncInputsCounters;
    THashMap<ui32, NMonitoring::TDynamicCounterPtr> InputChannelsCounters;
    NMonitoring::TDynamicCounters::TCounterPtr WatermarkCt;
    NMonitoring::TDynamicCounters::TCounterPtr InjectedToTaskRunnerWatermark;
    NMonitoring::TDynamicCounters::TCounterPtr InjectedToOutputsWatermark;
    NMonitoring::THistogramPtr WatermarkCollectLatency;

    THashMap<TInstant, TInstant> WatermarkStartedAt;
};

}
