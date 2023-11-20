#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/hash.h>

#include "dq_compute_actor_async_io.h"

namespace NYql::NDq {

struct TDqComputeActorMetrics {
public:
    TDqComputeActorMetrics(const NMonitoring::TDynamicCounterPtr& counters);

    void ReportEvent(ui32 type, TAutoPtr<NActors::IEventHandle>& ev);
    void ReportAsyncInputData(ui32 id, ui64 rows, ui64 bytes, TMaybe<TInstant> watermark);
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
    NMonitoring::THistogramPtr InputRows;
    NMonitoring::THistogramPtr InputBytes;

    NMonitoring::TDynamicCounters::TCounterPtr ResumeExecutionTot;
    NMonitoring::TDynamicCounters::TCounterPtr ResumeExecution[static_cast<ui32>(EResumeSource::Last)];
    NMonitoring::TDynamicCounters::TCounterPtr ChannelsInfo;
    NMonitoring::TDynamicCounters::TCounterPtr AbortExecution;
    NMonitoring::TDynamicCounters::TCounterPtr Wakeup;
    NMonitoring::TDynamicCounters::TCounterPtr Undelivered;
    NMonitoring::TDynamicCounters::TCounterPtr ChannelData;
    NMonitoring::TDynamicCounters::TCounterPtr ChannelDataAck;
    NMonitoring::TDynamicCounters::TCounterPtr Run;
    NMonitoring::TDynamicCounters::TCounterPtr StateRequest;
    NMonitoring::TDynamicCounters::TCounterPtr CheckpointCoordinator;
    NMonitoring::TDynamicCounters::TCounterPtr InjectCheckpoint;
    NMonitoring::TDynamicCounters::TCounterPtr CommitState;
    NMonitoring::TDynamicCounters::TCounterPtr RestoreFromCheckpoint;
    NMonitoring::TDynamicCounters::TCounterPtr NodeDisconnected;
    NMonitoring::TDynamicCounters::TCounterPtr NodeConnected;
    NMonitoring::TDynamicCounters::TCounterPtr NewAsyncInputDataArrived;
    NMonitoring::TDynamicCounters::TCounterPtr AsyncInputError;
    NMonitoring::TDynamicCounters::TCounterPtr OtherEvent;

    THashMap<TInstant, TInstant> WatermarkStartedAt;
};

}
