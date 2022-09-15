#include "dq_compute_actor_metrics.h"

namespace NYql::NDq {

TDqComputeActorMetrics::TDqComputeActorMetrics(const NMonitoring::TDynamicCounterPtr& counters) {
    if (!counters) {
        return;
    }

    ComputeActorSubgroup = counters->GetSubgroup("subsystem", "compute_actor");
    InjectedToTaskRunnerWatermark = ComputeActorSubgroup->GetCounter("watermark_injected_ms");
    InjectedToOutputsWatermark = ComputeActorSubgroup->GetCounter("watermark_outputs_ms");
    WatermarkCollectLatency = ComputeActorSubgroup->GetHistogram(
        "watermark_collect_ms",
        NMonitoring::ExplicitHistogram({0, 15, 50, 100, 250, 500, 1000, 10'000, 100'000}));
}

void TDqComputeActorMetrics::ReportAsyncInputData(ui32 id, ui64 dataSize, TMaybe<TInstant> watermark) {
    if (!Enable) {
        return;
    }

    auto counters = GetAsyncInputCounters(id);
    counters->GetCounter("rows", true)->Add(dataSize);

    if (!watermark) {
        return;
    }

    ReportInputWatermarkMetrics(counters, *watermark);
}

void TDqComputeActorMetrics::ReportInputChannelWatermark(ui32 id, ui64 dataSize, TMaybe<TInstant> watermark) {
    if (!Enable) {
        return;
    }

    auto counters = GetInputChannelCounters(id);
    counters->GetCounter("rows", true)->Add(dataSize);
    if (!watermark) {
        return;
    }

    ReportInputWatermarkMetrics(counters, *watermark);
}

void TDqComputeActorMetrics::ReportInjectedToTaskRunnerWatermark(TInstant watermark) {
    if (!Enable) {
        return;
    }

    InjectedToTaskRunnerWatermark->Set(watermark.MilliSeconds());
}

void TDqComputeActorMetrics::ReportInjectedToOutputsWatermark(TInstant watermark) {
    if (!Enable) {
        return;
    }

    InjectedToOutputsWatermark->Set(watermark.MilliSeconds());
    auto iter = WatermarkStartedAt.find(watermark);
    if (iter != WatermarkStartedAt.end()) {
        WatermarkCollectLatency->Collect((TInstant::Now() - iter->second).MilliSeconds());
        WatermarkStartedAt.erase(iter);
    }
}

NMonitoring::TDynamicCounterPtr TDqComputeActorMetrics::GetAsyncInputCounters(ui32 id) {
    auto iter = AsyncInputsCounters.find(id);
    if (iter == AsyncInputsCounters.end()) {
        iter = AsyncInputsCounters.emplace(id, ComputeActorSubgroup->GetSubgroup("async_input", ToString(id))).first;
    }

    return iter->second;
}

NMonitoring::TDynamicCounterPtr TDqComputeActorMetrics::GetInputChannelCounters(ui32 id) {
    auto iter = InputChannelsCounters.find(id);
    if (iter == InputChannelsCounters.end()) {
        iter = InputChannelsCounters.emplace(id, ComputeActorSubgroup->GetSubgroup("input_channel", ToString(id))).first;
    }

    return iter->second;
}

void TDqComputeActorMetrics::ReportInputWatermarkMetrics(NMonitoring::TDynamicCounterPtr& counters, TInstant watermark) {
    counters->GetCounter("watermark_ms")->Set(watermark.MilliSeconds());
    if (!WatermarkStartedAt.contains(watermark)) {
        WatermarkStartedAt[watermark] = TInstant::Now();
    }
}

}
