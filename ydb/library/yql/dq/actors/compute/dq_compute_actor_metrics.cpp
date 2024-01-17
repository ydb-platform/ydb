#include "dq_compute_actor_metrics.h"
#include "dq_compute_actor.h"

#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/actors/core/interconnect.h>

namespace NYql::NDq {

TDqComputeActorMetrics::TDqComputeActorMetrics(
    const NMonitoring::TDynamicCounterPtr& counters)
    : Enable(!!counters)
    , ComputeActorSubgroup(counters
        ? counters->GetSubgroup("subsystem", "compute_actor")
        : nullptr)
{
    if (ComputeActorSubgroup) {
        InjectedToTaskRunnerWatermark = ComputeActorSubgroup->GetCounter("watermark_injected_ms");
        InjectedToOutputsWatermark = ComputeActorSubgroup->GetCounter("watermark_outputs_ms");
        WatermarkCollectLatency = ComputeActorSubgroup->GetHistogram(
            "watermark_collect_ms",
            NMonitoring::ExplicitHistogram({0, 15, 50, 100, 250, 500, 1000, 10'000, 100'000}));

        InputRows = ComputeActorSubgroup->GetHistogram(
            "input_rows",
            NMonitoring::ExponentialHistogram(12, 2)
        );
        InputBytes = ComputeActorSubgroup->GetHistogram(
            "input_bytes",
            NMonitoring::ExponentialHistogram(12, 10)
        );

#define ADD_COUNTER(name) \
        name = ComputeActorSubgroup->GetCounter(#name)

        ADD_COUNTER(ChannelsInfo);
        ADD_COUNTER(AbortExecution);
        ADD_COUNTER(Wakeup);
        ADD_COUNTER(Undelivered);
        ADD_COUNTER(ChannelData);
        ADD_COUNTER(ChannelDataAck);
        ADD_COUNTER(Run);
        ADD_COUNTER(StateRequest);
        ADD_COUNTER(CheckpointCoordinator);
        ADD_COUNTER(InjectCheckpoint);
        ADD_COUNTER(CommitState);
        ADD_COUNTER(RestoreFromCheckpoint);
        ADD_COUNTER(NodeDisconnected);
        ADD_COUNTER(NodeConnected);
        ADD_COUNTER(NewAsyncInputDataArrived);
        ADD_COUNTER(AsyncInputError);
        ADD_COUNTER(OtherEvent);
        ADD_COUNTER(ResumeExecutionTot);

#undef ADD_COUNTER

        for (ui32 i = 0; i < static_cast<ui32>(EResumeSource::Last); ++i) {
            ResumeExecution[i] = ComputeActorSubgroup->GetCounter(TStringBuilder() << "ResumeExecution" << i);
        }
    }
}

void TDqComputeActorMetrics::ReportEvent(ui32 type, TAutoPtr<NActors::IEventHandle>& ev)
{
    if (!Enable) {
        return;
    }

    switch (type) {
        case TEvDqCompute::TEvResumeExecution::EventType: 
            ResumeExecution[static_cast<ui32>((*reinterpret_cast<TEvDqCompute::TEvResumeExecution::TPtr*>(&ev))->Get()->Source)]->Inc(); 
            ResumeExecutionTot->Inc();
            break;
        case TEvDqCompute::TEvChannelsInfo::EventType: ChannelsInfo->Inc(); break;
        case TEvDq::TEvAbortExecution::EventType: AbortExecution->Inc(); break;
        case NActors::TEvents::TEvWakeup::EventType: Wakeup->Inc(); break;
        case NActors::TEvents::TEvUndelivered::EventType: Undelivered->Inc(); break;
        case TEvDqCompute::TEvChannelData::EventType: ChannelData->Inc(); break;
        case TEvDqCompute::TEvChannelDataAck::EventType: ChannelDataAck->Inc(); break;
        case TEvDqCompute::TEvRun::EventType:  Run->Inc(); break;
        case TEvDqCompute::TEvStateRequest::EventType: StateRequest->Inc(); break;
        case TEvDqCompute::TEvNewCheckpointCoordinator::EventType: CheckpointCoordinator->Inc(); break;
        case TEvDqCompute::TEvInjectCheckpoint::EventType: InjectCheckpoint->Inc(); break;
        case TEvDqCompute::TEvCommitState::EventType: CommitState->Inc(); break;
        case TEvDqCompute::TEvRestoreFromCheckpoint::EventType: RestoreFromCheckpoint->Inc(); break;
        case NActors::TEvInterconnect::TEvNodeDisconnected::EventType: NodeDisconnected->Inc(); break;
        case NActors::TEvInterconnect::TEvNodeConnected::EventType: NodeConnected->Inc(); break;
        case IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::EventType: NewAsyncInputDataArrived->Inc(); break;
        case IDqComputeActorAsyncInput::TEvAsyncInputError::EventType: AsyncInputError->Inc(); break;
        default: OtherEvent->Inc(); break;
    }
}

void TDqComputeActorMetrics::ReportAsyncInputData(ui32 id, ui64 rows, ui64 bytes, TMaybe<TInstant> watermark) {
    if (!Enable) {
        return;
    }

    auto counters = GetAsyncInputCounters(id);
    counters->GetCounter("rows", true)->Add(rows);

    InputRows->Collect(rows);
    InputBytes->Collect(bytes);

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
