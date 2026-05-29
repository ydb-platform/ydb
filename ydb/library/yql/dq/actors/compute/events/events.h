#pragma once

#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/yql/dq/actors/dq_events_ids.h>
#include <util/system/types.h>

namespace NYql::NDq {

enum class EResumeSource : ui32 {
    Default,
    ChannelsHandleWork,
    ChannelsHandleUndeliveredData,
    ChannelsHandleUndeliveredAck,
    AsyncPopFinished,
    CheckpointRegister,
    CheckpointInject,
    CABootstrap,
    CABootstrapWakeup,
    CAPendingInput,
    CATakeInput,
    CASinkFinished,
    CATransformFinished,
    CAStart,
    CAPollAsync,
    CAPollAsyncNoSpace,
    CANewAsyncInput,
    CADataSent,
    CAPendingOutput,
    CATaskRunnerCreated,
    CAResumeByWatermark,
    CAWatermarkIdleness,
    CAWakeupCallback,
    CAResumeByCheckpoint,

    Last,
};

namespace TEvDqCompute {

struct TEvResumeExecution : public NActors::TEventLocal<TEvResumeExecution, TDqComputeEvents::EvResumeExecution> {
    TEvResumeExecution(EResumeSource source)
        : Source(source)
    { }

    TEvResumeExecution() = default;

    EResumeSource Source = EResumeSource::Default;
};

} // namespace TEvDqCompute

} // namespace NYql::NDq
