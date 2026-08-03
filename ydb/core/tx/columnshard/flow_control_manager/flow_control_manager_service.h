#pragma once

#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>

#include <ydb/library/actors/core/actor.h>

#include <util/datetime/base.h>

#include <memory>

namespace NKikimr::NColumnShard::NFlowControl {

class TFlowControlManagerServiceOperator {
public:
    // Drain-rate control is fully outcome-driven: growth is decided from per-request
    // write outcomes (TEvWriteOutcome) accumulated into cohorts, never from wall clock.
    // Hence there are no AimdGrow / AimdHold / AimdFeedback durations here anymore.
    struct TDrainRateParams {
        double RMin = 10.0;
        double RMax = 500.0;
        double RStart = 10.0;
        double Burst = 20.0;
        double AimdAdd = 5.0;
        double AimdBeta = 0.5;
    };

    static NActors::TActorId MakeServiceId(ui32 nodeId);

    static std::unique_ptr<NActors::IActor> CreateService(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);

    static void StartLongTxWrite(const TActorContext& ctx, TLongTxWrite&& longTxWrite);

    // Prefer ColumnShardConfig.FlowControl when present; else process-wide UT/default atomics.
    static ui64 GetMaxWaitQueueSize();
    static ui64 GetMaxDelayedRejectQueueSize();
    static TDuration GetDrainJitterMin();
    static TDuration GetDrainJitterMax();
    // Max wait = OperationTimeout * WaitTimeoutPercent / 100 (clamped to 1..100, default 50).
    static ui32 GetWaitTimeoutPercent();
    // Delay before OVERLOADED reply for delayed-reject queue = OperationTimeout *
    // DelayedRejectTimeoutPercent / 100 (clamped to 1..100).
    static ui32 GetDelayedRejectTimeoutPercent();
    static TDuration GetMaxWaitDuration(TDuration operationTimeout);
    static TInstant ComputeWaitDeadline(TInstant deadline, TDuration operationTimeout);
    static TDrainRateParams GetDrainRateParams();

    static TDuration PickDrainJitter();

    // Test / tuning hooks (used when ColumnShardConfig.FlowControl is unset).
    // MaxWaitQueueSize == 0 disables waiting (immediate OVERLOADED when gated).
    // DrainJitterMax == 0 drains immediately (no Schedule delay) aside from token-bucket pacing.
    static void SetWaitQueueParams(
        TDuration drainJitterMin, TDuration drainJitterMax, ui64 maxWaitQueueSize, ui64 maxDelayedRejectQueueSize = 512);
    static void SetWaitTimeoutPercent(ui32 percent);
    static void SetDelayedRejectTimeoutPercent(ui32 percent);
    static void SetDrainRateParams(const TDrainRateParams& params);
    static void ResetDrainRateParamsToDefaults();
};

}   // namespace NKikimr::NColumnShard::NFlowControl
