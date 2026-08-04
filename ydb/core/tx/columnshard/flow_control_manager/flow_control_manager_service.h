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
        // Count bucket (requests/sec). RMin/RMax default to 0 = "unset": the actor treats
        // an unset floor as a tiny non-zero value and an unset ceiling as +inf, letting AIMD
        // self-regulate instead of pinning the rate to arbitrary config nails. Burst is gone —
        // the token bucket now uses a soft one-cohort cap (see RefillTokens).
        double RMin = 0.0;
        double RMax = 0.0;
        double RStart = 10.0;
        // Percent of current rate per clean cohort (5 ⇒ +5%). Shared by count and bytes buckets.
        double AimdAdd = 5.0;
        double AimdBeta = 0.5;

        // Bytes bucket (bytes/sec). Same unset-bound semantics.
        double RMinBytes = 0.0;
        double RMaxBytes = 0.0;
        double RStartBytes = 10'000'000.0;
        // Filled from AimdAdd / AimdBeta by GetDrainRateParams (shared AIMD).
        double AimdAddBytes = 5.0;
        double AimdBetaBytes = 0.5;
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
