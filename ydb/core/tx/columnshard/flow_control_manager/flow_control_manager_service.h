#pragma once

#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_types.h>

#include <ydb/library/actors/core/actor.h>

#include <util/datetime/base.h>

#include <memory>

namespace NKikimr::NColumnShard::NFlowControl {

class TFlowControlManagerServiceOperator {
public:
    // Drain-rate control is outcome-driven with CUBIC recovery after cuts.
    // Growth uses W(t) toward Wmax over CubicRecoveryTargetSec, then ProbePercent of Wmax
    // per clean cohort — never absolute admits/s or MB/s nails.
    //
    // Default field values come from TColumnShardConfig.TFlowControlConfig proto defaults
    // (see Defaults()). RMax / RMaxBytes of 0 mean no limit (+inf ceiling).
    struct TDrainRateParams {
        double RMin;
        double RMax;
        double RStart;
        // Shared cut / CUBIC β (both buckets).
        double AimdBeta;
        // Shared CUBIC recovery time target (seconds) and post-Wmax probe (% of Wmax).
        double CubicRecoveryTargetSec;
        double CubicProbePercent;

        // Bytes bucket (bytes/sec).
        double RMinBytes;
        double RMaxBytes;
        double RStartBytes;
        // Filled from AimdBeta by GetDrainRateParams / Defaults (shared).
        double AimdBetaBytes;

        // Proto defaults from TFlowControlConfig.
        TDrainRateParams();
        static TDrainRateParams Defaults();
    };

    static NActors::TActorId MakeServiceId(ui32 nodeId);

    static std::unique_ptr<NActors::IActor> CreateService(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);

    static void StartLongTxWrite(const TActorContext& ctx, TLongTxWrite&& longTxWrite);

    // All knobs are read from ColumnShardConfig.FlowControl via Get* (protobuf defaults
    // apply for unset fields). When FlowControl is absent, a default-constructed
    // TFlowControlConfig is used — unless a UT Set* override is active.
    // DrainRateMax / DrainRateMaxBytes of 0 mean no limit.
    static ui64 GetMaxWaitQueueSize();
    static ui64 GetMaxDelayedRejectQueueSize();
    static TDuration GetDrainJitterMin();
    static TDuration GetDrainJitterMax();
    // Max wait = OperationTimeout * WaitTimeoutPercent / 100 (clamped to 1..100).
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
