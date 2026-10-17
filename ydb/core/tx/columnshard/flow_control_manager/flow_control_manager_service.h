#pragma once

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/feature_flags.h>
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

    // Header-only: libraries that only need to address the FCM (tx/data_events) must not take a
    // link dependency on this library — it already PEERDIRs data_events, so the reverse edge
    // would be a cycle. IsEnabled is inline for the same reason (shard_writer calls it).
    static NActors::TActorId MakeServiceId(ui32 nodeId) {
        return NActors::TActorId(nodeId, "FlowCtrlMng");
    }

    // Single source of truth for "is flow control on". Read from four subsystems (the write path,
    // the shard writer, and both halves of the overload manager), so any future qualification of
    // this condition has one place to happen rather than four.
    static bool IsEnabled() {
        return HasAppData() && AppData()->FeatureFlags.GetEnableCsFlowControl();
    }

    static std::unique_ptr<NActors::IActor> CreateService(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);

    // The one place that decides where FCM counters live, so the service initializer and the
    // per-request helper actors (which only have AppData to go on) agree without a mutable
    // process-global holding the group.
    static TIntrusivePtr<::NMonitoring::TDynamicCounters> BuildCountersGroup(TIntrusivePtr<::NMonitoring::TDynamicCounters> root);

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
    // Instant at which a request that could not even be queued is failed with OVERLOADED.
    static TInstant ComputeDelayedRejectAt(TInstant deadline, TDuration operationTimeout);
    static TDrainRateParams GetDrainRateParams();

    static TDuration PickDrainJitter();

    // Test / tuning hooks (used when ColumnShardConfig.FlowControl is unset).
    // MaxWaitQueueSize == 0 disables waiting (immediate OVERLOADED when gated).
    // DrainJitterMax == 0 drains immediately (no Schedule delay) aside from token-bucket pacing.
    static void SetWaitQueueParams(TDuration drainJitterMin, TDuration drainJitterMax, ui64 maxWaitQueueSize, ui64 maxDelayedRejectQueueSize);
    // Tunes only jitter and the wait queue; the delayed-reject queue keeps the proto default
    // rather than a hardcoded literal that would silently shrink it.
    static void SetWaitQueueParams(TDuration drainJitterMin, TDuration drainJitterMax, ui64 maxWaitQueueSize);
    static void SetWaitTimeoutPercent(ui32 percent);
    static void SetDelayedRejectTimeoutPercent(ui32 percent);
    static void SetDrainRateParams(const TDrainRateParams& params);
    // Clears every Ut* override (drain rates, wait queue, timeout percentages) so one test
    // cannot leak tuning into the next one.
    static void ResetUtOverrides();
};

}   // namespace NKikimr::NColumnShard::NFlowControl
