#include "flow_control_manager_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_actor.h>

#include <util/generic/utility.h>
#include <util/random/random.h>

#include <atomic>
#include <cmath>

namespace NKikimr::NColumnShard::NFlowControl {

namespace {

using TFlowControlConfig = NKikimrConfig::TColumnShardConfig::TFlowControlConfig;

// Single source of truth for production defaults: protobuf field defaults on TFlowControlConfig.
const TFlowControlConfig& DefaultFlowControlConfig() {
    static const TFlowControlConfig defaults;
    return defaults;
}

// UT-only overrides when ColumnShardConfig.FlowControl is absent. Production never sets these;
// Reset clears the flags and Get* falls back to DefaultFlowControlConfig().
std::atomic<bool> UtWaitQueueOverrides{ false };
std::atomic<bool> UtWaitTimeoutOverrides{ false };
std::atomic<bool> UtDelayedRejectTimeoutOverrides{ false };
std::atomic<bool> UtDrainOverrides{ false };

std::atomic<ui64> DrainJitterMinMs{ 0 };
std::atomic<ui64> DrainJitterMaxMs{ 0 };
std::atomic<ui64> MaxWaitQueueSize{ 0 };
std::atomic<ui64> MaxDelayedRejectQueueSize{ 0 };
std::atomic<ui32> WaitTimeoutPercent{ 0 };
std::atomic<ui32> DelayedRejectTimeoutPercent{ 0 };

std::atomic<ui64> DrainRMinMilli{ 0 };
std::atomic<ui64> DrainRMaxMilli{ 0 };
std::atomic<ui64> DrainRStartMilli{ 0 };
std::atomic<ui64> DrainAimdBetaMilli{ 0 };
std::atomic<ui64> DrainCubicRecoveryTargetSecMilli{ 0 };
std::atomic<ui64> DrainCubicProbePercentMilli{ 0 };

std::atomic<ui64> DrainRMinBytesMilli{ 0 };
std::atomic<ui64> DrainRMaxBytesMilli{ 0 };
std::atomic<ui64> DrainRStartBytesMilli{ 0 };

double MilliToRate(ui64 milli) {
    return static_cast<double>(milli) / 1000.0;
}

ui64 RateToMilli(double rate) {
    if (rate <= 0) {
        return 0;
    }
    return static_cast<ui64>(std::llround(rate * 1000.0));
}

ui32 ClampPercent(ui32 percent) {
    return Max<ui32>(1, Min<ui32>(100, percent));
}

const TFlowControlConfig* FlowControlConfigOrNull() {
    if (!HasAppData() || !AppDataVerified().ColumnShardConfig.HasFlowControl()) {
        return nullptr;
    }
    return &AppDataVerified().ColumnShardConfig.GetFlowControl();
}

// Start <= 0 in config means "use proto default" (configs often nail 0 for unset).
double PositiveOrDefaultStart(double configured, double protoDefault) {
    return configured > 0.0 ? configured : protoDefault;
}

void FillFromConfig(TFlowControlManagerServiceOperator::TDrainRateParams& params, const TFlowControlConfig& cfg) {
    const auto& defaults = DefaultFlowControlConfig();
    params.RMin = cfg.GetDrainRateMin();
    // 0 = no limit (EffectiveRMax → +inf).
    params.RMax = cfg.GetDrainRateMax();
    params.RStart = PositiveOrDefaultStart(cfg.GetDrainRateStart(), defaults.GetDrainRateStart());
    // Match SetDrainRateParams: Start must not sit below Min, otherwise construction seeds the rate
    // (and Wmax / tokens) below the floor until the first SyncBounds.
    if (params.RMin > 0.0) {
        params.RStart = Max(params.RStart, params.RMin);
    }
    params.AimdBeta = cfg.GetDrainAimdBeta();
    params.CubicRecoveryTargetSec = cfg.GetDrainCubicRecoveryTargetSec();
    params.CubicProbePercent = cfg.GetDrainCubicProbePercent();
    params.RMinBytes = cfg.GetDrainRateMinBytes();
    // 0 = no limit (EffectiveRMaxBytes → +inf).
    params.RMaxBytes = cfg.GetDrainRateMaxBytes();
    params.RStartBytes = PositiveOrDefaultStart(cfg.GetDrainRateStartBytes(), defaults.GetDrainRateStartBytes());
    if (params.RMinBytes > 0.0) {
        params.RStartBytes = Max(params.RStartBytes, params.RMinBytes);
    }
    params.AimdBetaBytes = params.AimdBeta;
}

TFlowControlManagerServiceOperator::TDrainRateParams ParamsFromConfig(const TFlowControlConfig& cfg) {
    TFlowControlManagerServiceOperator::TDrainRateParams params;
    FillFromConfig(params, cfg);
    return params;
}

void FillFromUtAtomics(TFlowControlManagerServiceOperator::TDrainRateParams& params) {
    params.RMin = MilliToRate(DrainRMinMilli.load());
    params.RMax = MilliToRate(DrainRMaxMilli.load());
    params.RStart = MilliToRate(DrainRStartMilli.load());
    params.AimdBeta = MilliToRate(DrainAimdBetaMilli.load());
    params.CubicRecoveryTargetSec = MilliToRate(DrainCubicRecoveryTargetSecMilli.load());
    params.CubicProbePercent = MilliToRate(DrainCubicProbePercentMilli.load());
    params.RMinBytes = MilliToRate(DrainRMinBytesMilli.load());
    params.RMaxBytes = MilliToRate(DrainRMaxBytesMilli.load());
    params.RStartBytes = MilliToRate(DrainRStartBytesMilli.load());
    params.AimdBetaBytes = params.AimdBeta;
}

}   // namespace

TFlowControlManagerServiceOperator::TDrainRateParams::TDrainRateParams() {
    // Avoid calling Defaults()/ParamsFromConfig here — those construct TDrainRateParams.
    FillFromConfig(*this, DefaultFlowControlConfig());
}

TFlowControlManagerServiceOperator::TDrainRateParams TFlowControlManagerServiceOperator::TDrainRateParams::Defaults() {
    return ParamsFromConfig(DefaultFlowControlConfig());
}

std::unique_ptr<NActors::IActor> TFlowControlManagerServiceOperator::CreateService(
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup) {
    return std::make_unique<TFlowControlManager>(countersGroup);
}

TIntrusivePtr<::NMonitoring::TDynamicCounters> TFlowControlManagerServiceOperator::BuildCountersGroup(
    TIntrusivePtr<::NMonitoring::TDynamicCounters> root) {
    if (!root) {
        return nullptr;
    }
    return GetServiceCounters(root, "tablets")->GetSubgroup("type", "CS_FLOW_CONTROL_MANAGER");
}

ui64 TFlowControlManagerServiceOperator::GetMaxWaitQueueSize() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return cfg->GetMaxWaitQueueSize();
    }
    if (UtWaitQueueOverrides.load()) {
        return MaxWaitQueueSize.load();
    }
    return DefaultFlowControlConfig().GetMaxWaitQueueSize();
}

ui64 TFlowControlManagerServiceOperator::GetMaxDelayedRejectQueueSize() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return cfg->GetMaxDelayedRejectQueueSize();
    }
    if (UtWaitQueueOverrides.load()) {
        return MaxDelayedRejectQueueSize.load();
    }
    return DefaultFlowControlConfig().GetMaxDelayedRejectQueueSize();
}

TDuration TFlowControlManagerServiceOperator::GetDrainJitterMin() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return TDuration::MilliSeconds(cfg->GetDrainJitterMinMs());
    }
    if (UtWaitQueueOverrides.load()) {
        return TDuration::MilliSeconds(DrainJitterMinMs.load());
    }
    return TDuration::MilliSeconds(DefaultFlowControlConfig().GetDrainJitterMinMs());
}

TDuration TFlowControlManagerServiceOperator::GetDrainJitterMax() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return TDuration::MilliSeconds(cfg->GetDrainJitterMaxMs());
    }
    if (UtWaitQueueOverrides.load()) {
        return TDuration::MilliSeconds(DrainJitterMaxMs.load());
    }
    return TDuration::MilliSeconds(DefaultFlowControlConfig().GetDrainJitterMaxMs());
}

ui32 TFlowControlManagerServiceOperator::GetWaitTimeoutPercent() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return ClampPercent(cfg->GetWaitTimeoutPercent());
    }
    if (UtWaitTimeoutOverrides.load()) {
        return ClampPercent(WaitTimeoutPercent.load());
    }
    return ClampPercent(DefaultFlowControlConfig().GetWaitTimeoutPercent());
}

ui32 TFlowControlManagerServiceOperator::GetDelayedRejectTimeoutPercent() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return ClampPercent(cfg->GetDelayedRejectTimeoutPercent());
    }
    if (UtDelayedRejectTimeoutOverrides.load()) {
        return ClampPercent(DelayedRejectTimeoutPercent.load());
    }
    return ClampPercent(DefaultFlowControlConfig().GetDelayedRejectTimeoutPercent());
}

TDuration TFlowControlManagerServiceOperator::GetMaxWaitDuration(TDuration operationTimeout) {
    return operationTimeout * GetWaitTimeoutPercent() / 100;
}

TInstant TFlowControlManagerServiceOperator::ComputeWaitDeadline(TInstant deadline, TDuration operationTimeout) {
    // WaitDeadline = Start + MaxWait = Deadline - Timeout + Timeout*pct/100
    //             = Deadline - Timeout * (100 - pct) / 100
    const ui32 pct = GetWaitTimeoutPercent();
    return deadline - operationTimeout * (100 - pct) / 100;
}

TInstant TFlowControlManagerServiceOperator::ComputeDelayedRejectAt(TInstant deadline, TDuration operationTimeout) {
    // Same shape as ComputeWaitDeadline, and deliberately anchored to the operation start
    // (Deadline - Timeout) rather than to "now". The point of DelayedRejectTimeoutPercent is to
    // hold the caller for that share of its budget and hand the rest back for a retry; measured
    // from the moment FCM sees the request it would really be (time already spent upstream) + pct%,
    // so the reserve the knob promises would shrink by however long the navigate and split took.
    // TInstant subtraction saturates, so a request that arrives with less than pct% of its budget
    // left yields an instant in the past and is rejected immediately, which is what it deserves.
    const ui32 pct = GetDelayedRejectTimeoutPercent();
    return deadline - operationTimeout * (100 - pct) / 100;
}

TFlowControlManagerServiceOperator::TDrainRateParams TFlowControlManagerServiceOperator::GetDrainRateParams() {
    // Prefer live ColumnShardConfig.FlowControl (Get* ⇒ proto defaults for unset fields).
    // Else UT drain overrides, else default-constructed TFlowControlConfig.
    // DrainRateMax / DrainRateMaxBytes of 0 mean no limit.
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return ParamsFromConfig(*cfg);
    }
    if (UtDrainOverrides.load()) {
        TDrainRateParams params;
        FillFromUtAtomics(params);
        return params;
    }
    return ParamsFromConfig(DefaultFlowControlConfig());
}

void TFlowControlManagerServiceOperator::SetWaitQueueParams(
    TDuration drainJitterMin, TDuration drainJitterMax, ui64 maxWaitQueueSize, ui64 maxDelayedRejectQueueSize) {
    if (drainJitterMax < drainJitterMin) {
        drainJitterMax = drainJitterMin;
    }
    DrainJitterMinMs.store(drainJitterMin.MilliSeconds());
    DrainJitterMaxMs.store(drainJitterMax.MilliSeconds());
    MaxWaitQueueSize.store(maxWaitQueueSize);
    MaxDelayedRejectQueueSize.store(maxDelayedRejectQueueSize);
    UtWaitQueueOverrides.store(true);
}

void TFlowControlManagerServiceOperator::SetWaitQueueParams(TDuration drainJitterMin, TDuration drainJitterMax, ui64 maxWaitQueueSize) {
    SetWaitQueueParams(drainJitterMin, drainJitterMax, maxWaitQueueSize, DefaultFlowControlConfig().GetMaxDelayedRejectQueueSize());
}

void TFlowControlManagerServiceOperator::SetWaitTimeoutPercent(ui32 percent) {
    WaitTimeoutPercent.store(ClampPercent(percent));
    UtWaitTimeoutOverrides.store(true);
}

void TFlowControlManagerServiceOperator::SetDelayedRejectTimeoutPercent(ui32 percent) {
    DelayedRejectTimeoutPercent.store(ClampPercent(percent));
    UtDelayedRejectTimeoutOverrides.store(true);
}

void TFlowControlManagerServiceOperator::SetDrainRateParams(const TDrainRateParams& params) {
    // Count bucket. RMax of 0 means no limit and is stored verbatim.
    const double rMin = Max(0.0, params.RMin);
    const double rMax = params.RMax > 0.0 ? Max(rMin, params.RMax) : 0.0;
    const double rStartLo = rMin > 0.0 ? rMin : 1.0;
    const double rStartHi = rMax > 0.0 ? rMax : params.RStart;
    const double rStartRaw = params.RStart > 0.0 ? params.RStart : rStartLo;
    const double rStart = Min(Max(rStartRaw, rStartLo), Max(rStartHi, rStartLo));
    const double beta = Min(1.0, Max(0.01, params.AimdBeta));
    const double kTarget = Max(0.001, params.CubicRecoveryTargetSec);
    const double probePct = Max(0.0, params.CubicProbePercent);

    DrainRMinMilli.store(RateToMilli(rMin));
    DrainRMaxMilli.store(RateToMilli(rMax));
    DrainRStartMilli.store(RateToMilli(rStart));
    DrainAimdBetaMilli.store(RateToMilli(beta));
    DrainCubicRecoveryTargetSecMilli.store(RateToMilli(kTarget));
    DrainCubicProbePercentMilli.store(RateToMilli(probePct));

    const double rMinB = Max(0.0, params.RMinBytes);
    const double rMaxB = params.RMaxBytes > 0.0 ? Max(rMinB, params.RMaxBytes) : 0.0;
    const double rStartBLo = rMinB > 0.0 ? rMinB : 1'000'000.0;
    const double rStartBHi = rMaxB > 0.0 ? rMaxB : params.RStartBytes;
    const double rStartBRaw = params.RStartBytes > 0.0 ? params.RStartBytes : rStartBLo;
    const double rStartB = Min(Max(rStartBRaw, rStartBLo), Max(rStartBHi, rStartBLo));

    DrainRMinBytesMilli.store(RateToMilli(rMinB));
    DrainRMaxBytesMilli.store(RateToMilli(rMaxB));
    DrainRStartBytesMilli.store(RateToMilli(rStartB));
    UtDrainOverrides.store(true);
}

void TFlowControlManagerServiceOperator::ResetUtOverrides() {
    UtDrainOverrides.store(false);
    UtWaitQueueOverrides.store(false);
    UtWaitTimeoutOverrides.store(false);
    UtDelayedRejectTimeoutOverrides.store(false);
}

TDuration TFlowControlManagerServiceOperator::PickDrainJitter() {
    const ui64 minMs = GetDrainJitterMin().MilliSeconds();
    const ui64 maxMs = GetDrainJitterMax().MilliSeconds();
    if (maxMs == 0) {
        return TDuration::Zero();
    }
    if (maxMs <= minMs) {
        return TDuration::MilliSeconds(minMs);
    }
    return TDuration::MilliSeconds(minMs + RandomNumber<ui64>(maxMs - minMs + 1));
}

TInstant TLongTxWrite::GetWaitDeadline() const {
    return TFlowControlManagerServiceOperator::ComputeWaitDeadline(Deadline, OperationTimeout);
}

}   // namespace NKikimr::NColumnShard::NFlowControl
