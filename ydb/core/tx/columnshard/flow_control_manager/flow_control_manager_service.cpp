#include "flow_control_manager_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/flow_control_manager/flow_control_manager_actor.h>

#include <util/generic/utility.h>
#include <util/random/random.h>

#include <atomic>
#include <cmath>

namespace NKikimr::NColumnShard::NFlowControl {

namespace {

// Process-wide defaults / UT overrides (used when ColumnShardConfig.FlowControl is unset).
std::atomic<ui64> DrainJitterMinMs{ 50 };
std::atomic<ui64> DrainJitterMaxMs{ 250 };
std::atomic<ui64> MaxWaitQueueSize{ 500 };
std::atomic<ui64> MaxDelayedRejectQueueSize{ 5000 };
std::atomic<ui32> WaitTimeoutPercent{ 10 };
std::atomic<ui32> DelayedRejectTimeoutPercent{ 10 };

// Count bucket. RMin/RMax default to 0 = unset (no floor / no ceiling); the actor's
// EffectiveRMin()/EffectiveRMax() supply a 1 req/s floor and +inf ceiling in that case.
std::atomic<ui64> DrainRMinMilli{ 0 };
std::atomic<ui64> DrainRMaxMilli{ 0 };
std::atomic<ui64> DrainRStartMilli{ 50'000 };
std::atomic<ui64> DrainAimdBetaMilli{ 500 };
std::atomic<ui64> DrainCubicRecoveryTargetSecMilli{ 10'000 };   // 10.0 s
std::atomic<ui64> DrainCubicProbePercentMilli{ 5'000 };   // 5.0 %

// Bytes bucket. Same unset-bound semantics. Encoded in the same milli-rate units.
std::atomic<ui64> DrainRMinBytesMilli{ 0 };
std::atomic<ui64> DrainRMaxBytesMilli{ 0 };
std::atomic<ui64> DrainRStartBytesMilli{ 10'000'000'000 };   // 10 MB/sec
std::atomic<ui64> DrainAimdBetaBytesMilli{ 500 };   // 0.5 (mirrors count)

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

const NKikimrConfig::TColumnShardConfig::TFlowControlConfig* FlowControlConfigOrNull() {
    if (!HasAppData() || !AppDataVerified().ColumnShardConfig.HasFlowControl()) {
        return nullptr;
    }
    return &AppDataVerified().ColumnShardConfig.GetFlowControl();
}

}   // namespace

NActors::TActorId TFlowControlManagerServiceOperator::MakeServiceId(ui32 nodeId) {
    return NActors::TActorId(nodeId, "FlowCtrlMng");
}

std::unique_ptr<NActors::IActor> TFlowControlManagerServiceOperator::CreateService(
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup) {
    return std::make_unique<TFlowControlManager>(countersGroup);
}

ui64 TFlowControlManagerServiceOperator::GetMaxWaitQueueSize() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return cfg->GetMaxWaitQueueSize();
    }
    return MaxWaitQueueSize.load();
}

ui64 TFlowControlManagerServiceOperator::GetMaxDelayedRejectQueueSize() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        if (cfg->HasMaxDelayedRejectQueueSize()) {
            return cfg->GetMaxDelayedRejectQueueSize();
        }
    }
    return MaxDelayedRejectQueueSize.load();
}

TDuration TFlowControlManagerServiceOperator::GetDrainJitterMin() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return TDuration::MilliSeconds(cfg->GetDrainJitterMinMs());
    }
    return TDuration::MilliSeconds(DrainJitterMinMs.load());
}

TDuration TFlowControlManagerServiceOperator::GetDrainJitterMax() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return TDuration::MilliSeconds(cfg->GetDrainJitterMaxMs());
    }
    return TDuration::MilliSeconds(DrainJitterMaxMs.load());
}

ui32 TFlowControlManagerServiceOperator::GetWaitTimeoutPercent() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return ClampPercent(cfg->GetWaitTimeoutPercent());
    }
    return ClampPercent(WaitTimeoutPercent.load());
}

ui32 TFlowControlManagerServiceOperator::GetDelayedRejectTimeoutPercent() {
    if (const auto* cfg = FlowControlConfigOrNull()) {
        return ClampPercent(cfg->GetDelayedRejectTimeoutPercent());
    }
    return ClampPercent(DelayedRejectTimeoutPercent.load());
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

TFlowControlManagerServiceOperator::TDrainRateParams TFlowControlManagerServiceOperator::GetDrainRateParams() {
    // Start from process-wide atomics (0 RMin/RMax = unset). Overlay only fields explicitly
    // set on ColumnShardConfig.FlowControl (Has*). DrainAimdBeta / CUBIC knobs are shared
    // across count and bytes — no separate bytes AIMD/CUBIC config.
    TDrainRateParams params;
    params.RMin = MilliToRate(DrainRMinMilli.load());
    params.RMax = MilliToRate(DrainRMaxMilli.load());
    params.RStart = MilliToRate(DrainRStartMilli.load());
    params.AimdBeta = MilliToRate(DrainAimdBetaMilli.load());
    params.CubicRecoveryTargetSec = MilliToRate(DrainCubicRecoveryTargetSecMilli.load());
    params.CubicProbePercent = MilliToRate(DrainCubicProbePercentMilli.load());
    params.RMinBytes = MilliToRate(DrainRMinBytesMilli.load());
    params.RMaxBytes = MilliToRate(DrainRMaxBytesMilli.load());
    params.RStartBytes = MilliToRate(DrainRStartBytesMilli.load());

    if (const auto* cfg = FlowControlConfigOrNull()) {
        if (cfg->HasDrainRateMin()) {
            params.RMin = cfg->GetDrainRateMin();
        }
        if (cfg->HasDrainRateMax()) {
            params.RMax = cfg->GetDrainRateMax();
        }
        // Start <= 0 means unset (proto default for StartBytes is 0; configs often nail 0
        // meaning "default"). Applying 0 cold-starts the actor at EffectiveRMin* and stalls
        // the wait queue after a process restart.
        if (cfg->HasDrainRateStart() && cfg->GetDrainRateStart() > 0.0) {
            params.RStart = cfg->GetDrainRateStart();
        }
        if (cfg->HasDrainAimdBeta()) {
            params.AimdBeta = cfg->GetDrainAimdBeta();
        }
        if (cfg->HasDrainCubicRecoveryTargetSec()) {
            params.CubicRecoveryTargetSec = cfg->GetDrainCubicRecoveryTargetSec();
        }
        if (cfg->HasDrainCubicProbePercent()) {
            params.CubicProbePercent = cfg->GetDrainCubicProbePercent();
        }
        if (cfg->HasDrainRateMinBytes()) {
            params.RMinBytes = cfg->GetDrainRateMinBytes();
        }
        if (cfg->HasDrainRateMaxBytes()) {
            params.RMaxBytes = cfg->GetDrainRateMaxBytes();
        }
        if (cfg->HasDrainRateStartBytes() && cfg->GetDrainRateStartBytes() > 0.0) {
            params.RStartBytes = cfg->GetDrainRateStartBytes();
        }
    }

    params.AimdBetaBytes = params.AimdBeta;
    return params;
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
}

void TFlowControlManagerServiceOperator::SetWaitTimeoutPercent(ui32 percent) {
    WaitTimeoutPercent.store(ClampPercent(percent));
}

void TFlowControlManagerServiceOperator::SetDelayedRejectTimeoutPercent(ui32 percent) {
    DelayedRejectTimeoutPercent.store(ClampPercent(percent));
}

void TFlowControlManagerServiceOperator::SetDrainRateParams(const TDrainRateParams& params) {
    // Count bucket. RMin/RMax of 0 mean "unset" and are stored verbatim (RateToMilli(0)==0);
    // only clamp/order them when a caller actually set positive bounds.
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

    // Bytes bucket bounds/start. Beta is shared with the count bucket.
    const double rMinB = Max(0.0, params.RMinBytes);
    const double rMaxB = params.RMaxBytes > 0.0 ? Max(rMinB, params.RMaxBytes) : 0.0;
    const double rStartBLo = rMinB > 0.0 ? rMinB : 1'000'000.0;
    const double rStartBHi = rMaxB > 0.0 ? rMaxB : params.RStartBytes;
    const double rStartBRaw = params.RStartBytes > 0.0 ? params.RStartBytes : rStartBLo;
    const double rStartB = Min(Max(rStartBRaw, rStartBLo), Max(rStartBHi, rStartBLo));

    DrainRMinBytesMilli.store(RateToMilli(rMinB));
    DrainRMaxBytesMilli.store(RateToMilli(rMaxB));
    DrainRStartBytesMilli.store(RateToMilli(rStartB));
    DrainAimdBetaBytesMilli.store(RateToMilli(beta));
}

void TFlowControlManagerServiceOperator::ResetDrainRateParamsToDefaults() {
    SetDrainRateParams(TDrainRateParams{});
    WaitTimeoutPercent.store(50);
    DelayedRejectTimeoutPercent.store(10);
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
