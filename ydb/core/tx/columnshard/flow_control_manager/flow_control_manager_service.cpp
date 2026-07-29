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

std::atomic<ui64> DrainRMinMilli{ 20'000 };
std::atomic<ui64> DrainRMaxMilli{ 500'000 };
std::atomic<ui64> DrainRStartMilli{ 50'000 };
std::atomic<ui64> DrainBurstMilli{ 100'000 };
std::atomic<ui64> DrainAimdAddMilli{ 5'000 };
std::atomic<ui64> DrainAimdGrowMs{ 1'000 };
std::atomic<ui64> DrainAimdHoldMs{ 2'000 };
std::atomic<ui64> DrainAimdFeedbackMs{ 5'000 };
std::atomic<ui64> DrainAimdBetaMilli{ 500 };

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
    if (const auto* cfg = FlowControlConfigOrNull()) {
        TDrainRateParams params;
        params.RMin = cfg->GetDrainRateMin();
        params.RMax = cfg->GetDrainRateMax();
        params.RStart = cfg->GetDrainRateStart();
        params.Burst = cfg->GetDrainBurst();
        params.AimdAdd = cfg->GetDrainAimdAdd();
        params.AimdGrow = TDuration::MilliSeconds(cfg->GetDrainAimdGrowMs());
        params.AimdHold = TDuration::MilliSeconds(cfg->GetDrainAimdHoldMs());
        params.AimdFeedback = TDuration::MilliSeconds(cfg->GetDrainAimdFeedbackMs());
        params.AimdBeta = cfg->GetDrainAimdBeta();
        return params;
    }

    TDrainRateParams params;
    params.RMin = MilliToRate(DrainRMinMilli.load());
    params.RMax = MilliToRate(DrainRMaxMilli.load());
    params.RStart = MilliToRate(DrainRStartMilli.load());
    params.Burst = MilliToRate(DrainBurstMilli.load());
    params.AimdAdd = MilliToRate(DrainAimdAddMilli.load());
    params.AimdGrow = TDuration::MilliSeconds(DrainAimdGrowMs.load());
    params.AimdHold = TDuration::MilliSeconds(DrainAimdHoldMs.load());
    params.AimdFeedback = TDuration::MilliSeconds(DrainAimdFeedbackMs.load());
    params.AimdBeta = MilliToRate(DrainAimdBetaMilli.load());
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

void TFlowControlManagerServiceOperator::SetDrainRateParams(const TDrainRateParams& params) {
    const double rMin = Max(0.001, params.RMin);
    const double rMax = Max(rMin, params.RMax);
    const double rStart = Min(rMax, Max(rMin, params.RStart));
    const double burst = Max(1.0, params.Burst);
    const double add = Max(0.0, params.AimdAdd);
    const double beta = Min(1.0, Max(0.01, params.AimdBeta));

    DrainRMinMilli.store(RateToMilli(rMin));
    DrainRMaxMilli.store(RateToMilli(rMax));
    DrainRStartMilli.store(RateToMilli(rStart));
    DrainBurstMilli.store(RateToMilli(burst));
    DrainAimdAddMilli.store(RateToMilli(add));
    DrainAimdGrowMs.store(Max<ui64>(1, params.AimdGrow.MilliSeconds()));
    DrainAimdHoldMs.store(Max<ui64>(1, params.AimdHold.MilliSeconds()));
    DrainAimdFeedbackMs.store(Max<ui64>(1, params.AimdFeedback.MilliSeconds()));
    DrainAimdBetaMilli.store(RateToMilli(beta));
}

void TFlowControlManagerServiceOperator::ResetDrainRateParamsToDefaults() {
    SetDrainRateParams(TDrainRateParams{});
    WaitTimeoutPercent.store(50);
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
