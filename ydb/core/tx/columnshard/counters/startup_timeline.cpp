#include "startup_timeline.h"

#include <ydb/core/tx/columnshard/tracing/startup_probes.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NColumnShard {

TStartupTimeline::TStartupTimeline(const ui64 tabletId, const TCSInitialization& counters)
    : TabletId(tabletId)
    , Counters(counters)
{
    TraceStartupStarted(Orbit, TabletId);
}

void TStartupTimeline::TraceStageFinished(const TString& stage, const TDuration duration) {
    TraceStartupStageFinished(Orbit, TabletId, "startup/" + stage, duration);
}

void TStartupTimeline::OnExecutorActivated() {
    ExecutorActivatedInstant = TMonotonic::Now();
    const TDuration executorBoot = *ExecutorActivatedInstant - CreateInstant;
    Counters.OnActivateExecutor(executorBoot);
    TraceStageFinished("executor_boot", executorBoot);
}

void TStartupTimeline::OnTxInitSchemaFinished() {
    AFL_VERIFY(ExecutorActivatedInstant);
    TxInitSchemaFinishInstant = TMonotonic::Now();
    const TDuration txInitSchema = TxInitSchemaFinishInstant - *ExecutorActivatedInstant;
    Counters.OnTxInitSchemaFinished(txInitSchema);
    TraceStageFinished("tx_init_schema", txInitSchema);
}

void TStartupTimeline::OnTxInitCreated() {
    TxInitCreateInstant = TMonotonic::Now();
    const TDuration normalization = TxInitCreateInstant - TxInitSchemaFinishInstant;
    Counters.OnNormalizationFinished(normalization);
    TraceStageFinished("normalization", normalization);
}

void TStartupTimeline::OnTxInitFinished() {
    TxInitFinishInstant = TMonotonic::Now();
    const TDuration txInit = TxInitFinishInstant - TxInitCreateInstant;
    Counters.OnTxInitFinished(txInit);
    TraceStageFinished("tx_init", txInit);
}

void TStartupTimeline::OnSwitchedToWork(const ui64 portionsCount) {
    AFL_VERIFY(ExecutorActivatedInstant);
    const TMonotonic now = TMonotonic::Now();
    Counters.OnSwitchToWork(now - *ExecutorActivatedInstant, now - CreateInstant);
    const TDuration switchToWork = now - TxInitFinishInstant;
    TraceStageFinished("switch_to_work", switchToWork);
    TraceStartupStageFinished(Orbit, TabletId, "startup", now - CreateInstant);
    // lwtrace publishes a track only when it ends
    Orbit.Reset();
    YDB_LOG_NOTICE_COMP(NKikimrServices::TX_COLUMNSHARD, "",
        {"event", "startup_stage_finished"},
        {"stage", "startup"},
        {"duration_us", (now - CreateInstant).MicroSeconds()},
        {"executor_boot_us", (*ExecutorActivatedInstant - CreateInstant).MicroSeconds()},
        {"tx_init_schema_us", (TxInitSchemaFinishInstant - *ExecutorActivatedInstant).MicroSeconds()},
        {"normalization_us", (TxInitCreateInstant - TxInitSchemaFinishInstant).MicroSeconds()},
        {"tx_init_us", (TxInitFinishInstant - TxInitCreateInstant).MicroSeconds()},
        {"switch_to_work_us", switchToWork.MicroSeconds()},
        {"portions", portionsCount});
}

}   // namespace NKikimr::NColumnShard
