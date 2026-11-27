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

TStartupTimeline::~TStartupTimeline() {
    if (MediatorForkedFromStartup) {
        Orbit.Join(MediatorTimeOrbit);
    }
}

void TStartupTimeline::OnMediatorTimeRegistrationStarted() {
    YDB_LOG_VERIFY(!MediatorInitializationStartedAt);
    MediatorInitializationStartedAt = TMonotonic::Now();
    if (!StartupStagesFinished) {
        Orbit.Fork(MediatorTimeOrbit);
        MediatorForkedFromStartup = true;
    }
    TraceMediatorTimeRegistrationStarted(MediatorTimeOrbit, TabletId);
}

void TStartupTimeline::OnMediatorTimeReady() {
    YDB_LOG_VERIFY(MediatorInitializationStartedAt);
    const TDuration duration = TMonotonic::Now() - *MediatorInitializationStartedAt;
    MediatorInitializationStartedAt.reset();
    Counters.OnMediatorTimeReady(duration);
    TraceStartupStageFinished(MediatorTimeOrbit, TabletId, "startup/mediator_time_initialization", duration);
    if (MediatorForkedFromStartup) {
        Orbit.Join(MediatorTimeOrbit);
        MediatorForkedFromStartup = false;
        if (StartupStagesFinished) {
            Orbit.Reset();
        }
    }
    MediatorTimeOrbit.Reset();
}

void TStartupTimeline::FinishStartupStages() {
    YDB_LOG_VERIFY(!StartupStagesFinished);
    StartupStagesFinished = true;
    if (!MediatorForkedFromStartup) {
        Orbit.Reset();
    }
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
    YDB_LOG_VERIFY(ExecutorActivatedInstant);
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

void TStartupTimeline::OnSwitchedToWork(const ui64 portionsCount, const bool portionsLoaded) {
    YDB_LOG_VERIFY(ExecutorActivatedInstant);
    const TMonotonic now = TMonotonic::Now();
    SwitchedToWorkInstant = now;
    Counters.OnSwitchToWork(now - *ExecutorActivatedInstant, now - CreateInstant);
    const TDuration switchToWork = now - TxInitFinishInstant;
    TraceStageFinished("switch_to_work", switchToWork);
    TraceStartupStageFinished(Orbit, TabletId, "startup", now - CreateInstant);
    if (portionsLoaded) {
        FinishStartupStages();
    }
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

void TStartupTimeline::OnAllPortionsLoaded(const ui64 portionsCount, const ui32 batches) {
    const TMonotonic now = TMonotonic::Now();
    TraceStageFinished("deferred_portion_loading", now - SwitchedToWorkInstant);
    FinishStartupStages();
    YDB_LOG_NOTICE_COMP(NKikimrServices::TX_COLUMNSHARD, "",
        {"event", "all_portions_loaded"},
        {"duration_us", (now - CreateInstant).MicroSeconds()},
        {"after_switch_to_work_us", (now - SwitchedToWorkInstant).MicroSeconds()},
        {"portions", portionsCount},
        {"batches", batches});
}

}   // namespace NKikimr::NColumnShard
