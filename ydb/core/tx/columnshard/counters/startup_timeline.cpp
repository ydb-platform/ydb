#include "startup_timeline.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr::NColumnShard {

void TStartupTimeline::LogStageFinished(const TString& stage, const TDuration duration) const {
    YDB_LOG_INFO_COMP(NKikimrServices::TX_COLUMNSHARD, "",
        {"event", "startup_stage_finished"},
        {"tabletId", TabletId},
        {"stage", "startup/" + stage},
        {"duration_us", duration.MicroSeconds()});
}

void TStartupTimeline::OnExecutorActivated() {
    ExecutorActivatedInstant = TMonotonic::Now();
    const TDuration executorBoot = *ExecutorActivatedInstant - CreateInstant;
    Counters.OnActivateExecutor(executorBoot);
    LogStageFinished("executor_boot", executorBoot);
}

void TStartupTimeline::OnTxInitSchemaFinished() {
    AFL_VERIFY(ExecutorActivatedInstant);
    TxInitSchemaFinishInstant = TMonotonic::Now();
    const TDuration txInitSchema = TxInitSchemaFinishInstant - *ExecutorActivatedInstant;
    Counters.OnTxInitSchemaFinished(txInitSchema);
    LogStageFinished("tx_init_schema", txInitSchema);
}

void TStartupTimeline::OnTxInitCreated() {
    TxInitCreateInstant = TMonotonic::Now();
    const TDuration normalization = TxInitCreateInstant - TxInitSchemaFinishInstant;
    Counters.OnNormalizationFinished(normalization);
    LogStageFinished("normalization", normalization);
}

void TStartupTimeline::OnTxInitFinished() {
    TxInitFinishInstant = TMonotonic::Now();
    const TDuration txInit = TxInitFinishInstant - TxInitCreateInstant;
    Counters.OnTxInitFinished(txInit);
    LogStageFinished("tx_init", txInit);
}

void TStartupTimeline::OnSwitchedToWork(const ui64 portionsCount) {
    AFL_VERIFY(ExecutorActivatedInstant);
    const TMonotonic now = TMonotonic::Now();
    Counters.OnSwitchToWork(now - *ExecutorActivatedInstant, now - CreateInstant);
    const TDuration switchToWork = now - TxInitFinishInstant;
    LogStageFinished("switch_to_work", switchToWork);
    YDB_LOG_NOTICE_COMP(NKikimrServices::TX_COLUMNSHARD, "",
        {"event", "startup_stage_finished"},
        {"tabletId", TabletId},
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
