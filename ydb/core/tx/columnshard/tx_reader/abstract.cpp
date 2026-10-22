#include "abstract.h"

#include <ydb/core/tx/columnshard/tracing/startup_probes.h>

#include <ydb/library/actors/struct_log/log_stack.h>

namespace NKikimr {

bool ITxReader::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    if (IsReady) {
        if (!NextReaderAfterLoad) {
            return true;
        } else {
            return NextReaderAfterLoad->Execute(txc, ctx);
        }
    }
    const TMonotonic attemptStart = TMonotonic::Now();
    if (!IsStarted) {
        AFL_VERIFY(Orbit)("stage", StageName);
        IsStarted = true;
        FirstAttemptInstant = attemptStart;
    }
    ++Attempts;
    const bool finished = ExecuteAttempt(txc, ctx);
    const TMonotonic attemptFinish = TMonotonic::Now();
    ExecutionDuration += attemptFinish - attemptStart;
    if (!finished) {
        return false;
    }
    IsReady = true;
    const TDuration duration = attemptFinish - FirstAttemptInstant;
    NColumnShard::TraceStartupTxStageFinished(*Orbit, txc.Tablet, StageName, duration, ExecutionDuration, Attempts);
    NextReaderAfterLoad = BuildNextReaderAfterLoad();
    if (!NextReaderAfterLoad) {
        return true;
    }
    // the next reader runs after this stage has finished, so it is named as its sibling
    NextReaderAfterLoad->AddNamePrefix(NamePrefix);
    NextReaderAfterLoad->SetOrbit(*Orbit);
    return NextReaderAfterLoad->Execute(txc, ctx);
}

bool ITxReader::ExecuteAttempt(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    {
        TMemoryProfileGuard g("ITxReader/" + StageName + "/Precharge");
        NColumnShard::TLoadTimeSignals::TLoadTimer timer = PrechargeCounters.StartGuard();
        YDB_LOG_CREATE_CONTEXT(
            {"loadStageName", "PRECHARGE:" + StageName});
        if (!DoPrecharge(txc, ctx)) {
            timer.AddLoadingFail();
            return false;
        }
    }

    {
        TMemoryProfileGuard g("ITxReader/" + StageName + "/Read");
        NColumnShard::TLoadTimeSignals::TLoadTimer timer = ReaderCounters.StartGuard();
        YDB_LOG_CREATE_CONTEXT(
            {"loadStageName", "EXECUTE:" + StageName});
        if (!DoExecute(txc, ctx)) {
            timer.AddLoadingFail();
            return false;
        }
    }
    return true;
}

}   // namespace NKikimr
