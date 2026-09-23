#include "abstract.h"
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
    IsStarted = true;
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
    IsReady = true;
    NextReaderAfterLoad = BuildNextReaderAfterLoad();
    return NextReaderAfterLoad ? NextReaderAfterLoad->Execute(txc, ctx) : true;
}

}   // namespace NKikimr
