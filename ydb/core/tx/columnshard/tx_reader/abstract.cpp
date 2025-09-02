#include "abstract.h"

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
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("load_stage_name", "PRECHARGE:" + StageName);
        if (!DoPrecharge(txc, ctx)) {
            timer.AddLoadingFail();
            return false;
        }
    }

    {
        TMemoryProfileGuard g("ITxReader/" + StageName + "/Read");
        NColumnShard::TLoadTimeSignals::TLoadTimer timer = ReaderCounters.StartGuard();
        NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("load_stage_name", "EXECUTE:" + StageName);
        if (!DoExecute(txc, ctx)) {
            timer.AddLoadingFail();
            return false;
        }
    }
    IsReady = true;
    NextReaderAfterLoad = BuildNextReaderAfterLoad();
    return NextReaderAfterLoad ? NextReaderAfterLoad->Execute(txc, ctx) : true;
}

}