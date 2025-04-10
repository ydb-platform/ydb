#include "abstract.h"

namespace NKikimr {

bool ITxReader::Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) {
    AFL_VERIFY(!GetIsFinished());
    if (IsFinishedItself) {
        AFL_VERIFY(NextReader);
        return NextReader->Execute(txc, ctx);
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
    IsFinishedItself = true;
    NextReader = BuildNextReader();
    //Next reader may be async, but if current reader is sync we get all result synchronously
    while (!IsAsync && NextReader && !NextReader->GetIsFinished()) {
        return NextReader->Execute(txc, ctx);
    }
    return true;
}

}