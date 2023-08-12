#include "abstract.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NOlap {

TString TColumnEngineChanges::DebugString() const {
    TStringStream sb;
    sb << "type=" << TypeString() << ";details=(";
    DoDebugString(sb);
    sb << ");";
    return sb.Str();
}

NKikimr::TConclusion<std::vector<TString>> TColumnEngineChanges::ConstructBlobs(TConstructionContext& context) {
    Y_VERIFY(Stage == EStage::Started);

    {
        ui64 readBytes = 0;
        for (auto&& i : Blobs) {
            readBytes += i.first.Size;
        }
        context.Counters.CompactionInputSize(readBytes);
    }
    const TMonotonic start = TMonotonic::Now();
    TConclusion<std::vector<TString>> result = DoConstructBlobs(context);
    if (result.IsSuccess()) {
        context.Counters.CompactionDuration->Collect((TMonotonic::Now() - start).MilliSeconds());
    } else {
        context.Counters.CompactionFails->Add(1);
    }
    Stage = EStage::Constructed;
    return result;
}

bool TColumnEngineChanges::ApplyChanges(TColumnEngineForLogs& self, TApplyChangesContext& context) {
    Y_VERIFY(Stage == EStage::Compiled);
    Y_VERIFY(DoApplyChanges(self, context));
    Stage = EStage::Applied;
    return true;
}

void TColumnEngineChanges::WriteIndex(NColumnShard::TColumnShard& self, TWriteIndexContext& context) {
    Y_VERIFY(Stage != EStage::Aborted);
    if ((ui32)Stage >= (ui32)EStage::Written) {
        return;
    }
    Y_VERIFY(Stage == EStage::Applied);

    DoWriteIndex(self, context);
    Stage = EStage::Written;
}

void TColumnEngineChanges::WriteIndexComplete(NColumnShard::TColumnShard& self, TWriteIndexCompleteContext& context) {
    Y_VERIFY(Stage == EStage::Written);
    Stage = EStage::Finished;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "WriteIndexComplete")("type", TypeString())("success", context.FinishedSuccessfully);
    DoWriteIndexComplete(self, context);
    DoOnFinish(self, context);
    self.IncCounter(GetCounterIndex(context.FinishedSuccessfully));

}

void TColumnEngineChanges::Compile(TFinalizationContext& context) noexcept {
    Y_VERIFY(Stage != EStage::Aborted);
    if ((ui32)Stage >= (ui32)EStage::Compiled) {
        return;
    }
    Y_VERIFY(Stage == EStage::Constructed);

    DoCompile(context);

    Stage = EStage::Compiled;
}

TColumnEngineChanges::~TColumnEngineChanges() {
    Y_VERIFY_DEBUG(!NActors::TlsActivationContext || Stage == EStage::Created || Stage == EStage::Finished || Stage == EStage::Aborted);
}

void TColumnEngineChanges::Abort(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    Y_VERIFY(Stage != EStage::Finished && Stage != EStage::Created && Stage != EStage::Aborted);
    Stage = EStage::Aborted;
    DoOnFinish(self, context);
}

void TColumnEngineChanges::Start(NColumnShard::TColumnShard& self) {
    Y_VERIFY(Stage == EStage::Created);
    DoStart(self);
    Stage = EStage::Started;
    if (!NeedConstruction()) {
        Stage = EStage::Constructed;
    }
}

void TColumnEngineChanges::StartEmergency() {
    Y_VERIFY(Stage == EStage::Created);
    Stage = EStage::Started;
    if (!NeedConstruction()) {
        Stage = EStage::Constructed;
    }
}

void TColumnEngineChanges::AbortEmergency() {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "AbortEmergency");
    Stage = EStage::Aborted;
    OnAbortEmergency();
}

TWriteIndexContext::TWriteIndexContext(NTabletFlatExecutor::TTransactionContext& txc, IDbWrapper& dbWrapper)
    : Txc(txc)
    , BlobManagerDb(std::make_shared<NColumnShard::TBlobManagerDb>(txc.DB))
    , DBWrapper(dbWrapper)
{

}

}
