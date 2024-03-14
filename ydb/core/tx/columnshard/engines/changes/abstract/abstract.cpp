#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NOlap {

TString TColumnEngineChanges::DebugString() const {
    TStringStream sb;
    sb << "type=" << TypeString() << ";details=(";
    DoDebugString(sb);
    sb << ");";
    return sb.Str();
}

TConclusionStatus TColumnEngineChanges::ConstructBlobs(TConstructionContext& context) noexcept {
    Y_ABORT_UNLESS(Stage == EStage::Started);

    context.Counters.CompactionInputSize(Blobs.GetTotalBlobsSize());
    const TMonotonic start = TMonotonic::Now();
    TConclusionStatus result = DoConstructBlobs(context);
    if (result.Ok()) {
        context.Counters.CompactionDuration->Collect((TMonotonic::Now() - start).MilliSeconds());
    } else {
        context.Counters.CompactionFails->Add(1);
    }
    Stage = EStage::Constructed;
    return result;
}

void TColumnEngineChanges::WriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    Y_ABORT_UNLESS(Stage != EStage::Aborted);
    Y_ABORT_UNLESS(Stage <= EStage::Written);
    Y_ABORT_UNLESS(Stage >= EStage::Compiled);

    DoWriteIndexOnExecute(self, context);
    Stage = EStage::Written;
}

void TColumnEngineChanges::WriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    Y_ABORT_UNLESS(Stage == EStage::Written || !self);
    Stage = EStage::Finished;
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "WriteIndexComplete")("type", TypeString())("success", context.FinishedSuccessfully);
    DoWriteIndexOnComplete(self, context);
    if (self) {
        OnFinish(*self, context);
        self->IncCounter(GetCounterIndex(context.FinishedSuccessfully));
    }

}

void TColumnEngineChanges::Compile(TFinalizationContext& context) noexcept {
    Y_ABORT_UNLESS(Stage != EStage::Aborted);
    if ((ui32)Stage >= (ui32)EStage::Compiled) {
        return;
    }
    Y_ABORT_UNLESS(Stage == EStage::Constructed);

    DoCompile(context);

    Stage = EStage::Compiled;
}

TColumnEngineChanges::~TColumnEngineChanges() {
    Y_DEBUG_ABORT_UNLESS(!NActors::TlsActivationContext || Stage == EStage::Created || Stage == EStage::Finished || Stage == EStage::Aborted);
}

void TColumnEngineChanges::Abort(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    Y_ABORT_UNLESS(Stage != EStage::Finished && Stage != EStage::Created && Stage != EStage::Aborted);
    Stage = EStage::Aborted;
    OnFinish(self, context);
}

void TColumnEngineChanges::Start(NColumnShard::TColumnShard& self) {
    self.DataLocksManager->RegisterLock(BuildDataLock());
    Y_ABORT_UNLESS(Stage == EStage::Created);
    DoStart(self);
    Stage = EStage::Started;
    if (!NeedConstruction()) {
        Stage = EStage::Constructed;
    }
}

void TColumnEngineChanges::StartEmergency() {
    Y_ABORT_UNLESS(Stage == EStage::Created);
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

void TColumnEngineChanges::OnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    self.DataLocksManager->UnregisterLock(TypeString() + "::" + GetTaskIdentifier());
    DoOnFinish(self, context);
}

TWriteIndexContext::TWriteIndexContext(NTable::TDatabase* db, IDbWrapper& dbWrapper, TColumnEngineForLogs& engineLogs)
    : DB(db)
    , DBWrapper(dbWrapper)
    , EngineLogs(engineLogs)
{

}

}
