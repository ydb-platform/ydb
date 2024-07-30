#include "abstract.h"
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
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
        self->Counters.GetTabletCounters().IncCounter(GetCounterIndex(context.FinishedSuccessfully));
    }

}

void TColumnEngineChanges::Compile(TFinalizationContext& context) noexcept {
    AFL_VERIFY(Stage != EStage::Aborted);
    AFL_VERIFY(Stage == EStage::Constructed)("real", Stage);

    DoCompile(context);
    DoOnAfterCompile();

    Stage = EStage::Compiled;
}

TColumnEngineChanges::~TColumnEngineChanges() {
//    AFL_VERIFY_DEBUG(!NActors::TlsActivationContext || Stage == EStage::Created || Stage == EStage::Finished || Stage == EStage::Aborted)("stage", Stage);
}

void TColumnEngineChanges::Abort(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Abort")("reason", context.ErrorMessage);
    AFL_VERIFY(Stage != EStage::Finished && Stage != EStage::Created && Stage != EStage::Aborted)("stage", Stage)("reason", context.ErrorMessage)("prev_reason", AbortedReason);
    Stage = EStage::Aborted;
    AbortedReason = context.ErrorMessage;
    OnFinish(self, context);
}

void TColumnEngineChanges::Start(NColumnShard::TColumnShard& self) {
    AFL_VERIFY(!LockGuard);
    LockGuard = self.DataLocksManager->RegisterLock(BuildDataLock());
    Y_ABORT_UNLESS(Stage == EStage::Created);
    NYDBTest::TControllers::GetColumnShardController()->OnWriteIndexStart(self.TabletID(), *this);
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

void TColumnEngineChanges::AbortEmergency(const TString& reason) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "AbortEmergency")("reason", reason)("prev_reason", AbortedReason);
    if (Stage == EStage::Aborted) {
        AbortedReason += "; AnotherReason: " + reason;
    } else {
        Stage = EStage::Aborted;
        AbortedReason = reason;
        if (!!LockGuard) {
            LockGuard->AbortLock();
        }
        OnAbortEmergency();
    }
}

void TColumnEngineChanges::OnFinish(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    if (!!LockGuard) {
        LockGuard->Release(*self.DataLocksManager);
    }
    DoOnFinish(self, context);
}

TWriteIndexContext::TWriteIndexContext(NTable::TDatabase* db, IDbWrapper& dbWrapper, TColumnEngineForLogs& engineLogs)
    : DB(db)
    , DBWrapper(dbWrapper)
    , EngineLogs(engineLogs)
{

}

}
