#include "abstract.h"

#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>

#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NOlap {

void TColumnEngineChanges::SetStage(const NChanges::EStage stage) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "new_stage")("stage", ::ToString(stage))("task_id", GetTaskIdentifier());
    StateGuard.SetState(stage);
}

TString TColumnEngineChanges::DebugString() const {
    TStringStream sb;
    sb << "type=" << TypeString() << ";details=(";
    DoDebugString(sb);
    sb << ");";
    return sb.Str();
}

TConclusionStatus TColumnEngineChanges::ConstructBlobs(TConstructionContext& context) noexcept {
    const NActors::TLogContextGuard lGuard = NActors::TLogContextBuilder::Build()("task_id", GetTaskIdentifier())("task_class", TypeString());
    AFL_VERIFY(StateGuard.GetStage() == NChanges::EStage::ReadyForConstruct || StateGuard.GetStage() == NChanges::EStage::Started)(
                                                                               "actual_stage", StateGuard.GetStage());

    context.Counters.CompactionInputSize(Blobs.GetTotalBlobsSize());
    const TMonotonic start = TMonotonic::Now();
    TConclusionStatus result = DoConstructBlobs(context);
    if (result.Ok()) {
        context.Counters.CompactionDuration->Collect((TMonotonic::Now() - start).MilliSeconds());
    } else {
        context.Counters.CompactionFails->Add(1);
    }
    SetStage(NChanges::EStage::Constructed);
    return result;
}

void TColumnEngineChanges::WriteIndexOnExecute(NColumnShard::TColumnShard* self, TWriteIndexContext& context) {
    AFL_VERIFY(StateGuard.GetStage() != NChanges::EStage::Aborted);
    AFL_VERIFY(StateGuard.GetStage() <= NChanges::EStage::Written);
    AFL_VERIFY(StateGuard.GetStage() >= NChanges::EStage::Compiled);

    DoWriteIndexOnExecute(self, context);
    SetStage(NChanges::EStage::Written);
}

void TColumnEngineChanges::WriteIndexOnComplete(NColumnShard::TColumnShard* self, TWriteIndexCompleteContext& context) {
    Y_ABORT_UNLESS(StateGuard.GetStage() == NChanges::EStage::Written || !self);
    SetStage(NChanges::EStage::Finished);
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "WriteIndexComplete")("type", TypeString())("success", context.FinishedSuccessfully);
    DoWriteIndexOnComplete(self, context);
    if (self) {
        OnFinish(*self, context);
        self->Counters.GetTabletCounters()->IncCounter(GetCounterIndex(context.FinishedSuccessfully));
    }
}

void TColumnEngineChanges::Compile(TFinalizationContext& context) noexcept {
    AFL_VERIFY(StateGuard.GetStage() != NChanges::EStage::Aborted);

    DoCompile(context);
    DoOnAfterCompile();

    SetStage(NChanges::EStage::Compiled);
}

TColumnEngineChanges::~TColumnEngineChanges() {
    //    AFL_VERIFY_DEBUG(!NActors::TlsActivationContext || Stage == EStage::Created || Stage == EStage::Finished || Stage == EStage::Aborted)("stage", Stage);
}

void TColumnEngineChanges::Abort(NColumnShard::TColumnShard& self, TChangesFinishContext& context) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "Abort")("reason", context.ErrorMessage);
    AFL_VERIFY(StateGuard.GetStage() != NChanges::EStage::Finished && StateGuard.GetStage() != NChanges::EStage::Created && StateGuard.GetStage() != NChanges::EStage::Aborted)(
                                              "stage", StateGuard.GetStage())(
                                                             "reason", context.ErrorMessage)("prev_reason", AbortedReason);
    SetStage(NChanges::EStage::Aborted);
    AbortedReason = context.ErrorMessage;
    OnFinish(self, context);
}

void TColumnEngineChanges::Start(NColumnShard::TColumnShard& self) {
    AFL_VERIFY(!LockGuard);
    LockGuard = self.DataLocksManager->RegisterLock(BuildDataLock());
    Y_ABORT_UNLESS(StateGuard.GetStage() == NChanges::EStage::Created);
    NYDBTest::TControllers::GetColumnShardController()->OnWriteIndexStart(self.TabletID(), *this);
    DoStart(self);
    SetStage(NChanges::EStage::Started);
//    if (!NeedConstruction()) {
//        SetStage(NChanges::EStage::Constructed);
//    }
}

void TColumnEngineChanges::StartEmergency() {
    Y_ABORT_UNLESS(StateGuard.GetStage() == NChanges::EStage::Created);
    SetStage(NChanges::EStage::Started);
//    if (!NeedConstruction()) {
//        SetStage(NChanges::EStage::Constructed);
//    }
}

void TColumnEngineChanges::AbortEmergency(const TString& reason) {
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "AbortEmergency")("reason", reason)("prev_reason", AbortedReason);
    if (StateGuard.GetStage() == NChanges::EStage::Aborted) {
        AbortedReason += "; AnotherReason: " + reason;
    } else {
        SetStage(NChanges::EStage::Aborted);
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

TWriteIndexContext::TWriteIndexContext(NTable::TDatabase* db, IDbWrapper& dbWrapper, TColumnEngineForLogs& engineLogs, const TSnapshot& snapshot)
    : DB(db)
    , DBWrapper(dbWrapper)
    , EngineLogs(engineLogs)
    , Snapshot(snapshot) {
}

}   // namespace NKikimr::NOlap
