#include "import_actor.h"

#include <ydb/core/tx/columnshard/backup/async_jobs/import_downloader.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/import_common.h>

namespace NKikimr::NOlap::NImport {

class TTxProposeFinish: public NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard> {
private:
    using TBase = NTabletFlatExecutor::TTransactionBase<NColumnShard::TColumnShard>;
    const ui64 TxId;

protected:
    virtual bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        Self->GetProgressTxController().FinishProposeOnExecute(TxId, txc);
        return true;
    }

    virtual void Complete(const TActorContext& ctx) override {
        Self->GetProgressTxController().FinishProposeOnComplete(TxId, ctx);
    }

public:
    TTxProposeFinish(NColumnShard::TColumnShard* self, const ui64 txId)
        : TBase(self)
        , TxId(txId)
    {
    }
};

TString TImportActor::StageToString(EStage stage) {
    switch (stage) {
        case EStage::Initialization:
            return "Initialization";
        case EStage::WaitData:
            return "WaitData";
        case EStage::WaitWriting:
            return "WaitWriting";
        case EStage::WaitSaveCursor:
            return "WaitSaveCursor";
        case EStage::Finished:
            return "Finished";
    }
    return "Unknown";
}

void TImportActor::ScheduleTimeoutCheck() {
    Schedule(TimeoutCheckInterval, new NActors::TEvents::TEvWakeup());
}

void TImportActor::HandleWakeup() {
    if (Stage == EStage::Finished) {
        return;
    }
    const auto elapsed = TInstant::Now() - StageStartTime;
    if (elapsed > OperationTimeout) {
        const TString errorMessage = TStringBuilder() << "Import operation timed out after " << elapsed.Seconds() << "s"
                                                      << " in stage " << StageToString(Stage) << ", tablet " << TabletId << ", importActorId "
                                                      << (ImportActorId ? ImportActorId.ToString() : "none");
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "import_timeout")("stage", StageToString(Stage))("elapsed_sec", elapsed.Seconds())(
            "tablet_id", TabletId)("import_actor_id", ImportActorId ? ImportActorId.ToString() : "none");
        Counters.OnError();
        AbortImport(errorMessage);
        return;
    }
    if (elapsed > WarningInterval) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "import_slow_operation")("stage", StageToString(Stage))(
            "elapsed_sec", elapsed.Seconds())("tablet_id", TabletId)("import_actor_id", ImportActorId ? ImportActorId.ToString() : "none");
    }
    ScheduleTimeoutCheck();
}

void TImportActor::AbortImport(const TString& errorMessage) {
    if (ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "abort_after_import_finished")("message", errorMessage);
        return;
    }
    ImportSession->Abort(errorMessage);
    if (Stage == EStage::WaitData) {
        Counters.OnReadFinished(TInstant::Now() - ReadStartTime);
    } else if (Stage == EStage::WaitWriting) {
        Counters.OnWriteFinished(TInstant::Now() - WriteStartTime);
    }
    Stage = EStage::WaitSaveCursor;
    StageStartTime = TInstant::Now();
    Counters.OnSaveProgressStarted();
    SaveProgressStartTime = TInstant::Now();
    SaveSessionProgress();
}

void TImportActor::PassAway() {
    if (ImportActorId) {
        Send(ImportActorId, new NActors::TEvents::TEvPoisonPill());
        ImportActorId = {};
    }
    Counters.OnActorDead();
    TBase::PassAway();
}

void TImportActor::OnSessionStateSaved() {
    AFL_VERIFY(ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished());
    NYDBTest::TControllers::GetColumnShardController()->OnImportFinished();
    if (ImportSession->GetTxId()) {
        ExecuteTransaction(std::make_unique<TTxProposeFinish>(GetShardVerified<NColumnShard::TColumnShard>(), *ImportSession->GetTxId()));
    } else {
        Session->FinishActor();
    }
}

void TImportActor::Handle(NColumnShard::TEvPrivate::TEvBackupImportRecordBatch::TPtr& ev) {
    Counters.OnReadFinished(TInstant::Now() - ReadStartTime);

    if (ev->Get()->Error) {
        Counters.OnError();
        ImportSession->Abort(ev->Get()->Error);
    } else if (ev->Get()->IsLast) {
        ImportSession->Finish();
    }

    if (!ev->Get()->Data || ev->Get()->IsLast || ev->Get()->Error) {
        Counters.OnSaveProgressStarted();
        SaveProgressStartTime = TInstant::Now();
        SaveSessionProgress();
        return;
    }

    AFL_VERIFY(ev->Get()->Data)("error", "empty record batch");
    NArrow::TBatchSplittingContext context(48 * 1024 * 1024);
    auto blobsSplittedConclusion = NArrow::SplitByBlobSize(ev->Get()->Data, context);
    AFL_VERIFY(blobsSplittedConclusion.IsSuccess());
    AFL_VERIFY(blobsSplittedConclusion.GetResult().size() == 1);
    auto writeEvent = MakeHolder<NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    NKikimr::NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite> writer(*writeEvent);
    TString data = blobsSplittedConclusion.GetResult()[0].GetData();
    writer.AddDataToPayload(std::move(data));

    auto* operation = writeEvent->Record.AddOperations();
    operation->SetPayloadSchema(NArrow::SerializeSchema(*ev->Get()->Data->schema()));
    operation->SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE);
    operation->SetPayloadFormat(NKikimrDataEvents::FORMAT_ARROW);
    operation->SetPayloadIndex(0);
    operation->MutableTableId()->SetTableId(ImportSession->GetTask().GetRestoreTask().GetTableId());
    operation->MutableTableId()->SetSchemaVersion(ImportSession->GetTask().GetSchemaVersion().value_or(0));
    operation->SetIsBulk(true);
    Send(TabletActorId, writeEvent.Release());
    Counters.OnWriteStarted();
    WriteStartTime = TInstant::Now();
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
}

void TImportActor::Handle(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
    Counters.OnWriteFinished(TInstant::Now() - WriteStartTime);
    AFL_VERIFY(ev->Get()->IsComplete())("error", ev->Get()->GetError());
    SwitchStage(EStage::WaitWriting, EStage::WaitData);
    Counters.OnSaveProgressStarted();
    SaveProgressStartTime = TInstant::Now();
    SaveSessionProgress();
}

void TImportActor::SwitchStage(const EStage from, const EStage to) {
    AFL_VERIFY(Stage == from)("from", (ui32)from)("real", (ui32)Stage)("to", (ui32)to);
    Stage = to;
    StageStartTime = TInstant::Now();
}

void TImportActor::OnTxCompleted(const ui64 /*txId*/) {
    Session->FinishActor();
}

void TImportActor::OnBootstrap(const TActorContext& /*ctx*/) {
    Counters.OnActorAlive();
    StageStartTime = TInstant::Now();
    ScheduleTimeoutCheck();

    const auto& restoreTask = ImportSession->GetTask().GetRestoreTask();
    auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));
    auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(
        SelfId(), 0, restoreTask, NKikimr::NDataShard::TTableInfo{ 0, userTable }, ImportSession->GetTask().GetColumns());
    ImportActorId = Register(importActor.release());
    SwitchStage(EStage::Initialization, EStage::WaitData);
    Counters.OnReadStarted();
    ReadStartTime = TInstant::Now();
    Become(&TImportActor::StateFunc);
}

TImportActor::TImportActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter>& adapter)
    : TBase(bgSession, adapter)
{
    ImportSession = bgSession->GetLogicAsVerifiedPtr<NImport::TSession>();
}

void TImportActor::OnSessionProgressSaved() {
    Counters.OnSaveProgressFinished(TInstant::Now() - SaveProgressStartTime);
    if (ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished()) {
        AFL_VERIFY(Stage == EStage::WaitData || Stage == EStage::WaitSaveCursor)("real", (ui32)Stage);
        Stage = EStage::Finished;
        StageStartTime = TInstant::Now();
        SaveSessionState();
    } else {
        AFL_VERIFY(Stage == EStage::WaitData)("real", (ui32)Stage);
        StageStartTime = TInstant::Now();
        AFL_VERIFY(ImportActorId);
        Counters.OnReadStarted();
        ReadStartTime = TInstant::Now();
        TBase::Send(ImportActorId, new NColumnShard::TEvPrivate::TEvBackupImportRecordBatchResult());
    }
}

}   // namespace NKikimr::NOlap::NImport
