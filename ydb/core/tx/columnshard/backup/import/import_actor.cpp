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
        , TxId(txId) {
    }
};

void TImportActor::OnSessionStateSaved() {
    AFL_VERIFY(ImportSession->IsFinished());
    NYDBTest::TControllers::GetColumnShardController()->OnImportFinished();
    if (ImportSession->GetTxId()) {
        ExecuteTransaction(std::make_unique<TTxProposeFinish>(GetShardVerified<NColumnShard::TColumnShard>(), *ImportSession->GetTxId()));
    } else {
        Session->FinishActor();
    }
}

void TImportActor::Handle(NColumnShard::TEvPrivate::TEvBackupImportRecordBatch::TPtr& ev) {
    AFL_VERIFY(!ev->Get()->Error)("error", ev->Get()->Error);
    
    if (ev->Get()->IsLast) {
        ImportSession->Finish();
    }
    
    if (!ev->Get()->Data && ev->Get()->IsLast) {
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
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
}

void TImportActor::Handle(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
    AFL_VERIFY(ev->Get()->IsComplete())("error", ev->Get()->GetError());
    SwitchStage(EStage::WaitWriting, EStage::WaitData);
    SaveSessionProgress();
}

void TImportActor::SwitchStage(const EStage from, const EStage to) {
    AFL_VERIFY(Stage == from)("from", (ui32)from)("real", (ui32)Stage)("to",
                                                                        (ui32)to);
    Stage = to;
}

void TImportActor::OnTxCompleted(const ui64 /*txId*/) {
    Session->FinishActor();
}

void TImportActor::OnBootstrap(const TActorContext & /*ctx*/) {
    const auto& restoreTask = ImportSession->GetTask().GetRestoreTask();
    auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));
    auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(SelfId(), 0, restoreTask, NKikimr::NDataShard::TTableInfo{0, userTable}, ImportSession->GetTask().GetColumns());
    ImportActorId = Register(importActor.release());
    SwitchStage(EStage::Initialization, EStage::WaitData);
    Become(&TImportActor::StateFunc);
}

TImportActor::TImportActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter> &adapter)
    : TBase(bgSession, adapter) {
    ImportSession = bgSession->GetLogicAsVerifiedPtr<NImport::TSession>();
}

void TImportActor::OnSessionProgressSaved() {
    SwitchStage(EStage::WaitData, EStage::WaitData);
    if (ImportSession->IsFinished()) {
        SaveSessionState();
    } else {
        AFL_VERIFY(ImportActorId);
        TBase::Send(ImportActorId, new NColumnShard::TEvPrivate::TEvBackupImportRecordBatchResult());
    }
}

} // namespace NKikimr::NOlap::NImport