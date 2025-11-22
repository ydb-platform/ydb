#include "export_actor.h"
#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NExport {
    
void TActor::SwitchStage(const EStage from, const EStage to) {
    AFL_VERIFY(Stage == from)("from", (ui32)from)("real", (ui32)Stage)("to", (ui32)to);
    Stage = to;
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
    SwitchStage(EStage::Initialization, EStage::WaitData);
    AFL_VERIFY(!ScanActorId);
    auto& msg = ev->Get()->Record;
    ScanActorId = ActorIdFromProto(msg.GetScanActorId());
    TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)TabletId, 1));
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& /*ev*/) {
}

void TActor::OnTxCompleted(const ui64 /*txId*/) {
    Session->FinishActor();
}

void TActor::OnSessionProgressSaved() {
    SwitchStage(EStage::WaitSaveCursor, EStage::WaitData);
    if (ExportSession->GetCursor().IsFinished()) {
        ExportSession->Finish();
        SaveSessionState();
    } else {
        AFL_VERIFY(ScanActorId);
        TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)TabletId, 1));
    }
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult::TPtr& /*ev*/) {
    SwitchStage(EStage::WaitWriting, EStage::WaitSaveCursor);
    AFL_VERIFY(ExportSession->GetCursor().HasLastKey());
    SaveSessionProgress();
}

void TActor::OnBootstrap(const TActorContext& /*ctx*/) {
    NDataShard::IExport::TTableColumns columns;
    int i = 0;
    for (const auto& [name, type] : ExportSession->GetTask().GetColumns()) {
        columns[i++] = NDataShard::TUserTable::TUserColumn(type, "", name, true);
    }
    auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(SelfId(), ExportSession->GetTask().GetBackupTask(), AppData()->DataShardExportFactory, columns, *ExportSession->GetTask().GetTxId());
    Exporter = Register(actor.release());
    auto evStart = BuildRequestInitiator(ExportSession->GetCursor());
    evStart->Record.SetGeneration((ui64)TabletId);
    evStart->Record.SetCSScanPolicy("PLAIN");
    Send(TabletActorId, evStart.release());
    Become(&TActor::StateFunc);
}

TActor::TActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter>& adapter)
    : TBase(bgSession, adapter) {
    ExportSession = bgSession->GetLogicAsVerifiedPtr<NExport::TSession>();
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
    auto data = ev->Get()->ArrowBatch;
    AFL_VERIFY(!!data || ev->Get()->Finished);
    if (data) {
        Send(new IEventHandle(Exporter, SelfId(), new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(NArrow::ToBatch(data), ev->Get()->Finished)));
    } else {
        Send(new IEventHandle(Exporter, SelfId(), new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(nullptr, true)));
    }
    TOwnedCellVec lastKey = ev->Get()->LastKey;
    AFL_VERIFY(!ExportSession->GetCursor().IsFinished());
    ExportSession->MutableCursor().InitNext(ev->Get()->LastKey, ev->Get()->Finished);
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportError::TPtr& /*ev*/) {
    AFL_VERIFY(false);
}

std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> TActor::BuildRequestInitiator(const TCursor& cursor) const {
    const auto& backupTask = ExportSession->GetTask().GetBackupTask();
    auto tablePathId = TInternalPathId::FromRawValue(backupTask.GetTableId());
    auto ev = std::make_unique<NKikimr::TEvDataShard::TEvKqpScan>();
    ev->Record.SetLocalPathId(tablePathId.GetRawValue());

    auto protoRanges = ev->Record.MutableRanges();

    if (cursor.HasLastKey()) {
        auto* newRange = protoRanges->Add();
        TSerializedTableRange(TSerializedCellVec::Serialize(*cursor.GetLastKey()), {}, false, false).Serialize(*newRange);
    }

    ev->Record.MutableSnapshot()->SetStep(backupTask.GetSnapshotStep());
    ev->Record.MutableSnapshot()->SetTxId(backupTask.GetSnapshotTxId());
    ev->Record.SetScanId(tablePathId.GetRawValue());
    ev->Record.SetTxId(tablePathId.GetRawValue());
    ev->Record.SetTablePath(backupTask.GetTableName());
    ev->Record.SetSchemaVersion(0);

    ev->Record.SetReverse(false);
    ev->Record.SetDataFormat(NKikimrDataEvents::EDataFormat::FORMAT_ARROW);
    return ev;
}

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

void TActor::OnSessionStateSaved() {
    AFL_VERIFY(ExportSession->IsFinished());
    NYDBTest::TControllers::GetColumnShardController()->OnExportFinished();
    if (ExportSession->GetTxId()) {
        ExecuteTransaction(std::make_unique<TTxProposeFinish>(GetShardVerified<NColumnShard::TColumnShard>(), *ExportSession->GetTxId()));
    } else {
        Session->FinishActor();
    }
}

}