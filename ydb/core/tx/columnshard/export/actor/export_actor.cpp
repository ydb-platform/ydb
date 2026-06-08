#include "export_actor.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NOlap::NExport {

void TActor::SwitchStage(const EStage from, const EStage to) {
    AFL_VERIFY(Stage == from)("from", (ui32)from)("real", (ui32)Stage)("to", (ui32)to);
    Stage = to;
    StageStartTime = TInstant::Now();
}

TString TActor::StageToString(EStage stage) {
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

void TActor::ScheduleTimeoutCheck() {
    Schedule(TimeoutCheckInterval, new NActors::TEvents::TEvWakeup());
}

void TActor::HandleWakeup() {
    if (Stage == EStage::Finished) {
        return;
    }
    const auto elapsed = TInstant::Now() - StageStartTime;
    if (elapsed > OperationTimeout) {
        const TString errorMessage = TStringBuilder()
                                     << "Export operation timed out after " << elapsed.Seconds() << "s"
                                     << " in stage " << StageToString(Stage) << ", tablet " << TabletId << ", scanActorId "
                                     << (ScanActorId ? ScanActorId->ToString() : "none") << ", exporter " << Exporter.ToString();
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "export_timeout")("stage", StageToString(Stage))("elapsed_sec", elapsed.Seconds())(
            "tablet_id", TabletId)("scan_actor_id", ScanActorId ? ScanActorId->ToString() : "none")("exporter", Exporter.ToString());
        Counters.OnError();
        AbortExport(errorMessage);
        return;
    }
    if (elapsed > WarningInterval) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "export_slow_operation")("stage", StageToString(Stage))(
            "elapsed_sec", elapsed.Seconds())("tablet_id", TabletId)("scan_actor_id", ScanActorId ? ScanActorId->ToString() : "none")(
            "exporter", Exporter.ToString());
    }
    ScheduleTimeoutCheck();
}

void TActor::AbortExport(const TString& errorMessage) {
    ErrorMessage = errorMessage;
    if (ExportSession->GetCursor().IsFinished()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "abort_after_cursor_finished")("message", errorMessage);
        return;
    }
    ExportSession->MutableCursor().InitNext({}, true);
    if (Stage == EStage::WaitData) {
        Counters.OnAckResponse();
        Counters.OnReadFinished(TInstant::Now() - ReadStartTime);
    } else if (Stage == EStage::WaitWriting) {
        Counters.OnWriteFinished(TInstant::Now() - WriteStartTime);
    }
    KillExporter();
    Stage = EStage::WaitSaveCursor;
    StageStartTime = TInstant::Now();
    Counters.OnSaveCursorStarted();
    SaveCursorStartTime = TInstant::Now();
    Become(&TActor::StateError);
    SaveSessionProgress();
}

void TActor::KillExporter() {
    if (Exporter) {
        Send(Exporter, new NActors::TEvents::TEvPoisonPill());
        Exporter = {};
    }
}

void TActor::PassAway() {
    KillExporter();
    Counters.OnActorDead();
    TBase::PassAway();
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
    SwitchStage(EStage::Initialization, EStage::WaitData);
    AFL_VERIFY(!ScanActorId);
    auto& msg = ev->Get()->Record;
    ScanActorId = ActorIdFromProto(msg.GetScanActorId());
    Counters.OnReadStarted();
    Counters.OnAckSent();
    ReadStartTime = TInstant::Now();
    TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)TabletId, 1));
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev) {
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "scan_error")("message", ev->Get()->Record.ShortDebugString());
    Counters.OnError();
    if (ExportSession->GetCursor().IsFinished()) {
        if (Stage == EStage::WaitData) {
            Counters.OnAckResponse();
        }
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "scan_error_after_finish")(
            "message", "ignoring scan error because cursor is already finished");
        return;
    }
    AbortExport("Scan error: " + ev->Get()->Record.ShortDebugString());
}

void TActor::OnTxCompleted(const ui64 /*txId*/) {
    Session->FinishActor();
}

void TActor::OnSessionProgressSaved() {
    Counters.OnSaveCursorFinished(TInstant::Now() - SaveCursorStartTime);
    if (ExportSession->GetCursor().IsFinished()) {
        if (ErrorMessage) {
            ExportSession->Abort(ErrorMessage);
        } else {
            ExportSession->Finish();
        }
        SaveSessionState();
        SwitchStage(EStage::WaitSaveCursor, EStage::Finished);
    } else {
        SwitchStage(EStage::WaitSaveCursor, EStage::WaitData);
        AFL_VERIFY(ScanActorId);
        Counters.OnReadStarted();
        Counters.OnAckSent();
        ReadStartTime = TInstant::Now();
        TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, (ui64)TabletId, 1));
    }
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult::TPtr& /*ev*/) {
    Counters.OnWriteFinished(TInstant::Now() - WriteStartTime);
    SwitchStage(EStage::WaitWriting, EStage::WaitSaveCursor);
    AFL_VERIFY(ExportSession->GetCursor().HasLastKey());
    Counters.OnSaveCursorStarted();
    SaveCursorStartTime = TInstant::Now();
    SaveSessionProgress();
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportError::TPtr& ev) {
    Counters.OnError();
    if (ExportSession->GetCursor().IsFinished()) {
        AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("event", "export_error_after_finish")(
            "message", "ignoring export error because cursor is already finished");
        return;
    }
    AbortExport(ev->Get()->ErrorMessage);
}

void TActor::OnBootstrap(const TActorContext& /*ctx*/) {
    Counters.OnActorAlive();
    StageStartTime = TInstant::Now();
    ScheduleTimeoutCheck();

    // Check if cursor is already finished (can happen after restart)
    if (ExportSession->GetCursor().IsFinished()) {
        AFL_INFO(NKikimrServices::TX_COLUMNSHARD)("event", "export_actor_bootstrap_cursor_finished")("tablet_id", TabletId)(
            "session_id", ExportSession->GetIdentifier().ToString());
        ExportSession->Finish();
        SaveSessionState();
        SwitchStage(EStage::Initialization, EStage::Finished);
        Become(&TActor::StateFunc);
        return;
    }

    NDataShard::IExport::TTableColumns columns;
    int i = 0;
    for (const auto& [name, type] : ExportSession->GetTask().GetColumns()) {
        columns[i++] = NDataShard::TUserTable::TUserColumn(type, "", name, true);
    }
    auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(
        SelfId(), ExportSession->GetTask().GetBackupTask(), AppData()->DataShardExportFactory, columns, *ExportSession->GetTask().GetTxId());
    Exporter = Register(actor.release());
    auto evStart = BuildRequestInitiator(ExportSession->GetCursor());
    evStart->Record.SetGeneration((ui64)TabletId);
    evStart->Record.SetCSScanPolicy("PLAIN");
    Send(TabletActorId, evStart.release());
    Become(&TActor::StateFunc);
}

TActor::TActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter>& adapter)
    : TBase(bgSession, adapter)
{
    ExportSession = bgSession->GetLogicAsVerifiedPtr<NExport::TSession>();
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    Counters.OnAckResponse();
    Counters.OnReadFinished(TInstant::Now() - ReadStartTime);
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
    auto data = ev->Get()->ArrowBatch;
    AFL_VERIFY(!!data || ev->Get()->Finished);
    Counters.OnWriteStarted();
    WriteStartTime = TInstant::Now();
    if (data) {
        Send(new IEventHandle(
            Exporter, SelfId(), new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(NArrow::ToBatch(data), ev->Get()->Finished)));
    } else {
        Send(new IEventHandle(Exporter, SelfId(), new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(nullptr, true)));
    }
    TOwnedCellVec lastKey = ev->Get()->LastKey;
    AFL_VERIFY(!ExportSession->GetCursor().IsFinished());
    ExportSession->MutableCursor().InitNext(ev->Get()->LastKey, ev->Get()->Finished);
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

    // Snapshot will be supported after the implementation of the copy column table
    // ev->Record.MutableSnapshot()->SetStep(backupTask.GetSnapshotStep());
    // ev->Record.MutableSnapshot()->SetTxId(backupTask.GetSnapshotTxId());
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
        , TxId(txId)
    {
    }
};

void TActor::OnSessionStateSaved() {
    AFL_VERIFY(ExportSession->IsFinished() || ExportSession->IsAborted());
    NYDBTest::TControllers::GetColumnShardController()->OnExportFinished();
    if (ExportSession->GetTxId()) {
        ExecuteTransaction(std::make_unique<TTxProposeFinish>(GetShardVerified<NColumnShard::TColumnShard>(), *ExportSession->GetTxId()));
    } else {
        Session->FinishActor();
    }
}

}   // namespace NKikimr::NOlap::NExport
