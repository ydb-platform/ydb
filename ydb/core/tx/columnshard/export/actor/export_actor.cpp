#include "export_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/backup/manifest/manifest.h>
#include <ydb/core/tx/columnshard/backup/manifest/manifest_actor.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NExport {

void TActor::SwitchStage(const EStage from, const EStage to) {
    AFL_VERIFY(Stage == from)("from", from)("real", Stage)("to", to);
    Stage = to;
    StageStartTime = TInstant::Now();
}

bool TActor::IsSavingCursor() const {
    switch (Stage) {
        case EStage::WaitSaveCursor:
        case EStage::WaitSaveCursorBeforeReset:
        case EStage::WaitSaveResetCursor:
        case EStage::WaitSaveCursorBeforeFinish:
        case EStage::WaitSaveFinalCursor:
        case EStage::WaitSaveCursorBeforeNextFile:
        case EStage::WaitSaveCursorBeforeManifest:
            return true;
        default:
            return false;
    }
}

bool TActor::IsFinishing() const {
    return Stage == EStage::WaitSaveCursorBeforeFinish || Stage == EStage::WaitSaveFinalCursor || Stage == EStage::Finished ||
           Stage == EStage::WaitSaveCursorBeforeManifest || Stage == EStage::WaitManifest;
}

void TActor::SaveCursor(EStage stage) {
    Stage = stage;
    AFL_VERIFY(IsSavingCursor())("stage", ::ToString(Stage));
    StageStartTime = TInstant::Now();
    Counters.OnSaveCursorStarted();
    SaveCursorStartTime = TInstant::Now();
    SaveSessionProgress();
}

void TActor::ScheduleTimeoutCheck() {
    Schedule(TimeoutCheckInterval, new NActors::TEvents::TEvWakeup());
}

void TActor::HandleWakeup() {
    if (Stage == EStage::Finished) {
        return;
    }
    if (AllowScanPings && ScanActorId) {
        TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanPing());
    }
    if (IsSavingCursor()) {
        ScheduleTimeoutCheck();
        return;
    }
    const auto elapsed = TInstant::Now() - StageStartTime;
    const auto operationTimeout = TDuration::Seconds(AppDataVerified().ColumnShardConfig.GetBackupSettings().GetExportOperationTimeoutSeconds());
    if (elapsed > operationTimeout) {
        const TString errorMessage = TStringBuilder()
                                     << "Export operation timed out after " << elapsed.Seconds() << "s"
                                     << " in stage " << ::ToString(Stage) << ", tablet " << TabletId << ", scanActorId "
                                     << (ScanActorId ? ScanActorId->ToString() : "none") << ", exporter " << Exporter.ToString();
        YDB_LOG_ERROR("",
            {"event", "export_timeout"},
            {"stage", ::ToString(Stage)},
            {"elapsedSec", elapsed.Seconds()},
            {"tabletId", TabletId},
            {"scanActorId", ScanActorId ? ScanActorId->ToString() : "none"},
            {"exporter", Exporter});
        Counters.OnError();
        AbortExport(errorMessage);
        return;
    }
    if (elapsed > WarningInterval) {
        YDB_LOG_WARN("",
            {"event", "export_slow_operation"},
            {"stage", ::ToString(Stage)},
            {"elapsedSec", elapsed.Seconds()},
            {"tabletId", TabletId},
            {"scanActorId", ScanActorId ? ScanActorId->ToString() : "none"},
            {"exporter", Exporter});
    }
    ScheduleTimeoutCheck();
}

void TActor::AbortExport(const TString& errorMessage) {
    if (ExportSession->IsFinished() || ExportSession->IsReadyForRemoveOnFinished()) {
        YDB_LOG_WARN("",
            {"event", "abort_after_export_finished"},
            {"message", errorMessage});
        return;
    }
    if (IsSavingCursor()) {
        if (!ExportSession->GetCursor().IsFinished()) {
            // Preserve an uploader error until the in-flight cursor transaction
            // completes. Otherwise a failed uploader would leave the export stuck.
            PendingAbortReason = errorMessage;
            KillExporter();
            AbortScanIfKnown(errorMessage);
        }
        YDB_LOG_WARN("",
            {"event", "abort_during_save_cursor"},
            {"message", errorMessage});
        return;
    }
    ExportSession->Abort(errorMessage);
    ExportSession->MutableCursor().InitNext({}, true);
    Counters.OnOperationsCancelled();
    KillExporter();
    KillManifestWriter();
    PendingFileEnd.reset();
    WritingBatch.reset();
    AbortScanIfKnown(errorMessage);
    Become(&TActor::StateError);
    SaveCursor(EStage::WaitSaveFinalCursor);
}

void TActor::KillExporter() {
    if (Exporter) {
        Send(Exporter, new NActors::TEvents::TEvPoisonPill());
        Exporter = {};
    }
}

void TActor::KillManifestWriter() {
    if (ManifestWriter) {
        Send(ManifestWriter, new NActors::TEvents::TEvPoisonPill());
        ManifestWriter = {};
    }
}

void TActor::AbortScanIfKnown(const TString& reason) {
    if (ScanActorId) {
        TBase::Send(*ScanActorId, NKqp::TEvKqp::TEvAbortExecution::Aborted(reason).Release());
        ScanActorId.reset();
    }
}

void TActor::PassAway() {
    YDB_LOG_INFO("",
        {"event", "export_actor_pass_away"},
        {"selfId", SelfId()},
        {"tabletId", TabletId},
        {"stage", ::ToString(Stage)});
    KillExporter();
    KillManifestWriter();
    AbortScanIfKnown("export actor destroyed");
    TBase::PassAway();
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
    if (ev->Get()->Record.GetGeneration() != ScanGeneration || IsFinishing()) {
        TBase::Send(
            ActorIdFromProto(ev->Get()->Record.GetScanActorId()), NKqp::TEvKqp::TEvAbortExecution::Aborted("export scan superseded").Release());
        return;
    }
    SwitchStage(EStage::Initialization, EStage::WaitData);
    AFL_VERIFY(!ScanActorId);
    auto& msg = ev->Get()->Record;
    ScanActorId = ActorIdFromProto(msg.GetScanActorId());
    AllowScanPings = msg.GetAllowPings();
    Counters.OnReadStarted();
    Counters.OnAckSent();
    ReadStartTime = TInstant::Now();
    TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, ScanGeneration, 1));
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev) {
    if (ev->Get()->Record.GetGeneration() != ScanGeneration || IsFinishing()) {
        return;
    }
    YDB_LOG_ERROR("",
        {"event", "scan_error"},
        {"message", ev->Get()->Record});
    Counters.OnError();
    if (ExportSession->IsFinished() || ExportSession->IsReadyForRemoveOnFinished()) {
        if (Stage == EStage::WaitData) {
            Counters.OnAckResponse();
        }
        YDB_LOG_WARN("",
            {"event", "scan_error_after_finish"},
            {"message", "ignoring scan error because export is already finished"});
        return;
    }
    AbortExport("Scan error: " + ev->Get()->Record.ShortDebugString());
}

void TActor::OnTxCompleted(const ui64 /*txId*/) {
    YDB_LOG_INFO("",
        {"event", "export_actor_on_tx_completed"},
        {"selfId", SelfId()},
        {"tabletId", TabletId});
    Session->FinishActor();
}

void TActor::OnSessionProgressSaved() {
    AFL_VERIFY(IsSavingCursor())("stage", ::ToString(Stage));
    Counters.OnSaveCursorFinished(TInstant::Now() - SaveCursorStartTime);
    if (ExportSession->IsFinished() || ExportSession->IsReadyForRemoveOnFinished()) {
        Stage = EStage::Finished;
        StageStartTime = TInstant::Now();
        SaveSessionState();
        return;
    }
    if (PendingAbortReason) {
        // Leave the cursor-save stage so AbortExport can finish the deferred abort.
        SwitchStage(Stage, EStage::WaitData);
        AbortExport(*std::exchange(PendingAbortReason, std::nullopt));
        return;
    }
    switch (Stage) {
        case EStage::WaitSaveCursorBeforeReset:
            return ResetCursor();
        case EStage::WaitSaveResetCursor:
            return StartScan();
        case EStage::WaitSaveCursorBeforeFinish:
            return OnFileCompleted();
        case EStage::WaitSaveCursorBeforeNextFile:
            CreateExporter();
            return StartScan();
        case EStage::WaitSaveCursorBeforeManifest:
            return WriteManifest();
        case EStage::WaitSaveFinalCursor:
            AFL_VERIFY(ExportSession->GetCursor().IsFinished());
            if (ExportSession->IsStarted()) {
                ExportSession->Finish();
            }
            SaveSessionState();
            SwitchStage(EStage::WaitSaveFinalCursor, EStage::Finished);
            return;
        case EStage::WaitSaveCursor:
            SwitchStage(EStage::WaitSaveCursor, EStage::WaitData);
            AFL_VERIFY(ScanActorId);
            Counters.OnReadStarted();
            Counters.OnAckSent();
            ReadStartTime = TInstant::Now();
            TBase::Send(*ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(FreeSpace, ScanGeneration, 1));
            return;
        default:
            AFL_VERIFY(false)("stage", ::ToString(Stage));
    }
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportRecordBatchResult::TPtr& ev) {
    if (ev->Get()->Generation != ScanGeneration || IsFinishing() || PendingAbortReason) {
        return;
    }
    if (ev->Get()->IsFinish) {
        if (!PendingFileEnd) {
            Counters.OnError();
            AbortExport("Export uploader finished a data file before its last batch");
            return;
        }
        // A retried CompleteMultipartUpload can finish before the rescan has
        // reached the end of the file, including while the preceding batch's cursor is saving.
        AbortScanIfKnown("data file completed");
        WritingBatch.reset();
        if (IsSavingCursor()) {
            Stage = EStage::WaitSaveCursorBeforeFinish;
            return;
        }
        if (Stage == EStage::WaitData) {
            Counters.OnAckResponse();
            Counters.OnReadFinished(TInstant::Now() - ReadStartTime);
        } else if (Stage == EStage::WaitWriting) {
            Counters.OnWriteFinished(TInstant::Now() - WriteStartTime);
        }
        OnFileCompleted();
        return;
    }
    AFL_VERIFY(Stage == EStage::WaitWriting)("stage", ::ToString(Stage));
    if (const auto fileRows = ev->Get()->DataFileEndRows) {
        // The uploader completes the data file with the first rows of the batch, the next file starts after them
        AFL_VERIFY(IsMultiFile());
        AFL_VERIFY(!PendingFileEnd || PendingFileEnd->IsEof);
        AFL_VERIFY(WritingBatch && *fileRows > 0 && *fileRows <= (ui64)WritingBatch->num_rows())("rows", *fileRows);
        auto lastKey = GetPrimaryKey(*WritingBatch, *fileRows - 1);
        if (lastKey.IsFail()) {
            Counters.OnError();
            return AbortExport("Cannot build the last key of the export data file: " + lastKey.GetErrorMessage());
        }
        PendingFileEnd = TPendingFileEnd{ .LastKey = lastKey.DetachResult(), .IsEof = false };
        WritingBatch.reset();
        AbortScanIfKnown("data file ended");
        return;
    }
    Counters.OnWriteFinished(TInstant::Now() - WriteStartTime);
    WritingBatch.reset();
    AFL_VERIFY(ExportSession->GetCursor().HasLastKey());
    SaveCursor(EStage::WaitSaveCursor);
}

bool TActor::IsMultiFile() const {
    return ExportSession->GetCursor().IsMultiFile();
}

TConclusion<TOwnedCellVec> TActor::GetPrimaryKey(const arrow::RecordBatch& batch, const ui64 row) const {
    const auto& index = GetShardVerified<NColumnShard::TColumnShard>()->GetIndexVerified();
    const auto& keyColumns = index.GetVersionedIndex().GetLastSchema()->GetIndexInfo().GetPrimaryKeyColumns();
    const auto rowBatch = batch.Slice(row, 1);
    arrow::FieldVector fields;
    std::vector<std::shared_ptr<arrow::Array>> columns;
    for (const auto& [name, type] : keyColumns) {
        const int idx = rowBatch->schema()->GetFieldIndex(name);
        if (idx < 0) {
            return TConclusionStatus::Fail("no key column " + name + " in the exported data");
        }
        fields.emplace_back(rowBatch->schema()->field(idx));
        columns.emplace_back(rowBatch->column(idx));
    }

    struct TSingleRowWriter: public NArrow::IRowWriter {
        std::optional<TOwnedCellVec> Row;

        void AddRow(const TConstArrayRef<TCell>& row) override {
            AFL_VERIFY(!Row);
            Row = TOwnedCellVec::Make(row);
        }
    } writer;

    NArrow::TArrowToYdbConverter converter(keyColumns, writer, false, false);
    TString error;
    if (!converter.Process(*arrow::RecordBatch::Make(arrow::schema(fields), 1, columns), error)) {
        return TConclusionStatus::Fail(error);
    }
    AFL_VERIFY(writer.Row);
    return std::move(*writer.Row);
}

void TActor::OnFileCompleted() {
    AFL_VERIFY(PendingFileEnd);
    const auto fileEnd = *std::exchange(PendingFileEnd, std::nullopt);
    Stage = EStage::WaitData;
    StageStartTime = TInstant::Now();
    // The uploader of the completed file stops by itself; the events of its scan are dropped
    Exporter = {};
    ++ScanGeneration;

    auto& cursor = ExportSession->MutableCursor();
    if (!IsMultiFile()) {
        AFL_VERIFY(fileEnd.IsEof);
        return FinishExport();
    }
    YDB_LOG_INFO("",
        {"event", "export_data_file_completed"},
        {"tabletId", TabletId},
        {"fileIdx", cursor.GetFileIdx()},
        {"isLast", fileEnd.IsEof});
    if (fileEnd.IsEof) {
        cursor.FinishData();
        return SaveCursor(EStage::WaitSaveCursorBeforeManifest);
    }
    if (cursor.GetFileIdx() + 1 >= NKikimr::NColumnShard::NBackup::MaxDataFileIndex) {
        Counters.OnError();
        return AbortExport(TStringBuilder() << "Too many export data files in shard, increase BytesBatchSize");
    }
    AFL_VERIFY(fileEnd.LastKey);
    cursor.StartNextFile(*fileEnd.LastKey);
    SaveCursor(EStage::WaitSaveCursorBeforeNextFile);
}

void TActor::WriteManifest() {
    const auto& cursor = ExportSession->GetCursor();
    AFL_VERIFY(cursor.IsDataFinished());
    Stage = EStage::WaitManifest;
    StageStartTime = TInstant::Now();
    NColumnShard::NBackup::TDataManifest manifest;
    manifest.FilesCount = cursor.GetFileIdx();
    ManifestWriter =
        Register(NColumnShard::NBackup::CreateDataManifestWriter(SelfId(), ExportSession->GetTask().GetBackupTask(), manifest).release());
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupManifestWriteResult::TPtr& ev) {
    if (ev->Sender != ManifestWriter || Stage != EStage::WaitManifest) {
        return;
    }
    ManifestWriter = {};
    if (ev->Get()->Error) {
        Counters.OnError();
        return AbortExport("Data manifest upload failed: " + ev->Get()->Error);
    }
    FinishExport();
}

void TActor::FinishExport() {
    ExportSession->MutableCursor().InitNext({}, true);
    SaveCursor(EStage::WaitSaveFinalCursor);
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportReset::TPtr& ev) {
    if (PendingAbortReason || IsFinishing()) {
        return;
    }
    AFL_VERIFY(ev->Sender == Exporter);
    AFL_VERIFY(ev->Get()->Generation == ScanGeneration + 1);
    ScanGeneration = ev->Get()->Generation;
    AbortScanIfKnown("export scan reset");
    if (IsSavingCursor()) {
        // Do not change the session cursor while its transaction is in flight.
        Stage = EStage::WaitSaveCursorBeforeReset;
        return;
    }
    if (Stage == EStage::WaitData) {
        Counters.OnAckResponse();
        Counters.OnReadFinished(TInstant::Now() - ReadStartTime);
    } else if (Stage == EStage::WaitWriting) {
        Counters.OnWriteFinished(TInstant::Now() - WriteStartTime);
    }
    ResetCursor();
}

void TActor::ResetCursor() {
    // The uploader restarts the data file being written
    ExportSession->MutableCursor().RewindToFileStart();
    SaveCursor(EStage::WaitSaveResetCursor);
}

void TActor::StartScan() {
    Stage = EStage::Initialization;
    StageStartTime = TInstant::Now();
    auto evStart = BuildRequestInitiator(ExportSession->GetCursor());
    evStart->Record.SetGeneration(ScanGeneration);
    evStart->Record.SetCSScanPolicy("EXPORT");
    Send(TabletActorId, evStart.release());
}

void TActor::HandleExecute(NColumnShard::TEvPrivate::TEvBackupExportError::TPtr& ev) {
    Counters.OnError();
    if (ExportSession->IsFinished() || ExportSession->IsReadyForRemoveOnFinished()) {
        YDB_LOG_WARN("",
            {"event", "export_error_after_finish"},
            {"message", "ignoring export error because export is already finished"});
        return;
    }
    AbortExport(ev->Get()->ErrorMessage);
}

void TActor::OnBootstrap(const TActorContext& /*ctx*/) {
    YDB_LOG_INFO("",
        {"event", "export_actor_bootstrap"},
        {"selfId", SelfId()},
        {"tabletId", TabletId});
    Counters.OnActorAlive();
    StageStartTime = TInstant::Now();
    ScheduleTimeoutCheck();

    // Check if cursor is already finished (can happen after restart)
    if (ExportSession->GetCursor().IsFinished()) {
        YDB_LOG_INFO("",
            {"event", "export_actor_bootstrap_cursor_finished"},
            {"tabletId", TabletId},
            {"sessionId", ExportSession->GetIdentifier()});
        if (ExportSession->IsStarted()) {
            ExportSession->Finish();
        }
        SaveSessionState();
        SwitchStage(EStage::Initialization, EStage::Finished);
        Become(&TActor::StateFunc);
        return;
    }

    auto& cursor = ExportSession->MutableCursor();
    const auto& backupTask = ExportSession->GetTask().GetBackupTask();
    if (!cursor.HasMultiFile()) {
        // Only S3 exports write the data manifest
        cursor.SetMultiFile(backupTask.HasS3Settings() && AppDataVerified().FeatureFlags.GetEnableColumnShardExportMultiFile());
    }
    Become(&TActor::StateFunc);
    if (IsMultiFile() && backupTask.HasEncryptionSettings() &&
        backupTask.GetShardNum() >= NKikimr::NColumnShard::NBackup::MaxDataFileShardNumber) {
        Counters.OnError();
        AbortExport(TStringBuilder() << "Encrypted exports with several data files per shard support up to "
                                     << NKikimr::NColumnShard::NBackup::MaxDataFileShardNumber << " shards");
        return;
    }
    if (cursor.IsDataFinished()) {
        WriteManifest();
        return;
    }
    // The multipart upload of the data file being written is lost with the previous uploader
    cursor.RewindToFileStart();
    CreateExporter();
    StartScan();
}

void TActor::CreateExporter() {
    AFL_VERIFY(!Exporter);
    NDataShard::IExport::TTableColumns columns;
    int i = 0;
    for (const auto& [name, type] : ExportSession->GetTask().GetColumns()) {
        columns[i++] = NDataShard::TUserTable::TUserColumn(type, "", name, true);
    }
    auto backupTask = ExportSession->GetTask().GetBackupTask();
    ui32 maxDataFileParts = 0;
    if (IsMultiFile()) {
        NKikimr::NColumnShard::NBackup::SetDataFile(backupTask, ExportSession->GetCursor().GetFileIdx());
        maxDataFileParts = NYDBTest::TControllers::GetColumnShardController()->GetExportDataFileMaxParts();
    }
    auto actor = NKikimr::NColumnShard::NBackup::CreateExportUploaderActor(
        SelfId(), backupTask, AppData()->DataShardExportFactory, columns, *ExportSession->GetTask().GetTxId(), ScanGeneration, maxDataFileParts);
    Exporter = Register(actor.release());
}

TActor::TActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter>& adapter)
    : TBase(bgSession, adapter)
{
    ExportSession = bgSession->GetLogicAsVerifiedPtr<NExport::TSession>();
}

void TActor::HandleExecute(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
    if (ev->Get()->Generation != ScanGeneration || IsFinishing()) {
        return;
    }
    Counters.OnAckResponse();
    Counters.OnReadFinished(TInstant::Now() - ReadStartTime);
    auto data = ev->Get()->ArrowBatch;
    const bool finished = ev->Get()->Finished;
    AFL_VERIFY(!!data || finished);
    auto& cursor = ExportSession->MutableCursor();
    std::shared_ptr<arrow::RecordBatch> batch = data ? NArrow::ToBatch(data) : nullptr;
    if (batch && batch->num_rows()) {
        cursor.AddFileRows(batch->num_rows());
    }
    if (finished && !PendingFileEnd && IsMultiFile() && cursor.GetFileIdx() > 0 && cursor.GetFileRows() == 0) {
        // The previous data file ended right before the end of the data: its uploader has nothing to upload
        YDB_LOG_INFO("",
            {"event", "export_empty_data_file_skipped"},
            {"tabletId", TabletId},
            {"fileIdx", cursor.GetFileIdx()});
        KillExporter();
        ++ScanGeneration;
        cursor.FinishDataWithoutCurrentFile();
        return SaveCursor(EStage::WaitSaveCursorBeforeManifest);
    }
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
    Counters.OnWriteStarted();
    WriteStartTime = TInstant::Now();
    if (finished && !PendingFileEnd) {
        PendingFileEnd = TPendingFileEnd{ .LastKey = std::nullopt, .IsEof = true };
    }
    // A rescan of a data file with the known end is bounded by its last key
    const bool isLast = finished;
    WritingBatch = batch;
    Send(new IEventHandle(Exporter, SelfId(), new NColumnShard::TEvPrivate::TEvBackupExportRecordBatch(batch, isLast, ScanGeneration)));
    AFL_VERIFY(!cursor.IsFinished());
    cursor.InitNext(ev->Get()->LastKey, false);
}

std::unique_ptr<NKikimr::TEvDataShard::TEvKqpScan> TActor::BuildRequestInitiator(const TCursor& cursor) const {
    const auto& backupTask = ExportSession->GetTask().GetBackupTask();
    auto tablePathId = TInternalPathId::FromRawValue(backupTask.GetTableId());
    auto ev = std::make_unique<NKikimr::TEvDataShard::TEvKqpScan>();
    ev->Record.SetLocalPathId(tablePathId.GetRawValue());

    auto protoRanges = ev->Record.MutableRanges();

    // A rescan of a data file whose last batch was already passed to the uploader has to end with the same key
    const TOwnedCellVec* toKey = (PendingFileEnd && !PendingFileEnd->IsEof) ? &*PendingFileEnd->LastKey : nullptr;
    if (cursor.HasLastKey() || toKey) {
        auto* newRange = protoRanges->Add();
        const TString from = cursor.HasLastKey() ? TSerializedCellVec::Serialize(*cursor.GetLastKey()) : TString();
        const TString to = toKey ? TSerializedCellVec::Serialize(*toKey) : TString();
        TSerializedTableRange(from, to, !cursor.HasLastKey(), !!toKey).Serialize(*newRange);
    }

    // Snapshot will be supported after the implementation of the copy column table
    // ev->Record.MutableSnapshot()->SetStep(backupTask.GetSnapshotStep());
    // ev->Record.MutableSnapshot()->SetTxId(backupTask.GetSnapshotTxId());
    ev->Record.SetScanId(tablePathId.GetRawValue());
    const auto txId = ExportSession->GetTxId();
    AFL_VERIFY(txId)("reason", "export_session_has_no_tx_id");
    ev->Record.SetTxId(*txId);
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
    const NActors::TActorId ProgressActorId;
    const ui64 TxInternalId;

protected:
    virtual bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) override {
        Self->GetProgressTxController().FinishProposeOnExecute(TxId, txc);
        return true;
    }

    virtual void Complete(const TActorContext& ctx) override {
        ctx.Send(ProgressActorId, new NBackground::TEvLocalTransactionCompleted(TxInternalId));
        Self->GetProgressTxController().FinishProposeOnComplete(TxId, ctx);
    }

public:
    TTxProposeFinish(NColumnShard::TColumnShard* self, const ui64 txId, const NActors::TActorId& progressActorId, const ui64 txInternalId)
        : TBase(self)
        , TxId(txId)
        , ProgressActorId(progressActorId)
        , TxInternalId(txInternalId)
    {
    }
};

void TActor::OnSessionStateSaved() {
    AFL_VERIFY(ExportSession->IsFinished() || ExportSession->IsReadyForRemoveOnFinished());
    NYDBTest::TControllers::GetColumnShardController()->OnExportFinished();
    if (ExportSession->GetTxId()) {
        YDB_LOG_INFO("",
            {"event", "export_actor_session_state_saved_with_tx"},
            {"selfId", SelfId()},
            {"tabletId", TabletId},
            {"txId", *ExportSession->GetTxId()});
        ExecuteTransaction(std::make_unique<TTxProposeFinish>(
            GetShardVerified<NColumnShard::TColumnShard>(), *ExportSession->GetTxId(), SelfId(), GetNextTxId()));
    } else {
        YDB_LOG_INFO("",
            {"event", "export_actor_session_state_saved_no_tx"},
            {"selfId", SelfId()},
            {"tabletId", TabletId});
        Session->FinishActor();
    }
}

}   // namespace NKikimr::NOlap::NExport
