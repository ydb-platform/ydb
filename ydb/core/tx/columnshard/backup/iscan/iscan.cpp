#include "iscan.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_s3_upload.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/library/signals/owner.h>

namespace NKikimr::NColumnShard::NBackup {

class TUploaderCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr BatchesReceived;
    NMonitoring::TDynamicCounters::TCounterPtr RowsFed;
    NMonitoring::TDynamicCounters::TCounterPtr IntermediateResultsSent;
    NMonitoring::TDynamicCounters::TCounterPtr FinalResultsSent;
    NMonitoring::TDynamicCounters::TCounterPtr SleepCount;
    NMonitoring::TDynamicCounters::TCounterPtr WakeupCount;
    NMonitoring::TDynamicCounters::TCounterPtr ErrorCount;
    NMonitoring::TDynamicCounters::TCounterPtr QueueSize;
    NMonitoring::TDynamicCounters::TCounterPtr UploaderSleeping;

public:
    TUploaderCounters()
        : TBase("UploaderActor")
    {
        BatchesReceived = TBase::GetDeriviative("Uploader/Batches/Received");
        RowsFed = TBase::GetDeriviative("Uploader/Rows/Fed");
        IntermediateResultsSent = TBase::GetDeriviative("Uploader/IntermediateResults/Sent");
        FinalResultsSent = TBase::GetDeriviative("Uploader/FinalResults/Sent");
        SleepCount = TBase::GetDeriviative("Uploader/Sleep/Count");
        WakeupCount = TBase::GetDeriviative("Uploader/Wakeup/Count");
        ErrorCount = TBase::GetDeriviative("Uploader/Errors/Count");
        QueueSize = TBase::GetValue("Uploader/Queue/Size");
        UploaderSleeping = TBase::GetValue("Uploader/Sleeping");
    }

    void OnBatchReceived() const {
        BatchesReceived->Inc();
    }

    void OnRowsFed(ui64 count) const {
        *RowsFed += count;
    }

    void OnIntermediateResultSent() const {
        IntermediateResultsSent->Inc();
    }

    void OnFinalResultSent() const {
        FinalResultsSent->Inc();
    }

    void OnSleep() const {
        SleepCount->Inc();
        UploaderSleeping->Set(1);
    }

    void OnWakeup() const {
        WakeupCount->Inc();
        UploaderSleeping->Set(0);
    }

    void OnError() const {
        ErrorCount->Inc();
    }

    void SetQueueSize(ui64 size) const {
        QueueSize->Set(size);
    }

    void ResetSleeping() const {
        UploaderSleeping->Set(0);
    }
};

TRowWriter::TRowWriter(TVector<TSerializedCellVec>& rows)
    : Rows(rows)
{
}

void TRowWriter::AddRow(const TConstArrayRef<TCell>& cells) {
    TSerializedCellVec serializedKey(cells);
    Rows.emplace_back(std::move(serializedKey));
}

TConclusion<TVector<TSerializedCellVec>> BatchToRows(
    const std::shared_ptr<arrow::RecordBatch>& batch, const TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema) {
    Y_ABORT_UNLESS(batch);
    TVector<TSerializedCellVec> cellVecs;
    cellVecs.reserve(batch->num_rows());
    TRowWriter writer(cellVecs);
    NArrow::TArrowToYdbConverter batchConverter(ydbSchema, writer, false, false);
    TString errorMessage;
    if (!batchConverter.Process(*batch, errorMessage)) {
        return TConclusionStatus::Fail(errorMessage);
    }
    return cellVecs;
}

void TExportDriver::Touch(NTable::EScan state) {
    ActorSystem->Send(SubscriberActorId, new TEvPrivate::TEvBackupExportState(state));
}

void TExportDriver::Throw(const std::exception& e) {
    ActorSystem->Send(SubscriberActorId, new TEvPrivate::TEvBackupExportError(e.what()));
}

ui64 TExportDriver::GetTotalCpuTimeUs() const {
    return 0;
}

TConclusion<std::unique_ptr<NTable::IScan>> CreateIScanExportUploader(const TActorId& subscriberActorId,
    const NKikimrSchemeOp::TBackupTask& backupTask, const NDataShard::IExportFactory* exportFactory,
    const NDataShard::IExport::TTableColumns& tableColumns, ui64 txId) {
    std::shared_ptr<::NKikimr::NDataShard::IExport> exp;
    switch (backupTask.GetSettingsCase()) {
        case NKikimrSchemeOp::TBackupTask::kYTSettings:
            if (backupTask.HasCompression()) {
                return TConclusionStatus::Fail("Exports to YT do not support compression");
            }
            if (exportFactory) {
                exp = std::shared_ptr<NDataShard::IExport>(exportFactory->CreateExportToYt(backupTask, tableColumns));
            } else {
                return TConclusionStatus::Fail("Exports to YT are disabled");
            }
            break;
        case NKikimrSchemeOp::TBackupTask::kS3Settings:
            NDataShard::NBackupRestoreTraits::ECompressionCodec codec;
            if (!TryCodecFromTask(backupTask, codec)) {
                return TConclusionStatus::Fail(TStringBuilder() << "Unsupported compression codec: " << backupTask.GetCompression().GetCodec());
            }
            if (exportFactory) {
                exp = std::shared_ptr<NDataShard::IExport>(exportFactory->CreateExportToS3(backupTask, tableColumns));
            } else {
                return TConclusionStatus::Fail("Exports to S3 are disabled");
            }
            break;
        case NKikimrSchemeOp::TBackupTask::kFSSettings:
            return TConclusionStatus::Fail("Exports to FS are not supported");
        case NKikimrSchemeOp::TBackupTask::SETTINGS_NOT_SET:
            return TConclusionStatus::Fail("Internal error. It is not possible to have empty settings for backup here");
    }

    auto createUploader = [subscriberActorId = subscriberActorId, txId = txId, exp]() {
        return exp->CreateUploader(subscriberActorId, txId);
    };

    THolder<NKikimr::NDataShard::NExportScan::IBuffer> buffer{ exp->CreateBuffer() };
    std::unique_ptr<NTable::IScan> scan{ NDataShard::CreateExportScan(std::move(buffer), createUploader) };

    return scan;
}

TExportDriver::TExportDriver(TActorSystem* actorSystem, const TActorId& subscriberActorId)
    : ActorSystem(actorSystem)
    , SubscriberActorId(subscriberActorId)
{
}

class TUploaderActor: public TActorBootstrapped<TUploaderActor> {
    static constexpr TDuration DiagnosticInterval = TDuration::Seconds(30);

public:
    struct TBatchItem {
        std::shared_ptr<arrow::RecordBatch> Data;
        bool IsLast = false;
    };

    struct TCurrentBatchItem {
        ui64 Position = 0;
        TVector<TSerializedCellVec> Batch;
        bool IsLast = false;
        bool NeedResult = false;

        void Clear() {
            Batch.clear();
            Position = 0;
            IsLast = false;
            NeedResult = true;
        }

        TSerializedCellVec& GetRow() {
            return Batch[Position];
        }

        bool HasMoreRows() const {
            return Position < Batch.size();
        }
    };

    TUploaderActor(const NKikimrSchemeOp::TBackupTask& backupTask, const NDataShard::IExportFactory* exportFactory,
        const NDataShard::IExport::TTableColumns& tableColumns, const TActorId& subscriberActorId, ui64 txId)
        : BackupTask(backupTask)
        , ExportFactory(exportFactory)
        , TableColumns(tableColumns)
        , SubscriberActorId(subscriberActorId)
        , TxId(txId)
        , ColumnTypes(MakeColumnTypes())
    {
    }

    void Bootstrap() {
        auto exporter = NColumnShard::NBackup::CreateIScanExportUploader(SelfId(), BackupTask, ExportFactory, TableColumns, TxId);
        if (!exporter.IsSuccess()) {
            Fail(exporter.GetErrorMessage());
            return;
        }

        Driver = std::make_unique<NColumnShard::NBackup::TExportDriver>(TActorContext::ActorSystem(), SelfId());
        Exporter = exporter.DetachResult().release();
        auto initialState = Exporter->Prepare(Driver.get(), MakeRowSchema());
        AFL_VERIFY(initialState.Scan == NTable::EScan::Feed);

        NTable::TLead lead;
        auto seekState = Exporter->Seek(lead, 0);
        AFL_VERIFY(seekState == NTable::EScan::Feed);
        LastActivityTime = TInstant::Now();
        ScheduleDiagnostics();
        Become(&TThis::StateMain);
    }

    void ScheduleDiagnostics() {
        Schedule(DiagnosticInterval, new NActors::TEvents::TEvWakeup());
    }

    void HandleWakeup() {
        const auto elapsed = TInstant::Now() - LastActivityTime;
        if (elapsed > DiagnosticInterval) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("component", "TUploaderActor")("event", "slow_upload")("elapsed_sec", elapsed.Seconds())(
                "last_state", LastState == NTable::EScan::Sleep ? "Sleep" : "Feed")("queue_size", DataQueue.size())(
                "current_batch_rows", CurrentBatch.Batch.size())("current_batch_position", CurrentBatch.Position)(
                "current_batch_is_last", CurrentBatch.IsLast)("total_batches_received", TotalBatchesReceived)("total_rows_fed", TotalRowsFed)(
                "total_intermediate_results", TotalIntermediateResults)("subscriber", SubscriberActorId.ToString());
        }
        ScheduleDiagnostics();
    }

    void Fail(const TString& errorMessage) {
        Counters.OnError();
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("component", "TUploaderActor")("error", errorMessage);
        Send(SubscriberActorId, new TEvPrivate::TEvBackupExportError(errorMessage));
        Counters.ResetSleeping();
        Counters.SetQueueSize(0);
        if (Exporter) {
            Exporter->Finish(NTable::EStatus::Done);
        }
        PassAway();
    }

    STRICT_STFUNC(StateMain,
        hFunc(TEvPrivate::TEvBackupExportError, Handle) hFunc(TEvPrivate::TEvBackupExportState, Handle)
            hFunc(TEvPrivate::TEvBackupExportRecordBatch, Handle) hFunc(TEvDataShard::TEvGetS3Upload, HandleGetS3Upload) hFunc(
                TEvDataShard::TEvStoreS3UploadId, HandleStoreS3UploadId) hFunc(TEvDataShard::TEvChangeS3UploadStatus, HandleChangeS3UploadStatus)
                cFunc(NActors::TEvents::TEvWakeup::EventType, HandleWakeup) cFunc(NActors::TEvents::TEvPoisonPill::EventType, HandlePoisonPill))

    void HandlePoisonPill() {
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("component", "TUploaderActor")("event", "poison_pill");
        Counters.ResetSleeping();
        Counters.SetQueueSize(0);
        if (Exporter) {
            Exporter->Finish(NTable::EStatus::Done);
        }
        PassAway();
    }

    void HandleGetS3Upload(TEvDataShard::TEvGetS3Upload::TPtr& ev) {
        const auto& msg = *ev->Get();
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("component", "TUploaderActor")("event", "GetS3Upload")("tx_id", msg.TxId);
        auto it = S3Uploads.find(msg.TxId);
        if (it != S3Uploads.end()) {
            Send(msg.ReplyTo, new TEvDataShard::TEvS3Upload(it->second), 0, ev->Cookie);
        } else {
            Send(msg.ReplyTo, new TEvDataShard::TEvS3Upload(), 0, ev->Cookie);
        }
    }

    void HandleStoreS3UploadId(TEvDataShard::TEvStoreS3UploadId::TPtr& ev) {
        const auto& msg = *ev->Get();
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("component", "TUploaderActor")("event", "StoreS3UploadId")("tx_id", msg.TxId)(
            "upload_id", msg.UploadId);
        auto [it, inserted] = S3Uploads.emplace(msg.TxId, NDataShard::TS3Upload(msg.UploadId));
        if (!inserted) {
            it->second.Id = msg.UploadId;
            it->second.Status = NDataShard::TS3Upload::EStatus::UploadParts;
            it->second.Error.Clear();
            it->second.Parts.clear();
        }
        Send(msg.ReplyTo, new TEvDataShard::TEvS3Upload(it->second), 0, ev->Cookie);
    }

    void HandleChangeS3UploadStatus(TEvDataShard::TEvChangeS3UploadStatus::TPtr& ev) {
        auto& msg = *ev->Get();
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("component", "TUploaderActor")("event", "ChangeS3UploadStatus")("tx_id", msg.TxId)(
            "status", static_cast<ui8>(msg.Status));
        auto it = S3Uploads.find(msg.TxId);
        AFL_VERIFY(it != S3Uploads.end())("tx_id", msg.TxId)("event", "ChangeS3UploadStatus without prior StoreS3UploadId");
        it->second.Status = msg.Status;
        it->second.Error = std::move(msg.Error);
        it->second.Parts = std::move(msg.Parts);
        Send(msg.ReplyTo, new TEvDataShard::TEvS3Upload(it->second), 0, ev->Cookie);
    }

    void Handle(const TEvPrivate::TEvBackupExportRecordBatch::TPtr& ev) {
        const auto& event = *ev.Get()->Get();
        DataQueue.emplace(event.Data, event.IsLast);
        ++TotalBatchesReceived;
        Counters.OnBatchReceived();
        Counters.SetQueueSize(DataQueue.size());
        LastActivityTime = TInstant::Now();
        UploadData();
    }

    void Handle(const TEvPrivate::TEvBackupExportError::TPtr& ev) {
        Fail(ev.Get()->Get()->ErrorMessage);
    }

    void UploadData() {
        if (LastState == NTable::EScan::Sleep) {
            return;
        }
        DrainQueue();
        SendIntermediateResult();
        MarkAsLast();
    }

    void SendIntermediateResult() {
        if (!CurrentBatch.HasMoreRows() && !CurrentBatch.IsLast && CurrentBatch.NeedResult) {
            Send(SubscriberActorId, new TEvPrivate::TEvBackupExportRecordBatchResult(false));
            CurrentBatch.NeedResult = false;
            ++TotalIntermediateResults;
            Counters.OnIntermediateResultSent();
            LastActivityTime = TInstant::Now();
        }
    }

    void SendFinalResult() {
        auto result = Exporter->Finish(NTable::EStatus::Done);
        auto* scanProduct = static_cast<NDataShard::TExportScanProduct*>(result.Get());
        Counters.OnFinalResultSent();
        Counters.ResetSleeping();
        Counters.SetQueueSize(0);
        switch (scanProduct->Outcome) {
            case NDataShard::EExportOutcome::Success:
                AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)
                ("component", "TUploaderActor")("reason", "successfully finished")("bytes_read", scanProduct->BytesRead)(
                    "rows_read", scanProduct->RowsRead)("total_batches_received", TotalBatchesReceived)("total_rows_fed", TotalRowsFed)(
                    "total_intermediate_results", TotalIntermediateResults);
                Send(SubscriberActorId, new TEvPrivate::TEvBackupExportRecordBatchResult(true));
                break;
            case NDataShard::EExportOutcome::Error:
                Counters.OnError();
                Send(SubscriberActorId, new TEvPrivate::TEvBackupExportError(scanProduct->Error));
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)
                ("component", "TUploaderActor")("reason", "error")("error", scanProduct->Error)("bytes_read", scanProduct->BytesRead)(
                    "rows_read", scanProduct->RowsRead);
                break;
            case NDataShard::EExportOutcome::Aborted:
                Counters.OnError();
                Send(SubscriberActorId, new TEvPrivate::TEvBackupExportError(scanProduct->Error));
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)
                ("component", "TUploaderActor")("reason", "aborted")("error", scanProduct->Error)("bytes_read", scanProduct->BytesRead)(
                    "rows_read", scanProduct->RowsRead);
                break;
        }
        PassAway();
    }

    void DrainQueue() {
        if (LastState == NTable::EScan::Sleep) {
            return;
        }

        while (true) {
            if (DataQueue.empty() && !CurrentBatch.HasMoreRows()) {
                return;
            }
            if (!CurrentBatch.HasMoreRows()) {
                auto batch = DataQueue.front();
                DataQueue.pop();
                Counters.SetQueueSize(DataQueue.size());
                SendIntermediateResult();

                if (!batch.Data) {
                    CurrentBatch.Clear();
                    CurrentBatch.IsLast = batch.IsLast;
                    SendIntermediateResult();
                    continue;
                }

                auto result = NColumnShard::NBackup::BatchToRows(batch.Data, ColumnTypes);
                if (!result.IsSuccess()) {
                    Fail(result.GetErrorMessage());
                    return;
                }

                CurrentBatch.Clear();
                CurrentBatch.IsLast = batch.IsLast;
                CurrentBatch.Batch = result.DetachResult();
            }

            FeedRowsToExporter();
            if (LastState == NTable::EScan::Sleep) {
                return;
            }
        }
    }

    void FeedRowsToExporter() {
        ui64 rowsFedInBatch = 0;
        while (CurrentBatch.HasMoreRows()) {
            auto& row = CurrentBatch.GetRow();
            NTable::TRowState rowState(row.GetCells().size());
            int i = 0;
            for (const auto& cell : row.GetCells()) {
                rowState.Set(i++, { NTable::ECellOp::Set, NTable::ELargeObj::Inline }, cell);
            }
            auto result = Exporter->Feed({}, rowState);
            CurrentBatch.Position++;
            ++rowsFedInBatch;
            if (result == NTable::EScan::Sleep) {
                LastState = NTable::EScan::Sleep;
                Counters.OnSleep();
                TotalRowsFed += rowsFedInBatch;
                Counters.OnRowsFed(rowsFedInBatch);
                return;
            }
        }
        TotalRowsFed += rowsFedInBatch;
        Counters.OnRowsFed(rowsFedInBatch);
    }

    void Handle(const TEvPrivate::TEvBackupExportState::TPtr& ev) {
        const auto& event = *ev.Get()->Get();
        if (event.State == NTable::EScan::Final) {
            SendFinalResult();
            return;
        }
        if (LastState == NTable::EScan::Sleep && event.State != NTable::EScan::Sleep) {
            Counters.OnWakeup();
        }
        LastState = event.State;
        LastActivityTime = TInstant::Now();
        UploadData();
    }

    void MarkAsLast() {
        if (CurrentBatch.HasMoreRows()) {
            return;
        }
        if (!DataQueue.empty()) {
            return;
        }
        if (CurrentBatch.IsLast) {
            LastState = Exporter->Exhausted();
        }
    }

private:
    TIntrusiveConstPtr<NTable::TRowScheme> MakeRowSchema() {
        THashMap<ui32, NTable::TColumn> columns;
        for (const auto& column : TableColumns) {
            columns[column.first] = NTable::TColumn(column.second.Name, column.first, column.second.Type, column.second.TypeMod);
            columns[column.first].KeyOrder = column.first;
        }
        return NTable::TRowScheme::Make(columns, NUtil::TSecond());
    }

    TVector<std::pair<TString, NScheme::TTypeInfo>> MakeColumnTypes() {
        TVector<std::pair<TString, NScheme::TTypeInfo>> columnTypes;
        for (const auto& column : TableColumns) {
            columnTypes.emplace_back(column.second.Name, column.second.Type);
        }
        return columnTypes;
    }

private:
    TCurrentBatchItem CurrentBatch;
    NTable::EScan LastState = NTable::EScan::Sleep;
    std::queue<TBatchItem> DataQueue;
    std::unique_ptr<NColumnShard::NBackup::TExportDriver> Driver;
    NTable::IScan* Exporter = nullptr;
    NKikimrSchemeOp::TBackupTask BackupTask;
    const NDataShard::IExportFactory* ExportFactory;
    NDataShard::IExport::TTableColumns TableColumns;
    TActorId SubscriberActorId;
    ui64 TxId;
    TVector<std::pair<TString, NScheme::TTypeInfo>> ColumnTypes;

    TUploaderCounters Counters;
    TInstant LastActivityTime;
    ui64 TotalBatchesReceived = 0;
    ui64 TotalRowsFed = 0;
    ui64 TotalIntermediateResults = 0;

    // In-memory S3 multipart upload state (replaces DataShard's persistent storage for ColumnShard case)
    THashMap<ui64, NDataShard::TS3Upload> S3Uploads;
};

std::unique_ptr<IActor> CreateExportUploaderActor(const TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& backupTask,
    const NDataShard::IExportFactory* exportFactory, const NDataShard::IExport::TTableColumns& tableColumns, ui64 txId) {
    return std::make_unique<TUploaderActor>(backupTask, exportFactory, tableColumns, subscriberActorId, txId);
}

}   // namespace NKikimr::NColumnShard::NBackup
