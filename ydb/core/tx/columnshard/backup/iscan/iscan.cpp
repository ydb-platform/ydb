#include "iscan.h"

#include <ydb/core/formats/arrow/serializer/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>

namespace NKikimr::NColumnShard::NBackup {

TRowWriter::TRowWriter(TVector<TSerializedCellVec>& rows)
    : Rows(rows)
{}

void TRowWriter::AddRow(const TConstArrayRef<TCell>& cells) {
    TSerializedCellVec serializedKey(cells);
    Rows.emplace_back(std::move(serializedKey));
}

TConclusion<TVector<TSerializedCellVec>> BatchToRows(const std::shared_ptr<arrow::RecordBatch>& batch,
                                                     const TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema) {
    Y_ABORT_UNLESS(batch);
    TVector<TSerializedCellVec> cellVecs;
    cellVecs.reserve(batch->num_rows());
    TRowWriter writer(cellVecs);
    NArrow::TArrowToYdbConverter batchConverter(ydbSchema, writer, false);
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

TConclusion<std::unique_ptr<NTable::IScan>> CreateIScanExportUploader(const TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& backupTask, const NDataShard::IExportFactory* exportFactory, const NDataShard::IExport::TTableColumns& tableColumns, ui64 txId) {
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
        case NKikimrSchemeOp::TBackupTask::SETTINGS_NOT_SET:
            return TConclusionStatus::Fail("Internal error. It is not possible to have empty settings for backup here");
    }

    auto createUploader = [subscriberActorId = subscriberActorId, txId = txId, exp]() {
        return exp->CreateUploader(subscriberActorId, txId);
    };

    THolder<NKikimr::NDataShard::NExportScan::IBuffer> buffer{exp->CreateBuffer()};
    std::unique_ptr<NTable::IScan> scan{NDataShard::CreateExportScan(std::move(buffer), createUploader)};

    return scan;
}

TExportDriver::TExportDriver(TActorSystem* actorSystem, const TActorId& subscriberActorId)
    : ActorSystem(actorSystem)
    , SubscriberActorId(subscriberActorId) {
}

class TUploaderActor: public TActorBootstrapped<TUploaderActor> {
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

    TUploaderActor(const NKikimrSchemeOp::TBackupTask& backupTask, const NDataShard::IExportFactory* exportFactory, const NDataShard::IExport::TTableColumns& tableColumns, const TActorId& subscriberActorId, ui64 txId)
        : BackupTask(backupTask)
        , ExportFactory(exportFactory)
        , TableColumns(tableColumns)
        , SubscriberActorId(subscriberActorId)
        , TxId(txId)
        , ColumnTypes(MakeColumnTypes()) {
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
        Become(&TThis::StateMain);
    }

    void Fail(const TString& errorMessage) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("component", "TUploaderActor")("error", errorMessage);
        Send(SubscriberActorId, new TEvPrivate::TEvBackupExportError(errorMessage));
        Exporter->Finish(NTable::EStatus::Done);
        PassAway();
    }

    STRICT_STFUNC(
        StateMain,
        hFunc(TEvPrivate::TEvBackupExportError, Handle)
        hFunc(TEvPrivate::TEvBackupExportState, Handle)
        hFunc(TEvPrivate::TEvBackupExportRecordBatch, Handle)
    )

    void Handle(const TEvPrivate::TEvBackupExportRecordBatch::TPtr& ev) {
        const auto& event = *ev.Get()->Get();
        DataQueue.emplace(event.Data, event.IsLast);
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
        }
    }
    
    void SendFinalResult() {
        auto result = Exporter->Finish(NTable::EStatus::Done);
        auto* scanProduct = static_cast<NDataShard::TExportScanProduct*>(result.Get());
        switch (scanProduct->Outcome) {
            case NDataShard::EExportOutcome::Success:
                AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)
                    ("component", "TUploaderActor")
                    ("reason", "successfully finished")
                    ("bytes_read", scanProduct->BytesRead)
                    ("rows_read", scanProduct->RowsRead);
                Send(SubscriberActorId, new TEvPrivate::TEvBackupExportRecordBatchResult(true));
                break;
            case NDataShard::EExportOutcome::Error:
                Send(SubscriberActorId, new TEvPrivate::TEvBackupExportError(scanProduct->Error));
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)
                    ("component", "TUploaderActor")
                    ("reason", "error")
                    ("error", scanProduct->Error)
                    ("bytes_read", scanProduct->BytesRead)
                    ("rows_read", scanProduct->RowsRead);
                break;
            case NDataShard::EExportOutcome::Aborted:
                Send(SubscriberActorId, new TEvPrivate::TEvBackupExportError(scanProduct->Error));
                AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)
                    ("component", "TUploaderActor")
                    ("reason", "aborted")
                    ("error", scanProduct->Error)
                    ("bytes_read", scanProduct->BytesRead)
                    ("rows_read", scanProduct->RowsRead);
                break;
        }
        PassAway();
    }

    void DrainQueue() {
        if (LastState == NTable::EScan::Sleep) {
            return;
        }

        while (true) {
            if (DataQueue.empty() && !CurrentBatch.HasMoreRows() ) {
                return;
            }
            if (!CurrentBatch.HasMoreRows()) {
                auto batch = DataQueue.front();
                DataQueue.pop();
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
        while (CurrentBatch.HasMoreRows()) {
            auto& row = CurrentBatch.GetRow();
            NTable::TRowState rowState(row.GetCells().size());
            int i = 0;
            for (const auto& cell: row.GetCells()) {
                rowState.Set(i++, { NTable::ECellOp::Set, NTable::ELargeObj::Inline }, cell);
            }
            auto result = Exporter->Feed({}, rowState);
            CurrentBatch.Position++;
            if (result == NTable::EScan::Sleep) {
                LastState = NTable::EScan::Sleep;
                return;
            }
        }
    }

    void Handle(const TEvPrivate::TEvBackupExportState::TPtr& ev) {
        const auto& event = *ev.Get()->Get();
        if (event.State == NTable::EScan::Final) {
            SendFinalResult();
            return;
        }
        LastState = event.State;
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
};

std::unique_ptr<IActor> CreateExportUploaderActor(const TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& backupTask, const NDataShard::IExportFactory* exportFactory, const NDataShard::IExport::TTableColumns& tableColumns, ui64 txId) {
    return std::make_unique<TUploaderActor>(backupTask, exportFactory, tableColumns, subscriberActorId, txId);
}

} // namespace NKikimr::NColumnShard::NBackup