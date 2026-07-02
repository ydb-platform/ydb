#include "import_downloader.h"
#include "import_downloader_counters.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/import_common.h>
#include <ydb/core/tx/datashard/import_s3.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <contrib/libs/protobuf/src/google/protobuf/util/message_differencer.h>

namespace NKikimr::NColumnShard::NBackup {

TConclusion<std::unique_ptr<NActors::IActor>> CreateAsyncJobImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId,
    const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo) {
    const auto settingsKind = restoreTask.GetSettingsCase();
    switch (settingsKind) {
        case NKikimrSchemeOp::TRestoreTask::kS3Settings:
#ifndef KIKIMR_DISABLE_S3_OPS
            return std::unique_ptr<NActors::IActor>(CreateS3Downloader(subscriberActorId, txId, restoreTask, tableInfo));
#else
            return TConclusionStatus::Fail("Import from S3 are disabled");
#endif
        default:
            return TConclusionStatus::Fail(TStringBuilder() << "Unknown settings: " << static_cast<ui32>(settingsKind));
    }
}

class TImportDownloader: public TActorBootstrapped<TImportDownloader> {
public:
    TImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask,
        const NKikimr::NDataShard::TTableInfo& tableInfo, const THashMap<ui32, std::pair<TString, NScheme::TTypeInfo>>& ydbSchema)
        : SubscriberActorId(subscriberActorId)
        , TxId(txId)
        , RestoreTask(restoreTask)
        , TableInfo(tableInfo)
        , YdbSchema(ydbSchema)
    {
    }

    void Bootstrap() {
        Counters.OnActorAlive();
        auto result = CreateAsyncJobImportDownloader(SelfId(), TxId, RestoreTask, TableInfo);
        if (!result) {
            Counters.OnError();
            return Fail(result.GetError().GetErrorMessage());
        }
        Register(result.DetachResult().release());
        Become(&TThis::StateMain);
    }

    STRICT_STFUNC(StateMain,
        hFunc(NKikimr::TEvDataShard::TEvGetS3DownloadInfo, Handle) hFunc(NKikimr::TEvDataShard::TEvStoreS3DownloadInfo, Handle)
            hFunc(NKikimr::TEvDataShard::TEvS3UploadRowsRequest, Handle) hFunc(NKikimr::TEvDataShard::TEvAsyncJobComplete, Handle)
                hFunc(TEvPrivate::TEvBackupImportRecordBatchResult, Handle))

    void Handle(TEvPrivate::TEvBackupImportRecordBatchResult::TPtr&) {
        auto response = std::make_unique<NKikimr::TEvDataShard::TEvS3UploadRowsResponse>();
        response->Info = LastInfo;
        Send(LastActorId, std::move(response));
    }

    void Handle(NKikimr::TEvDataShard::TEvGetS3DownloadInfo::TPtr& ev) {
        Send(ev->Sender, std::make_unique<NKikimr::TEvDataShard::TEvS3DownloadInfo>());
    }

    void Handle(NKikimr::TEvDataShard::TEvStoreS3DownloadInfo::TPtr& ev) {
        AFL_VERIFY(google::protobuf::util::MessageDifferencer::Equals(ev->Get()->Info.DownloadState, NKikimrBackup::TS3DownloadState()));
        Send(ev->Sender, std::make_unique<NKikimr::TEvDataShard::TEvS3DownloadInfo>(ev->Get()->Info));
    }

    void Handle(NKikimr::TEvDataShard::TEvS3UploadRowsRequest::TPtr& ev) {
        const NActors::TLogContextGuard gLogging = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)(
            "event", "import_s3_upload_rows")("tx_id", TxId)("ydb_schema_size", YdbSchema.size());

        Counters.OnProcessStarted();
        const TInstant processStartTime = TInstant::Now();

        const auto& record = ev->Get()->Record;
        const auto& rowScheme = record.GetRowScheme();

        TVector<std::pair<TString, NScheme::TTypeInfo>> rowSchemaOrder;
        rowSchemaOrder.reserve(rowScheme.KeyColumnIdsSize() + rowScheme.ValueColumnIdsSize());

        for (ui32 i = 0; i < rowScheme.KeyColumnIdsSize(); ++i) {
            const ui32 colId = rowScheme.GetKeyColumnIds(i);
            auto it = YdbSchema.find(colId);
            AFL_VERIFY(it != YdbSchema.end());
            auto [name, typeInfo] = it->second;
            rowSchemaOrder.emplace_back(std::move(name), typeInfo);
        }
        for (ui32 i = 0; i < rowScheme.ValueColumnIdsSize(); ++i) {
            const ui32 colId = rowScheme.GetValueColumnIds(i);
            auto it = YdbSchema.find(colId);
            AFL_VERIFY(it != YdbSchema.end());
            auto [name, typeInfo] = it->second;
            rowSchemaOrder.emplace_back(std::move(name), typeInfo);
        }

        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "s3_upload_rows_schema")("key_columns", rowScheme.KeyColumnIdsSize())(
            "value_columns", rowScheme.ValueColumnIdsSize())("total_columns", rowSchemaOrder.size())("rows", record.RowsSize());

        TSerializedCellVec keyCells;
        TSerializedCellVec valueCells;

        NArrow::TArrowBatchBuilder batchBuilder;
        const auto startStatus = batchBuilder.Start(rowSchemaOrder);
        if (!startStatus.ok()) {
            Counters.OnProcessFinished(TInstant::Now() - processStartTime, 0, 0);
            Counters.OnError();
            return Fail(startStatus.ToString());
        }

        ui64 totalBytes = 0;
        const ui32 rowCount = record.RowsSize();

        TVector<TCell> allCells;
        for (const auto& r : record.GetRows()) {
            keyCells.Parse(r.GetKeyColumns());
            valueCells.Parse(r.GetValueColumns());

            allCells.reserve(rowSchemaOrder.size());
            for (const auto& cell : keyCells.GetCells()) {
                allCells.push_back(cell);
                totalBytes += cell.Size();
            }
            for (const auto& cell : valueCells.GetCells()) {
                allCells.push_back(cell);
                totalBytes += cell.Size();
            }

            AFL_VERIFY(allCells.size() == rowSchemaOrder.size())
            ("all_cells_size", allCells.size())("schema_size", rowSchemaOrder.size());

            batchBuilder.AddRow(TConstArrayRef<TCell>(allCells.data(), allCells.size()));
            allCells.clear();
        }

        auto resultBatch = batchBuilder.FlushBatch(false);
        Counters.OnProcessFinished(TInstant::Now() - processStartTime, rowCount, totalBytes);

        LastInfo = ev->Get()->Info;
        LastActorId = ev->Sender;
        Send(SubscriberActorId, std::make_unique<TEvPrivate::TEvBackupImportRecordBatch>(resultBatch, false));
    }

    void Handle(NKikimr::TEvDataShard::TEvAsyncJobComplete::TPtr& ev) {
        auto product = static_cast<NDataShard::TImportJobProduct*>(ev->Get()->Prod.Get());
        if (!product->Success) {
            Counters.OnError();
            return Fail(product->Error);
        }
        Send(SubscriberActorId, std::make_unique<TEvPrivate::TEvBackupImportRecordBatch>(nullptr, true));
        Counters.OnActorDead();
        PassAway();
    }

    void Fail(const TString& error) {
        auto result = std::make_unique<TEvPrivate::TEvBackupImportRecordBatch>(nullptr, true);
        result->Error = error;
        Send(SubscriberActorId, std::move(result));
        Counters.OnActorDead();
        PassAway();
    }

private:
    NDataShard::TS3Download LastInfo;
    TActorId LastActorId;
    NActors::TActorId SubscriberActorId;
    ui64 TxId;
    NKikimrSchemeOp::TRestoreTask RestoreTask;
    NKikimr::NDataShard::TTableInfo TableInfo;
    THashMap<ui32, std::pair<TString, NScheme::TTypeInfo>> YdbSchema;
    TImportDownloaderCounters Counters;
};

std::unique_ptr<NActors::IActor> CreateImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId,
    const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo,
    const THashMap<ui32, std::pair<TString, NScheme::TTypeInfo>>& ydbSchema) {
    return std::make_unique<TImportDownloader>(subscriberActorId, txId, restoreTask, tableInfo, ydbSchema);
}

}   // namespace NKikimr::NColumnShard::NBackup
