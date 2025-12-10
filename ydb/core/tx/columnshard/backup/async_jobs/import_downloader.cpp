#include "import_downloader.h"

#include <ydb/core/formats/arrow/arrow_batch_builder.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/import_common.h>
#include <ydb/core/tx/datashard/import_s3.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/libs/protobuf/src/google/protobuf/util/message_differencer.h>

namespace NKikimr::NColumnShard::NBackup {
    
TConclusion<std::unique_ptr<NActors::IActor>> CreateAsyncJobImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo) {
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
    TImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo, const TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema)
        : SubscriberActorId(subscriberActorId)
        , TxId(txId)
        , RestoreTask(restoreTask)
        , TableInfo(tableInfo)
        , YdbSchema(ydbSchema) {
    }
    
    void Bootstrap() {
        auto result = CreateAsyncJobImportDownloader(SelfId(), TxId, RestoreTask, TableInfo);
        if (!result) {
            return Fail(result.GetError().GetErrorMessage());
        }
        Register(result.DetachResult().release());
        Become(&TThis::StateMain);
    }
    
    STRICT_STFUNC(
        StateMain,
        hFunc(NKikimr::TEvDataShard::TEvGetS3DownloadInfo, Handle)
        hFunc(NKikimr::TEvDataShard::TEvStoreS3DownloadInfo, Handle)
        hFunc(NKikimr::TEvDataShard::TEvS3UploadRowsRequest, Handle)
        hFunc(NKikimr::TEvDataShard::TEvAsyncJobComplete, Handle)
        hFunc(TEvPrivate::TEvBackupImportRecordBatchResult, Handle)
    )
    
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
        TSerializedCellVec keyCells;
        TSerializedCellVec valueCells;
        
        NArrow::TArrowBatchBuilder batchBuilder;
        const auto startStatus = batchBuilder.Start(YdbSchema);
        if (!startStatus.ok()) {
            return Fail(startStatus.ToString());
        }
        
        for (const auto& r : ev->Get()->Record.GetRows()) {
            keyCells.Parse(r.GetKeyColumns());
            valueCells.Parse(r.GetValueColumns());
            batchBuilder.AddRow(keyCells.GetCells(), valueCells.GetCells());
        }
        
        auto resultBatch = batchBuilder.FlushBatch(false);
        LastInfo = ev->Get()->Info;
        LastActorId = ev->Sender;
        Send(SubscriberActorId, std::make_unique<TEvPrivate::TEvBackupImportRecordBatch>(resultBatch, false));
    }
    
    void Handle(NKikimr::TEvDataShard::TEvAsyncJobComplete::TPtr&) {
        Send(SubscriberActorId, std::make_unique<TEvPrivate::TEvBackupImportRecordBatch>(nullptr, true));
        PassAway();
    }
    
    void Fail(const TString& error) {
        auto result = std::make_unique<TEvPrivate::TEvBackupImportRecordBatch>(nullptr, true);
        result->Error = error;
        Send(SubscriberActorId, std::move(result));
        PassAway();
    }

private:
    NDataShard::TS3Download LastInfo;
    TActorId LastActorId;
    NActors::TActorId SubscriberActorId;
    ui64 TxId;
    NKikimrSchemeOp::TRestoreTask RestoreTask;
    NKikimr::NDataShard::TTableInfo TableInfo;
    TVector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema;
};


std::unique_ptr<NActors::IActor> CreateImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo, const TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema) {
    return std::make_unique<TImportDownloader>(subscriberActorId, txId, restoreTask, tableInfo, ydbSchema);
}

}