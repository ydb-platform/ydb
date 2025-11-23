#include "import_downloader.h"

#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/formats/arrow/arrow_batch_builder.h>

namespace NKikimr::NColumnShard::NBackup {
    
TConclusion<std::unique_ptr<NActors::IActor>> CreateAsyncJobImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo) {
    const auto settingsKind = restoreTask.GetSettingsCase();
    switch (settingsKind) {
    case NKikimrSchemeOp::TRestoreTask::kS3Settings:
    #ifndef KIKIMR_DISABLE_S3_OPS
        return std::unique_ptr<NActors::IActor>(CreateS3Downloader(subscriberActorId, txId, restoreTask, tableInfo));
    #else
        return TConclusionStatus::Fail("Exports to S3 are disabled");
    #endif
    default:
        return TConclusionStatus::Fail(TStringBuilder() << "Unknown settings: " << static_cast<ui32>(settingsKind));
    }
}

class TImportDownloader: public TActorBootstrapped<TImportDownloader> {
public:
    TImportDownloader(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo)
        : SubscriberActorId(subscriberActorId)
        , TxId(txId)
        , RestoreTask(restoreTask)
        , TableInfo(tableInfo) {
    }
    
    void Bootstrap() {
        Register(CreateAsyncJobImportDownloader(SelfId(), TxId, RestoreTask, TableInfo).DetachResult().release());
        Become(&TThis::StateMain);
    }
    
    STRICT_STFUNC(
        StateMain,
        hFunc(NKikimr::TEvDataShard::TEvGetS3DownloadInfo, Handle)
        hFunc(NKikimr::TEvDataShard::TEvStoreS3DownloadInfo, Handle)
        hFunc(NKikimr::TEvDataShard::TEvS3UploadRowsRequest, Handle)
        hFunc(NDataShard::TDataShard::TEvPrivate::TEvAsyncJobComplete, Handle)
    )
    
    void Handle(NKikimr::TEvDataShard::TEvGetS3DownloadInfo::TPtr& ev) {
        Cerr << "TEvGetS3DownloadInfo: " << ev->Get()->ToString() << Endl;
        Send(ev->Sender, new NKikimr::TEvDataShard::TEvS3DownloadInfo());
        Y_UNUSED(ev);
        
    }
    
    void Handle(NKikimr::TEvDataShard::TEvStoreS3DownloadInfo::TPtr& ev) {
        Send(ev->Sender, new NKikimr::TEvDataShard::TEvS3DownloadInfo(ev->Get()->Info));
        Y_UNUSED(ev);
    }
    
    TVector<std::pair<TString, NScheme::TTypeInfo>> MakeYdbSchema() {
        return {{"key", NScheme::TTypeInfo(NScheme::NTypeIds::String)}, {"value", NScheme::TTypeInfo(NScheme::NTypeIds::String)}};
    }

    void Handle(NKikimr::TEvDataShard::TEvS3UploadRowsRequest::TPtr& ev) {        
        TSerializedCellVec keyCells;
        TSerializedCellVec valueCells;
        
        NArrow::TArrowBatchBuilder batchBuilder;
        const auto startStatus = batchBuilder.Start(MakeYdbSchema());
        if (!startStatus.ok()) {
            /* TODO: error handling */
        }

        
        for (const auto& r : ev->Get()->Record.GetRows()) {
            // TODO: use safe parsing!
            keyCells.Parse(r.GetKeyColumns());
            valueCells.Parse(r.GetValueColumns());
            batchBuilder.AddRow(keyCells.GetCells(), valueCells.GetCells());
        }
        
        auto resultBatch = batchBuilder.FlushBatch(false);
    
        auto response = new NKikimr::TEvDataShard::TEvS3UploadRowsResponse();
        response->Info = ev->Get()->Info;
        Send(ev->Sender, response);
        Y_UNUSED(ev);
    }
    
    void Handle(NDataShard::TDataShard::TEvPrivate::TEvAsyncJobComplete::TPtr& ev) {
        Y_UNUSED(ev);
    }

private:
    NActors::TActorId SubscriberActorId;
    ui64 TxId;
    NKikimrSchemeOp::TRestoreTask RestoreTask;
    NKikimr::NDataShard::TTableInfo TableInfo;
};


std::unique_ptr<NActors::IActor> CreateImportDownloaderImport(const NActors::TActorId& subscriberActorId, ui64 txId, const NKikimrSchemeOp::TRestoreTask& restoreTask, const NKikimr::NDataShard::TTableInfo& tableInfo) {
    return std::make_unique<TImportDownloader>(subscriberActorId, txId, restoreTask, tableInfo);
}

}