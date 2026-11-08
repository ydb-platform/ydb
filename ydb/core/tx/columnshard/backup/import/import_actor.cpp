#include "import_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/backup/async_jobs/import_downloader.h>
#include <ydb/core/tx/columnshard/backup/manifest/manifest_actor.h>
#include <ydb/core/tx/columnshard/bg_tasks/events/events.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/data_events/payload_helper.h>
#include <ydb/core/tx/datashard/datashard_user_table.h>
#include <ydb/core/tx/datashard/import_common.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NImport {

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

void TImportActor::ScheduleTimeoutCheck() {
    Schedule(TimeoutCheckInterval, new NActors::TEvents::TEvWakeup());
}

void TImportActor::Handle(NActors::TEvents::TEvWakeup::TPtr& ev) {
    if (ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished() || Stage == EStage::Finished ||
        Stage == EStage::WaitSaveCursor) {
        return;
    }
    if (ev->Get()->Tag == WriteRetryTag) {
        if (Stage == EStage::WaitWriting) {
            SendWriteRequest();
        }
        return;
    }
    const auto elapsed = TInstant::Now() - StageStartTime;
    const auto operationTimeout = TDuration::Seconds(AppDataVerified().ColumnShardConfig.GetBackupSettings().GetImportOperationTimeoutSeconds());
    if (elapsed > operationTimeout) {
        const TString errorMessage = TStringBuilder() << "Import operation timed out after " << elapsed.Seconds() << "s"
                                                      << " in stage " << ::ToString(Stage) << ", tablet " << TabletId << ", importActorId "
                                                      << (ImportActorId ? ImportActorId.ToString() : "none");
        YDB_LOG_ERROR("",
            {"event", "import_timeout"},
            {"stage", ::ToString(Stage)},
            {"elapsedSec", elapsed.Seconds()},
            {"tabletId", TabletId},
            {"importActorId", ImportActorId ? ImportActorId.ToString() : "none"});
        Counters.OnError();
        AbortImport(errorMessage);
        return;
    }
    if (elapsed > WarningInterval) {
        YDB_LOG_WARN("",
            {"event", "import_slow_operation"},
            {"stage", ::ToString(Stage)},
            {"elapsedSec", elapsed.Seconds()},
            {"tabletId", TabletId},
            {"importActorId", ImportActorId ? ImportActorId.ToString() : "none"});
    }
    ScheduleTimeoutCheck();
}

void TImportActor::AbortImport(const TString& errorMessage) {
    if (ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished()) {
        YDB_LOG_WARN("",
            {"event", "abort_after_import_finished"},
            {"message", errorMessage});
        return;
    }
    if (Stage == EStage::WaitSaveCursor) {
        YDB_LOG_WARN("",
            {"event", "abort_during_save_cursor"},
            {"message", errorMessage});
        return;
    }
    ImportSession->Abort(errorMessage);
    PendingBlobs.clear();
    PendingPayloadSchema.clear();
    WriteRetryState.reset();
    PendingDownload.reset();
    StartNextFilePending = false;
    KillManifestReader();
    if (ImportActorId) {
        Send(ImportActorId, new NActors::TEvents::TEvPoisonPill());
        ImportActorId = {};
    }
    Counters.OnOperationsCancelled();
    Stage = EStage::WaitSaveCursor;
    StageStartTime = TInstant::Now();
    Counters.OnSaveProgressStarted();
    SaveProgressStartTime = TInstant::Now();
    SaveSessionProgress();
}

void TImportActor::KillManifestReader() {
    if (ManifestReader) {
        Send(ManifestReader, new NActors::TEvents::TEvPoisonPill());
        ManifestReader = {};
    }
}

void TImportActor::PassAway() {
    KillManifestReader();
    if (ImportActorId) {
        Send(ImportActorId, new NActors::TEvents::TEvPoisonPill());
        ImportActorId = {};
    }
    TBase::PassAway();
}

void TImportActor::OnSessionStateSaved() {
    AFL_VERIFY(ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished());
    NYDBTest::TControllers::GetColumnShardController()->OnImportFinished();
    if (ImportSession->GetTxId()) {
        ExecuteTransaction(std::make_unique<TTxProposeFinish>(
            GetShardVerified<NColumnShard::TColumnShard>(), *ImportSession->GetTxId(), SelfId(), GetNextTxId()));
    } else {
        Session->FinishActor();
    }
}

void TImportActor::Handle(NColumnShard::TEvPrivate::TEvBackupImportRecordBatch::TPtr& ev) {
    if (ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished()) {
        YDB_LOG_WARN("",
            {"event", "import_batch_after_terminal"},
            {"stage", ::ToString(Stage)},
            {"isFinished", ImportSession->IsFinished()},
            {"isAborted", ImportSession->IsReadyForRemoveOnFinished()});
        return;
    }
    Counters.OnReadFinished(TInstant::Now() - ReadStartTime);

    if (ev->Get()->Error) {
        Counters.OnError();
        ImportSession->Abort(ev->Get()->Error);
    } else if (ev->Get()->IsLast) {
        auto& progress = ImportSession->MutableProgress();
        if (progress.FilesCount && progress.FileIdx + 1 < progress.FilesCount) {
            YDB_LOG_INFO("",
                {"event", "import_data_file_completed"},
                {"tabletId", TabletId},
                {"fileIdx", progress.FileIdx},
                {"filesCount", progress.FilesCount});
            ++progress.FileIdx;
            progress.Download.reset();
            StartNextFilePending = true;
            // The downloader of the completed file stops by itself
            ImportActorId = {};
        } else if (ImportSession->IsStarted()) {
            ImportSession->Finish();
        }
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
    TString splitError;
    if (blobsSplittedConclusion.IsFail()) {
        splitError = blobsSplittedConclusion.GetErrorMessage();
    } else if (blobsSplittedConclusion.GetResult().empty()) {
        splitError = "import batch split produced no blobs";
    }
    if (splitError) {
        Counters.OnError();
        ImportSession->Abort(splitError);
        Counters.OnSaveProgressStarted();
        SaveProgressStartTime = TInstant::Now();
        SaveSessionProgress();
        return;
    }

    AFL_VERIFY(PendingBlobs.empty());
    PendingDownload = ev->Get()->Download;
    PendingPayloadSchema = NArrow::SerializeSchema(*ev->Get()->Data->schema());
    for (const auto& blob : blobsSplittedConclusion.GetResult()) {
        PendingBlobs.push_back(blob.GetData());
    }
    SendNextBlob();
    SwitchStage(EStage::WaitData, EStage::WaitWriting);
}

void TImportActor::SendNextBlob() {
    static const auto retryPolicy = IRetryPolicy<>::GetExponentialBackoffPolicy([]() {
        return ERetryErrorClass::LongRetry;
    }, TDuration::Seconds(1), TDuration::Seconds(1), TDuration::Seconds(30));
    WriteRetryState = retryPolicy->CreateRetryState();
    Counters.OnWriteStarted();
    WriteStartTime = TInstant::Now();
    SendWriteRequest();
}

void TImportActor::SendWriteRequest() {
    AFL_VERIFY(!PendingBlobs.empty());
    AFL_VERIFY(PendingPayloadSchema);
    auto writeEvent = MakeHolder<NEvents::TDataEvents::TEvWrite>(NKikimrDataEvents::TEvWrite::MODE_IMMEDIATE);
    NKikimr::NEvWrite::TPayloadWriter<NEvents::TDataEvents::TEvWrite> writer(*writeEvent);
    writer.AddDataToPayload(TString(PendingBlobs.front()));

    auto* operation = writeEvent->Record.AddOperations();
    operation->SetPayloadSchema(PendingPayloadSchema);
    operation->SetType(NKikimrDataEvents::TEvWrite::TOperation::OPERATION_REPLACE);
    operation->SetPayloadFormat(NKikimrDataEvents::FORMAT_ARROW);
    operation->SetPayloadIndex(0);
    operation->MutableTableId()->SetTableId(ImportSession->GetTask().GetRestoreTask().GetTableId());
    operation->MutableTableId()->SetSchemaVersion(ImportSession->GetTask().GetSchemaVersion().value_or(0));
    operation->SetIsBulk(true);
    Send(TabletActorId, writeEvent.Release());
}

void TImportActor::Handle(NEvents::TDataEvents::TEvWriteResult::TPtr& ev) {
    if (ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished()) {
        YDB_LOG_WARN("",
            {"event", "import_write_result_after_terminal"},
            {"stage", ::ToString(Stage)},
            {"isFinished", ImportSession->IsFinished()},
            {"isAborted", ImportSession->IsReadyForRemoveOnFinished()});
        return;
    }
    if (!ev->Get()->IsComplete()) {
        Counters.OnError();
        if (ev->Get()->GetStatus() == NKikimrDataEvents::TEvWriteResult::STATUS_OVERLOADED) {
            // Keep the cursor and the write timeout unchanged until this blob is written.
            if (const auto delay = WriteRetryState->GetNextRetryDelay()) {
                Schedule(*delay, new NActors::TEvents::TEvWakeup(WriteRetryTag));
                return;
            }
        }
        AbortImport(ev->Get()->GetError());
        return;
    }
    Counters.OnWriteFinished(TInstant::Now() - WriteStartTime);
    AFL_VERIFY(!PendingBlobs.empty());
    PendingBlobs.pop_front();
    WriteRetryState.reset();
    if (!PendingBlobs.empty()) {
        SendNextBlob();
        StageStartTime = TInstant::Now();
        return;
    }
    PendingPayloadSchema.clear();
    if (PendingDownload) {
        ImportSession->MutableProgress().Download = std::move(*std::exchange(PendingDownload, std::nullopt));
    }
    SwitchStage(EStage::WaitWriting, EStage::WaitData);
    Counters.OnSaveProgressStarted();
    SaveProgressStartTime = TInstant::Now();
    SaveSessionProgress();
}

void TImportActor::SwitchStage(const EStage from, const EStage to) {
    AFL_VERIFY(Stage == from)("from", from)("real", Stage)("to", to);
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

    Become(&TImportActor::StateFunc);
    const auto& restoreTask = ImportSession->GetTask().GetRestoreTask();
    if (!ImportSession->GetProgress().ManifestLoaded && restoreTask.HasS3Settings()) {
        SwitchStage(EStage::Initialization, EStage::WaitManifest);
        ManifestReader = Register(NKikimr::NColumnShard::NBackup::CreateDataManifestReader(SelfId(), restoreTask).release());
        return;
    }
    StartDownloader();
}

void TImportActor::Handle(NColumnShard::TEvPrivate::TEvBackupManifestReadResult::TPtr& ev) {
    if (ev->Sender != ManifestReader || Stage != EStage::WaitManifest) {
        return;
    }
    ManifestReader = {};
    if (ev->Get()->Error) {
        Counters.OnError();
        AbortImport("Data manifest download failed: " + ev->Get()->Error);
        return;
    }
    auto& progress = ImportSession->MutableProgress();
    progress.ManifestLoaded = true;
    progress.FilesCount = ev->Get()->FilesCount;
    progress.FileIdx = 0;
    progress.Download.reset();
    YDB_LOG_INFO("",
        {"event", "import_data_manifest_loaded"},
        {"tabletId", TabletId},
        {"filesCount", progress.FilesCount});
    SwitchStage(EStage::WaitManifest, EStage::Initialization);
    StartDownloader();
}

void TImportActor::StartDownloader() {
    AFL_VERIFY(!ImportActorId);
    auto restoreTask = ImportSession->GetTask().GetRestoreTask();
    const auto& progress = ImportSession->GetProgress();
    if (progress.FilesCount) {
        AFL_VERIFY(progress.FileIdx < progress.FilesCount)("file_idx", progress.FileIdx)("files_count", progress.FilesCount);
        NKikimr::NColumnShard::NBackup::SetDataFile(restoreTask, progress.FileIdx);
    }
    auto userTable = MakeIntrusiveConst<NDataShard::TUserTable>(ui32(0), restoreTask.GetTableDescription(), ui32(0));
    auto importActor = NKikimr::NColumnShard::NBackup::CreateImportDownloader(SelfId(), static_cast<ui64>(TabletId), 0, restoreTask,
        NKikimr::NDataShard::TTableInfo{ 0, userTable }, ImportSession->GetTask().GetColumns(), progress.Download);
    ImportActorId = Register(importActor.release());
    Stage = EStage::WaitData;
    StageStartTime = TInstant::Now();
    Counters.OnReadStarted();
    ReadStartTime = TInstant::Now();
}

TImportActor::TImportActor(std::shared_ptr<NBackground::TSession> bgSession, const std::shared_ptr<NBackground::ITabletAdapter>& adapter)
    : TBase(bgSession, adapter)
{
    ImportSession = bgSession->GetLogicAsVerifiedPtr<NImport::TSession>();
}

void TImportActor::OnSessionProgressSaved() {
    Counters.OnSaveProgressFinished(TInstant::Now() - SaveProgressStartTime);
    if (ImportSession->IsFinished() || ImportSession->IsReadyForRemoveOnFinished()) {
        AFL_VERIFY(Stage == EStage::WaitData || Stage == EStage::WaitSaveCursor)("real", Stage);
        Stage = EStage::Finished;
        StageStartTime = TInstant::Now();
        SaveSessionState();
    } else if (StartNextFilePending) {
        AFL_VERIFY(Stage == EStage::WaitData)("real", Stage);
        StartNextFilePending = false;
        StartDownloader();
    } else {
        AFL_VERIFY(Stage == EStage::WaitData)("real", Stage);
        StageStartTime = TInstant::Now();
        AFL_VERIFY(ImportActorId);
        Counters.OnReadStarted();
        ReadStartTime = TInstant::Now();
        TBase::Send(ImportActorId, new NColumnShard::TEvPrivate::TEvBackupImportRecordBatchResult());
    }
}

}   // namespace NKikimr::NOlap::NImport
