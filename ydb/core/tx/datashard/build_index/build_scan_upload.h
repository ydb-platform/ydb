#pragma once

#include "common_helper.h"
#include "../range_ops.h"
#include "../buffer_data.h"
#include <ydb/core/base/appdata.h>
#include <ydb/core/tablet_flat/flat_row_state.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

namespace NKikimr::NDataShard {

template <NKikimrServices::TActivity::EType Activity>
class TBuildScanUpload: public TActor<TBuildScanUpload<Activity>>, public IActorExceptionHandler, public NTable::IScan {
    using TThis = TBuildScanUpload<Activity>;
    using TBase = TActor<TThis>;

protected:
    const TIndexBuildScanSettings ScanSettings;

    const ui64 BuildIndexId;
    const TString DatabaseName;
    const TString TargetTable;
    const TScanRecord::TSeqNo SeqNo;

    const ui64 DataShardId;
    const TActorId ProgressActorId;

    TTags ScanTags;                                             // first: columns we scan, order as in IndexTable
    std::shared_ptr<NTxProxy::TUploadTypes> UploadColumnsTypes; // columns types we upload to indexTable
    NTxProxy::EUploadRowsMode UploadMode;

    const TTags KeyColumnIds;
    const TVector<NScheme::TTypeInfo> KeyTypes;

    const TSerializedTableRange TableRange;
    const TSerializedTableRange RequestedRange;

    IDriver* Driver = nullptr;

    TBufferData ReadBuf;
    TBufferData WriteBuf;
    TSerializedCellVec LastUploadedKey;

    TActorId Uploader;
    ui32 RetryCount = 0;

    TUploadMonStats Stats = TUploadMonStats("tablets", "build_index_upload");
    TUploadStatus UploadStatus;

    const bool DisableChangeCollection;
    TMaybe<NKikimrTxDataShard::TGeneratedColumnFillContext> GeneratedFillContext;

    TBuildScanUpload(ui64 buildIndexId,
                     const TString& databaseName,
                     const TString& target,
                     const TScanRecord::TSeqNo& seqNo,
                     ui64 dataShardId,
                     const TActorId& progressActorId,
                     const TSerializedTableRange& range,
                     const TUserTable& tableInfo,
                     const TIndexBuildScanSettings& scanSettings,
                     bool disableChangeCollection)
        : TBase(&TThis::StateWork)
        , ScanSettings(scanSettings)
        , BuildIndexId(buildIndexId)
        , DatabaseName(databaseName)
        , TargetTable(target)
        , SeqNo(seqNo)
        , DataShardId(dataShardId)
        , ProgressActorId(progressActorId)
        , KeyColumnIds(tableInfo.KeyColumnIds)
        , KeyTypes(tableInfo.KeyColumnTypes)
        , TableRange(tableInfo.Range)
        , RequestedRange(range)
        , DisableChangeCollection(disableChangeCollection) {
    }

    template <typename TAddRow>
    EScan FeedImpl([[maybe_unused]] TArrayRef<const TCell> key, const TRow& /*row*/, TAddRow&& addRow) {
        addRow();

        if (!ReadBuf.HasReachedLimits(ScanSettings)) {
            return EScan::Feed;
        }

        if (!WriteBuf.IsEmpty()) {
            return EScan::Sleep;
        }

        ReadBuf.FlushTo(WriteBuf);

        Upload();

        return EScan::Feed;
    }

public:
    static constexpr auto ActorActivityType() {
        return Activity;
    }

    ~TBuildScanUpload() override = default;

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) override {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);

        YDB_LOG_INFO("Scan actor prepared",
            {"debug", Debug()});

        Driver = driver;

        return {EScan::Feed, {}};
    }

    EScan Seek(TLead& lead, ui64 seq) override {
        YDB_LOG_TRACE("Seek",
            {"seekSequence", seq},
            {"debug", Debug()});
        if (seq) {
            if (!WriteBuf.IsEmpty()) {
                return EScan::Sleep;
            }

            if (!ReadBuf.IsEmpty()) {
                ReadBuf.FlushTo(WriteBuf);
                Upload();
                return EScan::Sleep;
            }

            if (UploadStatus.IsNone()) {
                UploadStatus.StatusCode = Ydb::StatusIds::SUCCESS;
                UploadStatus.Issues.AddIssue(NYql::TIssue("Shard or requested range is empty"));
            }

            return EScan::Final;
        }

        auto scanRange = Intersect(KeyTypes, RequestedRange.ToTableRange(), TableRange.ToTableRange());

        if (scanRange.From) {
            auto seek = scanRange.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper;
            lead.To(ScanTags, scanRange.From, seek);
        } else {
            lead.To(ScanTags, {}, NTable::ESeek::Lower);
        }

        if (scanRange.To) {
            lead.Until(scanRange.To, scanRange.InclusiveTo);
        }

        return EScan::Feed;
    }

    TAutoPtr<IDestructable> Finish(const std::exception& exc) final
    {
        UploadStatus.Issues.AddIssue(NYql::TIssue(TStringBuilder()
            << "Scan failed " << exc.what()));
        return Finish(EStatus::Exception);
    }

    TAutoPtr<IDestructable> Finish(EStatus status) override {
        if (Uploader) {
            this->Send(Uploader, new TEvents::TEvPoisonPill);
            Uploader = {};
        }

        TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress = new TEvDataShard::TEvBuildIndexProgressResponse;
        FillScanResponseCommonFields(*progress, BuildIndexId, DataShardId, SeqNo);

        if (status == EStatus::Exception) {
            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        } else if (status != EStatus::Done) {
            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
        } else if (!UploadStatus.IsSuccess()) {
            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        } else {
            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
        }

        UploadStatusToMessage(progress->Record);

        if (progress->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            YDB_LOG_NOTICE("Scan completed successfully",
                {"debug", Debug()},
                {"progressRecord", progress->Record.ShortDebugString()});
        } else {
            YDB_LOG_ERROR("Scan failed",
                {"debug", Debug()},
                {"progressRecord", progress->Record.ShortDebugString()});
        }
        this->Send(ProgressActorId, progress.Release());

        Driver = nullptr;
        this->PassAway();
        return nullptr;
    }

    bool OnUnhandledException(const std::exception& exc) override {
        if (!Driver) {
            return false;
        }
        Driver->Throw(exc);
        return true;
    }

    void UploadStatusToMessage(NKikimrTxDataShard::TEvBuildIndexProgressResponse& msg) {
        msg.SetUploadStatus(UploadStatus.StatusCode);
        NYql::IssuesToMessage(UploadStatus.Issues, msg.MutableIssues());
    }

    void Describe(IOutputStream& out) const override {
        out << Debug();
    }

    TString Debug() const {
        return TStringBuilder() << "TBuildIndexScan TabletId: " << DataShardId << " Id: " << BuildIndexId
            << ", requested range: " << DebugPrintRange(KeyTypes, RequestedRange.ToTableRange(), *AppData()->TypeRegistry)
            << ", last acked point: " << DebugPrintPoint(KeyTypes, LastUploadedKey.GetCells(), *AppData()->TypeRegistry)
            << " " << Stats.ToString()
            << " " << UploadStatus.ToString();
    }

    EScan PageFault() override {
        YDB_LOG_TRACE("Page fault, flushing read buffer to upload buffer",
            {"readBufferEmpty", ReadBuf.IsEmpty()},
            {"writeBufferEmpty", WriteBuf.IsEmpty()},
            {"debug", Debug()});

        if (!ReadBuf.IsEmpty() && WriteBuf.IsEmpty()) {
            ReadBuf.FlushTo(WriteBuf);
            Upload();
        }

        return EScan::Feed;
    }

protected:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                YDB_LOG_ERROR("Unexpected event in scan actor",
                    {"eventType", ev->GetTypeRewrite()},
                    {"eventDetails", ev->ToString()});
        }
    }

    virtual void HandleWakeup(const NActors::TActorContext& /*ctx*/) {
        YDB_LOG_DEBUG("Retrying row upload",
            {"debug", Debug()});

        if (!WriteBuf.IsEmpty()) {
            RetryUpload();
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx) {
        YDB_LOG_DEBUG("Received row upload response",
            {"debug", Debug()},
            {"uploaderActorId", Uploader},
            {"senderActorId", ev->Sender});

        if (Uploader) {
            Y_ENSURE(Uploader == ev->Sender,
                       "Mismatch"
                           << " Uploader: " << Uploader.ToString()
                           << " ev->Sender: " << ev->Sender.ToString());
            // The uploader has replied and is about to die, so a new one may be
            // registered for the next batch.
            Uploader = {};
        } else {
            Y_ENSURE(Driver == nullptr);
            return;
        }

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues.AddIssues(ev->Get()->Issues);

        if (UploadStatus.IsSuccess()) {
            Stats.Aggr(WriteBuf.GetRows(), WriteBuf.GetRowCellBytes());
            LastUploadedKey = ExtractAcknowledgedKey();

            //send progress
            TAutoPtr<TEvDataShard::TEvBuildIndexProgressResponse> progress = new TEvDataShard::TEvBuildIndexProgressResponse;
            FillScanResponseCommonFields(*progress, BuildIndexId, DataShardId, SeqNo);

            // TODO(mbkkt) ReleaseBuffer isn't possible, we use LastUploadedKey for logging
            progress->Record.SetLastKeyAck(LastUploadedKey.GetBuffer());
            progress->Record.SetRowsDelta(WriteBuf.GetRows());
            // TODO: use GetRowCellBytes method?
            progress->Record.SetBytesDelta(WriteBuf.GetBufferBytes());
            WriteBuf.Clear();
            const bool resumeScan = OnUploadSuccess();

            progress->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);
            UploadStatusToMessage(progress->Record);

            this->Send(ProgressActorId, progress.Release());

            if (ReadBuf.HasReachedLimits(ScanSettings)) {
                ReadBuf.FlushTo(WriteBuf);
                Upload();
            }

            if (resumeScan) {
                Driver->Touch(EScan::Feed);
            }
            return;
        }

        if (RetryCount < ScanSettings.GetMaxBatchRetries() && UploadStatus.IsRetriable()) {
            YDB_LOG_NOTICE("Row upload failed with retriable error",
                {"debug", Debug()});

            ctx.Schedule(GetRetryWakeupTimeoutBackoff(RetryCount), new TEvents::TEvWakeup());
            return;
        }

        YDB_LOG_NOTICE("Row upload failed, aborting scan",
            {"debug", Debug()});

        Driver->Touch(EScan::Final);
    }

    virtual TSerializedCellVec ExtractAcknowledgedKey() {
        return WriteBuf.ExtractLastKey();
    }

    virtual bool OnUploadSuccess() { return true; }

    void RetryUpload() {
        Upload(true);
    }

    void Upload(bool isRetry = false) {
        if (Uploader) {
            // Only one upload may be in flight: WriteBuf is not cleared until the
            // current uploader replies, and a late reply from a superseded uploader
            // would trip the sender check in Handle().
            YDB_LOG_DEBUG("Upload already in flight, skipping",
                {"uploaderActorId", Uploader},
                {"isRetry", isRetry},
                {"debug", Debug()});
            return;
        }

        if (isRetry) {
            ++RetryCount;
        } else {
            RetryCount = 0;
        }

        YDB_LOG_DEBUG("Uploading row batch",
            {"lastKey", DebugPrintPoint(KeyTypes, WriteBuf.GetLastKey().GetCells(), *AppData()->TypeRegistry)},
            {"debug", Debug()});

        auto actor = NTxProxy::CreateUploadRowsInternal(
            this->SelfId(),
            DatabaseName,
            TargetTable,
            UploadColumnsTypes,
            WriteBuf.GetRowsData(),
            UploadMode,
            true /*writeToPrivateTable*/,
            true /*writeToIndexImplTable*/,
            DisableChangeCollection,
            0, TBackoff(0),
            GeneratedFillContext.Get());

        Uploader = this->Register(actor, TMailboxType::HTSwap, AppData()->BatchPoolId);
    }
};


} // namespace NKikimr::NDataShard

#undef YDB_LOG_THIS_FILE_COMPONENT
