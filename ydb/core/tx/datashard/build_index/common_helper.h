#pragma once

#include <ydb/core/tx/datashard/buffer_data.h>
#include <ydb/core/tx/datashard/datashard_impl.h>
#include <ydb/core/tx/datashard/scan_common.h>
#include <ydb/core/tx/datashard/upload_stats.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NDataShard {
using namespace NTableIndex;

#define LOG_T(stream) LOG_TRACE_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_D(stream) LOG_DEBUG_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_I(stream) LOG_INFO_S  (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_W(stream) LOG_WARN_S  (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)
#define LOG_E(stream) LOG_ERROR_S (*TlsActivationContext, NKikimrServices::BUILD_INDEX, stream)

class TBatchRowsUploader
{
    struct TDestination {
        TBufferData Buffer;
        TString Table;
        std::shared_ptr<NTxProxy::TUploadTypes> Types;
        // Last source-table key whose entries for this destination have been
        // durably uploaded. Updated when an upload of this destination acks
        // successfully. Used by GetMinFlushedKey() to compute a cross-
        // destination safe checkpoint for the scan-level LastKeyAck.
        //
        // The accompanying FlushedSeqNo is a monotonic counter sampled at
        // the same moment FlushedKey is sampled. Since the scan reads
        // source rows in ascending key order, capture order == key order;
        // we therefore use SeqNo to compare keys cheaply and type-
        // independently across destinations.
        TSerializedCellVec FlushedKey;
        ui64 FlushedSeqNo = 0;
        // Snapshot of the scan's current source key captured at the moment
        // this destination's buffer was claimed for the in-flight upload.
        // Promoted to FlushedKey when that upload acks.
        TSerializedCellVec PendingFlushKey;
        ui64 PendingFlushSeqNo = 0;
        bool HasFlushedOnce = false;

        operator bool() const {
            return !Buffer.IsEmpty();
        }
    };

public:
    TBatchRowsUploader(const TString& database, const TIndexBuildScanSettings& scanSettings)
        : Database(database)
        , ScanSettings(scanSettings)
    {}

    TBufferData* AddDestination(TString table, std::shared_ptr<NTxProxy::TUploadTypes> types) {
        auto& dst = Destinations[table];
        dst.Table = std::move(table);
        dst.Types = std::move(types);
        return &dst.Buffer;
    }

    bool Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        Y_ENSURE(UploaderId == ev->Sender, "Mismatch"
            << " Uploader: " << UploaderId.ToString()
            << " Sender: " << ev->Sender.ToString());
        Y_ENSURE(Uploading);

        UploaderId = {};

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues = ev->Get()->Issues;
        if (!UploadStatus.IsSuccess()) {
            return false;
        }

        UploadRows += Uploading.Buffer.GetRows();
        UploadBytes += Uploading.Buffer.GetRowCellBytes();
        Uploading.Buffer.Clear();
        RetryCount = 0;

        if (auto it = Destinations.find(Uploading.Table); it != Destinations.end()) {
            it->second.FlushedKey = it->second.PendingFlushKey;
            it->second.FlushedSeqNo = it->second.PendingFlushSeqNo;
            it->second.HasFlushedOnce = true;
        }

        for (auto& [_, dst] : Destinations) {
            if (TryUpload(dst, true /* by limit */)) {
                break;
            }
        }

        return true;
    }

    bool ShouldWaitUpload()
    {
        bool hasReachedLimit = false;
        for (auto& [_, dst] : Destinations) {
            if (dst.Buffer.HasReachedLimits(ScanSettings)) {
                hasReachedLimit = true;
                break;
            }
        }
        if (!hasReachedLimit) {
            return false;
        }

        if (Uploading) {
            return true;
        }
        for (auto& [_, dst] : Destinations) {
            if (TryUpload(dst, true /* by limit */)) {
                break;
            }
        }

        hasReachedLimit = false;
        for (auto& [_, dst] : Destinations) {
            if (dst.Buffer.HasReachedLimits(ScanSettings)) {
                hasReachedLimit = true;
                break;
            }
        }
        return hasReachedLimit;
    }

    std::optional<TDuration> GetRetryAfter() const {
        if (RetryCount < ScanSettings.GetMaxBatchRetries() && UploadStatus.IsRetriable()) {
            return GetRetryWakeupTimeoutBackoff(RetryCount);
        }
        return {};
    }

    void RetryUpload()
    {
        if (!Uploading) {
            return;
        }

        ++RetryCount;
        StartUploadRowsInternal();
    }

    bool CanFinish() {
        if (Uploading) {
            return false;
        }

        for (auto& [_, dst] : Destinations) {
            if (TryUpload(dst, false /* not by limit */)) {
                return false;
            }
        }

        return true;
    }

    bool AllFlushed() const {
        if (Uploading) {
            return false;
        }
        for (const auto& [_, dst] : Destinations) {
            if (!dst.Buffer.IsEmpty()) {
                return false;
            }
        }
        return true;
    }

    ui64 GetUploadBytes() const {
        return UploadBytes;
    }

    void AddIssue(const std::exception& exc) {
        UploadStatus.Issues.AddIssue(NYql::TIssue(TStringBuilder()
            << "Scan failed " << exc.what()));
    }

    template<typename TResponse>
    void Finish(TResponse& response, NTable::EStatus status) {
        if (UploaderId) {
            TlsActivationContext->Send(new IEventHandle(UploaderId, TActorId(), new TEvents::TEvPoison));
            UploaderId = {};
        }

        response.MutableMeteringStats()->SetUploadRows(UploadRows);
        response.MutableMeteringStats()->SetUploadBytes(UploadBytes);

        if (status == NTable::EStatus::Exception) {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        } else if (status != NTable::EStatus::Done) {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
        } else if (UploadStatus.IsNone() || UploadStatus.IsSuccess()) {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
            if (UploadStatus.IsNone()) {
                UploadStatus.Issues.AddIssue(NYql::TIssue("Shard or requested range is empty"));
            }
        } else {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        }

        NYql::IssuesToMessage(UploadStatus.Issues, response.MutableIssues());
    }

    const TUploadStatus& GetUploadStatus() const {
        return UploadStatus;
    }

    bool IsSuccess() const {
        return UploadStatus.IsSuccess();
    }

    void SetOwner(const TActorId& owner) {
        Owner = owner;
    }

    // The scan must call this whenever it processes a new source row, before
    // (or right at) AddRow on any destination buffer. The uploader uses the
    // most recent value to snapshot a per-destination "PendingFlushKey" at
    // each TryUpload, which becomes the destination's FlushedKey on a
    // successful upload ack. See GetMinFlushedKey.
    void SetCurrentSourceKey(TSerializedCellVec key) {
        CurrentSourceKey = std::move(key);
    }

    // Returns the latest source-table key K such that, for every destination
    // buffer, all entries this destination would produce for source rows
    // <= K are durably persisted via the upload pipeline. Returns nullopt
    // while any destination has not yet completed a successful upload — in
    // that case there is no cross-destination safe checkpoint and the caller
    // must NOT advance LastKeyAck on the scan-level response.
    //
    // The min across destinations is the safe checkpoint because each
    // destination's FlushedKey reflects only the rows whose entries it has
    // already uploaded; rows past that key may still have entries buffered
    // and would be lost if the scan is resumed past that key.
    std::optional<TSerializedCellVec> GetMinFlushedKey() const {
        if (Destinations.empty()) {
            return std::nullopt;
        }
        const TDestination* minDst = nullptr;
        for (const auto& [_, dst] : Destinations) {
            if (!dst.HasFlushedOnce) {
                return std::nullopt;
            }
            if (!minDst || dst.FlushedSeqNo < minDst->FlushedSeqNo) {
                minDst = &dst;
            }
        }
        return minDst->FlushedKey;
    }

    TString Debug() const {
        TStringBuilder result;

        if (Uploading) {
            result << "UploadTable: " << Uploading.Table << " UploadBuf size: " << Uploading.Buffer.GetBufferBytes() << " RetryCount: " << RetryCount;
        }

        return result;
    }

private:
    bool TryUpload(TDestination& destination, bool byLimit) {
        if (Y_UNLIKELY(Uploading)) {
            // already uploading something
            return true;
        }

        if (!destination.Buffer.IsEmpty() && (!byLimit || destination.Buffer.HasReachedLimits(ScanSettings))) {
            Uploading.Table = destination.Table;
            Uploading.Types = destination.Types;
            destination.Buffer.FlushTo(Uploading.Buffer);
            // Capture the scan's current source key as the boundary that the
            // upload of this destination's batch covers — i.e. all entries
            // for source rows <= CurrentSourceKey produced by this scan and
            // belonging to this destination are now committed to the upload.
            destination.PendingFlushKey = CurrentSourceKey;
            destination.PendingFlushSeqNo = ++CurrentSourceSeqNo;
            StartUploadRowsInternal();
            return true;
        }

        return false;
    }

    void StartUploadRowsInternal() {
        YDB_LOG_DEBUG_COMP(NKikimrServices::BUILD_INDEX, "Starting batch row upload",
            {"debug", Debug()});

        Y_ENSURE(Uploading);
        Y_ENSURE(!Uploading.Buffer.IsEmpty());
        Y_ENSURE(!UploaderId);
        Y_ENSURE(Owner);
        auto actor = NTxProxy::CreateUploadRowsInternal(
            Owner, Database, Uploading.Table, Uploading.Types, Uploading.Buffer.GetRowsData(),
            NTxProxy::EUploadRowsMode::WriteToTableShadow,
            true /*writeToPrivateTable*/,
            true /*writeToIndexImplTable*/);

        UploaderId = TlsActivationContext->Register(actor, Owner, TMailboxType::HTSwap, AppData()->BatchPoolId);
    }

private:
    const TString Database;
    const TIndexBuildScanSettings ScanSettings;
    TActorId Owner;

    TMap<TString, TDestination> Destinations;
    TActorId UploaderId = {};
    TDestination Uploading;
    TUploadStatus UploadStatus = {};
    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;
    ui32 RetryCount = 0;

    // Updated by the scan via SetCurrentSourceKey before AddRow on any
    // destination. Snapshotted into per-destination PendingFlushKey at
    // each TryUpload.
    TSerializedCellVec CurrentSourceKey;
    ui64 CurrentSourceSeqNo = 0;
};

inline void StartScan(TDataShard* dataShard, TAutoPtr<NTable::IScan>&& scan, ui64 id,
    TScanRecord::TSeqNo seqNo, std::optional<TRowVersion> rowVersion, ui32 tableId)
{
    auto& scanManager = dataShard->GetScanManager();

    if (const auto* recCard = scanManager.Get(id)) {
        if (recCard->SeqNo == seqNo) {
            // do no start one more scan
            return;
        }

        for (auto scanId : recCard->ScanIds) {
            dataShard->CancelScan(tableId, scanId);
        }
        scanManager.Drop(id);
    }

    TScanOptions scanOpts;
    if (rowVersion) {
        scanOpts.SetSnapshotRowVersion(*rowVersion);
    }
    scanOpts.SetResourceBroker("build_index", 10);
    const auto scanId = dataShard->QueueScan(tableId, std::move(scan), 0, scanOpts);
    scanManager.Set(id, seqNo).push_back(scanId);
}

template<typename TResponse>
void FillScanResponseCommonFields(TResponse& response, ui64 scanId, ui64 tabletId, TScanRecord::TSeqNo seqNo)
{
    auto& rec = response.Record;
    rec.SetId(scanId);
    rec.SetTabletId(tabletId);
    rec.SetRequestSeqNoGeneration(seqNo.Generation);
    rec.SetRequestSeqNoRound(seqNo.Round);
}

template<typename TResponse>
inline void FailScan(ui64 scanId, ui64 tabletId, TActorId sender, TScanRecord::TSeqNo seqNo, const std::exception& exc, const TString& logScanType)
{
    YDB_LOG_ERROR_COMP(NKikimrServices::BUILD_INDEX, "Unhandled exception in build index scan",
        {"scanType", logScanType},
        {"tabletId", tabletId},
        {"exceptionType", TypeName(exc)},
        {"exceptionMessage", exc.what()},
        {"backtrace", TBackTrace::FromCurrentException().PrintToString()});

    GetServiceCounters(AppData()->Counters, "tablets")->GetCounter("alerts_scan_broken", true)->Inc();

    auto response = MakeHolder<TResponse>();
    FillScanResponseCommonFields(*response, scanId, tabletId, seqNo);
    response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);

    auto* issue = response->Record.AddIssues();
    issue->set_severity(NYql::TSeverityIds::S_ERROR);
    issue->set_message(TStringBuilder() << "Scan failed " << exc.what());

    TlsActivationContext->Send(new IEventHandle(sender, TActorId(), response.Release()));
}

}
