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

        operator bool() const {
            return !Buffer.IsEmpty();
        }
    };

public:
    TBatchRowsUploader(const TIndexBuildScanSettings& scanSettings)
        : ScanSettings(scanSettings)
    {}

    TBufferData* AddDestination(TString table, std::shared_ptr<NTxProxy::TUploadTypes> types) {
        auto& dst = Destinations[table];
        dst.Table = std::move(table);
        dst.Types = std::move(types);
        return &dst.Buffer;
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        Y_ENSURE(UploaderId == ev->Sender, "Mismatch"
            << " Uploader: " << UploaderId.ToString()
            << " Sender: " << ev->Sender.ToString());
        Y_ENSURE(Uploading);

        UploaderId = {};

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues = ev->Get()->Issues;
        if (!UploadStatus.IsSuccess()) {
            return;
        }

        UploadRows += Uploading.Buffer.GetRows();
        UploadBytes += Uploading.Buffer.GetRowCellBytes();
        Uploading.Buffer.Clear();
        RetryCount = 0;

        for (auto& [_, dst] : Destinations) {
            if (TryUpload(dst, true /* by limit */)) {
                break;
            }
        }
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
            StartUploadRowsInternal();
            return true;
        }

        return false;
    }

    void StartUploadRowsInternal() {
        LOG_D("TBatchRowsUploader StartUploadRowsInternal " << Debug());

        Y_ENSURE(Uploading);
        Y_ENSURE(!Uploading.Buffer.IsEmpty());
        Y_ENSURE(!UploaderId);
        Y_ENSURE(Owner);
        auto actor = NTxProxy::CreateUploadRowsInternal(
            Owner, Uploading.Table, Uploading.Types, Uploading.Buffer.GetRowsData(),
            NTxProxy::EUploadRowsMode::WriteToTableShadow,
            true /*writeToPrivateTable*/,
            true /*writeToIndexImplTable*/);

        UploaderId = TlsActivationContext->Register(actor);
    }

private:
    const TIndexBuildScanSettings ScanSettings;
    TActorId Owner;

    TMap<TString, TDestination> Destinations;
    TActorId UploaderId = {};
    TDestination Uploading;
    TUploadStatus UploadStatus = {};
    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;
    ui32 RetryCount = 0;
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
    LOG_E("Unhandled exception " << logScanType << " TabletId: " << tabletId
        << " " << TypeName(exc) << ": " << exc.what() << Endl
        << TBackTrace::FromCurrentException().PrintToString());

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
