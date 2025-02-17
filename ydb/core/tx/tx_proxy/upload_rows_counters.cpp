#include "upload_rows_counters.h"


namespace NKikimr {

TUploadStatus::TUploadStatus(const NSchemeCache::TSchemeCacheNavigate::EStatus status) {
    switch (status) {
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
            Code = Ydb::StatusIds::SUCCESS;
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
            Code = Ydb::StatusIds::UNAVAILABLE;
            ErrorMessage = "table unavaliable";
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
        case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
            Code = Ydb::StatusIds::SCHEME_ERROR;
            ErrorMessage = "unknown table";
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            Code = Ydb::StatusIds::SCHEME_ERROR;
            ErrorMessage = "unknown database";
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
            Code = Ydb::StatusIds::UNAUTHORIZED;
            ErrorMessage = "access denied";
            break;
        case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
            Code = Ydb::StatusIds::GENERIC_ERROR;
            ErrorMessage = "unknown error";
            break;
    }
}

TUploadStatus::TUploadStatus(const NKikimrTxDataShard::TError::EKind status, const TString& errorDescription) {
    Subcode = NKikimrTxDataShard::TError::EKind_Name(status);
    if (status != NKikimrTxDataShard::TError::OK) {
        ErrorMessage = errorDescription;
    }
    switch (status) {
        case NKikimrTxDataShard::TError::OK:
            Code = Ydb::StatusIds::SUCCESS;
            break;
        case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
        case NKikimrTxDataShard::TError::SHARD_IS_BLOCKED:
            Code = Ydb::StatusIds::OVERLOADED;
            break;
        case NKikimrTxDataShard::TError::DISK_SPACE_EXHAUSTED:
        case NKikimrTxDataShard::TError::OUT_OF_SPACE:
            Code = Ydb::StatusIds::UNAVAILABLE;
            break;
        case NKikimrTxDataShard::TError::SCHEME_ERROR:
            Code = Ydb::StatusIds::SCHEME_ERROR;
            break;
        case NKikimrTxDataShard::TError::BAD_ARGUMENT:
            Code = Ydb::StatusIds::BAD_REQUEST;
            break;
        case NKikimrTxDataShard::TError::EXECUTION_CANCELLED:
            Code = Ydb::StatusIds::TIMEOUT;
            break;
        default:
            Code = Ydb::StatusIds::GENERIC_ERROR;
            break;
    };
}

NMonitoring::TDynamicCounters::TCounterPtr TUploadCounters::GetCodeCounter(const TUploadStatus& status) {
    auto it = CodesCount.FindPtr(status);
    if (it) {
        return *it;
    }
    const auto counters = [this, &status]() {
        auto groupByCode = CreateSubGroup("reply_code", status.GetCodeString());
        if (status.GetSubcode()) {
            return groupByCode.CreateSubGroup("subcode", *status.GetSubcode());
        }
        return groupByCode;
    }();
    return CodesCount.emplace(status, counters.GetDeriviative("Replies/Count")).first->second;
}

TUploadCounters::TUploadCounters()
    : TBase("BulkUpsert") {
            RequestsCount = TBase::GetDeriviative("Requests/Count");
            ReplyDuration = TBase::GetHistogram("Replies/Duration", NMonitoring::ExponentialHistogram(15, 2, 10));

            RowsCount = TBase::GetDeriviative("Rows/Count");
            PackageSizeRecordsByRecords = TBase::GetHistogram("ByRecords/PackageSize/Records", NMonitoring::ExponentialHistogram(15, 2, 10));
            PackageSizeCountByRecords = TBase::GetHistogram("ByRecords/PackageSize/Count", NMonitoring::ExponentialHistogram(15, 2, 10));

            PreparingDuration = TBase::GetHistogram("Preparing/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10));
            WritingDuration = TBase::GetHistogram("Writing/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10));
            CommitDuration = TBase::GetHistogram("Commit/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10));
            PrepareReplyDuration = TBase::GetHistogram("ToReply/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10));
}
}
