#include "upload_rows_common_impl.h"


namespace NKikimr {

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
        : TBase("BulkUpsert")
    {
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
