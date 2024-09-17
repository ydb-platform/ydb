#include "upload_rows_common_impl.h"


namespace NKikimr {

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

        const google::protobuf::EnumDescriptor* descriptor = ::Ydb::StatusIds::StatusCode_descriptor();
        for (ui32 i = 0; i < (ui32)descriptor->value_count(); ++i) {
            auto vDescription = descriptor->value(i);
            CodesCount.emplace(vDescription->name(), CreateSubGroup("reply_code", vDescription->name()).GetDeriviative("Replies/Count"));
        }
    }

}
