#include "upload_rows_common_impl.h"


namespace NKikimr {

    TUploadCounters::TUploadCounters()
        : TBase("BulkUpsert")
    {
        RequestsCount = TBase::GetDeriviative("Requests/Count");
        ReplyDuration = TBase::GetHistogram("Replies/Duration", NMonitoring::ExponentialHistogram(15, 2, 1));

        RowsCount = TBase::GetDeriviative("Rows/Count");
        PackageSize = TBase::GetHistogram("Rows/PackageSize", NMonitoring::ExponentialHistogram(15, 2, 10));

        DurationToStartCommit = TBase::GetHistogram("ToStartCommit/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10));
        DurationToFinishCommit = TBase::GetHistogram("ToFinishCommit/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10));
        DurationToStartWriting = TBase::GetHistogram("ToStartWriting/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10));
        DurationToTxStarted = TBase::GetHistogram("ToTxStarted/DurationMs", NMonitoring::ExponentialHistogram(15, 2, 10));

        const google::protobuf::EnumDescriptor* descriptor = ::Ydb::StatusIds::StatusCode_descriptor();
        for (ui32 i = 0; i < (ui32)descriptor->value_count(); ++i) {
            auto vDescription = descriptor->value(i);
            CodesCount.emplace(vDescription->name(), CreateSubGroup("reply_code", vDescription->name()).GetDeriviative("Replies/Count"));
        }
    }

}
