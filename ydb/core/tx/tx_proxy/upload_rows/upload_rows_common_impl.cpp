#include "upload_rows_common_impl.h"


namespace NKikimr {

    TUploadCounters::TUploadCounters()
        : TBase("BulkUpsert")
    {
        RequestsCount = TBase::GetDeriviative("Requests/Count");
        RepliesCount = TBase::GetDeriviative("Replies/Count");
        ReplyDuration = TBase::GetHistogram("Replies/Duration", NMonitoring::ExponentialHistogram(15, 2, 1));

        RowsCount = TBase::GetDeriviative("Rows/Count");
        PackageSize = TBase::GetHistogram("Rows/PackageSize", NMonitoring::ExponentialHistogram(15, 2, 10));
    }
}
