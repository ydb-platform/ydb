#include "upload_rows_common_impl.h"


namespace NKikimr {

    TUploadCounters::TUploadCounters()
        : TBase("BulkUpsert")
    {
        RequestsCount = TBase::GetDeriviative("Requests/Count");
        ReplyDuration = TBase::GetHistogram("Replies/Duration", NMonitoring::ExponentialHistogram(15, 2, 1));

        RowsCount = TBase::GetDeriviative("Rows/Count");
        PackageSize = TBase::GetHistogram("Rows/PackageSize", NMonitoring::ExponentialHistogram(15, 2, 10));

        const google::protobuf::EnumDescriptor* descriptor = ::Ydb::StatusIds::StatusCode_descriptor();
        for (ui32 i = 0; i < (ui32)descriptor->value_count(); ++i) {
            auto vDescription = descriptor->value(i);
            CodesCount.emplace(vDescription->name(), CreateSubGroup("reply_code", vDescription->name()).GetDeriviative("Replies/Count"));
        }
    }

    void TUploadCounters::OnReply(const TDuration d, const ::Ydb::StatusIds::StatusCode code) const {
        const TString name = ::Ydb::StatusIds::StatusCode_Name(code);
        auto it = CodesCount.find(name);
        Y_ABORT_UNLESS(it != CodesCount.end());
        it->second->Add(1);
        ReplyDuration->Collect(d.MilliSeconds());
    }

}
