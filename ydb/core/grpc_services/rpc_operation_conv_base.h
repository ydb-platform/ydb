#pragma once

#include <ydb/public/api/protos/ydb_operation.pb.h>

namespace NKikimr::NGRpcService {

template <typename TOperation>
struct TOperationConv {
    using Operation = Ydb::Operations::Operation;

    static Operation ToOperation(const TOperation& in) {
        Operation operation;

        operation.set_ready(true);
        operation.set_status(in.GetStatus());

        if (in.IssuesSize()) {
            operation.mutable_issues()->CopyFrom(in.GetIssues());
        }

        if (in.HasStartTime()) {
            *operation.mutable_create_time() = in.GetStartTime();
        }

        if (in.HasEndTime()) {
            *operation.mutable_end_time() = in.GetEndTime();
        }

        if (in.HasUserSID()) {
            operation.set_created_by(in.GetUserSID());
        }

        return operation;
    }

protected:
    template <typename TMetadata, typename TResult, typename TSettings>
    static void Fill(Operation& out, const TOperation& in, const TSettings& settings) {
        TMetadata metadata;
        metadata.mutable_settings()->CopyFrom(settings);
        metadata.set_progress(in.GetProgress());
        metadata.mutable_items_progress()->CopyFrom(in.GetItemsProgress());
        out.mutable_metadata()->PackFrom(metadata);

        TResult result;
        out.mutable_result()->PackFrom(result);
    }
}; // TOperationConv

}
