#include "session.h"
#include "adapter.h"
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/library/operation_id/operation_id.h>

namespace NKikimr::NOlap::NBackground {

Ydb::Operations::Operation TSessionInfoReport::SerializeToProto() const {
    Ydb::Operations::Operation result;
    result.set_id("/" + ::ToString((int)NKikimr::NOperationId::TOperationId::SS_BG_TASKS) + "?type=" + ClassName + "&id=" + Identifier);
    result.set_ready(IsFinished);
    return result;
}

}