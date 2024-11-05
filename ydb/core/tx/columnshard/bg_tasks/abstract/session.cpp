#include "session.h"
#include "adapter.h"
#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/lib/operation_id/protos/operation_id.pb.h>

namespace NKikimr::NOlap::NBackground {

Ydb::Operations::Operation TSessionInfoReport::SerializeToProto() const {
    Ydb::Operations::Operation result;
    result.set_id("/" + ::ToString((int)Ydb::TOperationId::SS_BG_TASKS) + "?type=" + ClassName + "&id=" + Identifier);
    result.set_ready(IsFinished);
    return result;
}

}