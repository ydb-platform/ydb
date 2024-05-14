#include "session.h"
#include "adapter.h"
#include <ydb/public/api/protos/ydb_operation.pb.h>

namespace NKikimr::NOlap::NBackground {

Ydb::Operations::Operation TSessionInfoReport::SerializeToProto() const {
    Ydb::Operations::Operation result;
    result.set_id(ClassName + "::" + Identifier);
    result.set_ready(IsFinished);
    return result;
}

}