#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include "coordination.h"

namespace NYdb::inline V2 {

const Ydb::Coordination::DescribeNodeResult& TProtoAccessor::GetProto(const NCoordination::TNodeDescription& nodeDescription) {
    return nodeDescription.GetProto();
}

}