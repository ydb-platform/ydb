#include <ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb-cpp-sdk/client/coordination/coordination.h>

namespace NYdb::inline Dev {

const Ydb::Coordination::DescribeNodeResult& TProtoAccessor::GetProto(const NCoordination::TNodeDescription& nodeDescription) {
    return nodeDescription.GetProto();
}

}