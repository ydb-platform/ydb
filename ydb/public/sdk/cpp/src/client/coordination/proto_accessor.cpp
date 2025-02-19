#include <ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb-cpp-sdk/client/coordination/coordination.h>

namespace NYdb::inline V3 {

const NYdbProtos::Coordination::DescribeNodeResult& TProtoAccessor::GetProto(const NCoordination::TNodeDescription& nodeDescription) {
    return nodeDescription.GetProto();
}

}