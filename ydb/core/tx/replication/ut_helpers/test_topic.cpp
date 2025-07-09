#include "test_topic.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NReplication::NTestHelpers {

void TTestTopicDescription::SerializeTo(NKikimrSchemeOp::TPersQueueGroupDescription& proto) const {
    proto.SetName(Name);
    proto.SetTotalGroupCount(1);

    auto& pq = *proto.MutablePQTabletConfig();
    auto& p = *pq.MutablePartitionConfig();
    p.SetLifetimeSeconds(3600);
}

THolder<NKikimrSchemeOp::TPersQueueGroupDescription> MakeTopicDescription(const TTestTopicDescription& desc) {
    auto result = MakeHolder<NKikimrSchemeOp::TPersQueueGroupDescription>();
    desc.SerializeTo(*result);
    return result;
}

}
