#pragma once

#include <ydb/core/protos/pqconfig.pb.h>

namespace Ydb::Topic {
    class Consumer;
    class DescribeTopicResult;
}

namespace NKikimrSchemeOp {
    class TPersQueueGroupDescription;
}

namespace NKikimr {

void FillConsumer(Ydb::Topic::Consumer& out,
    const NKikimrPQ::TPQTabletConfig::TConsumer& in);
void FillTopicDescription(Ydb::Topic::DescribeTopicResult& out,
   const NKikimrSchemeOp::TPersQueueGroupDescription& in);

} // namespace NKikimr
