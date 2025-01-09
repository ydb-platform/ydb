#pragma once

// #include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
// #include <ydb/public/api/protos/ydb_table.pb.h>

namespace Ydb {
    namespace Topic {
        class Consumer;
        class DescribeTopicResult;
    }
}

// namespace NKikimrPQ {
//     class TPQTabletConfig {
//     public:
//         class TConsumer;
//     };
// }

namespace NKikimrSchemeOp {
    class TPersQueueGroupDescription;
}

namespace NKikimr {

void FillConsumerProto(Ydb::Topic::Consumer& out,
    const NKikimrPQ::TPQTabletConfig::TConsumer& in);
void FillTopicDescription(Ydb::Topic::DescribeTopicResult& out,
   const NKikimrSchemeOp::TPersQueueGroupDescription& in);

} // namespace NKikimr
