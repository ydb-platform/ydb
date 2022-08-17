#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb {
    const Ydb::Topic::DescribeTopicResult& TProtoAccessor::GetProto(const NTopic::TTopicDescription& topicDescription) {
        return topicDescription.GetProto();
    }
}// namespace NYdb

