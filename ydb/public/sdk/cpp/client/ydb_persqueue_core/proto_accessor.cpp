#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb {
    const Ydb::PersQueue::V1::DescribeTopicResult& TProtoAccessor::GetProto(const NPersQueue::TDescribeTopicResult& topicDescription) {
        return topicDescription.GetProto();
    }
}// namespace NYdb

