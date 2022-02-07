#pragma once
#include <google/protobuf/any.pb.h>

#include <util/generic/maybe.h>

namespace NYql::NPq {

// Partitioning parameters for topic
struct TTopicPartitionsSet {
    ui64 EachTopicPartitionGroupId;
    ui64 DqPartitionsCount;
    ui64 TopicPartitionsCount;

    bool Intersects(const TTopicPartitionsSet& other) const {
        return DqPartitionsCount != other.DqPartitionsCount || EachTopicPartitionGroupId == other.EachTopicPartitionGroupId;
    }
};

TMaybe<TTopicPartitionsSet> GetTopicPartitionsSet(const google::protobuf::Any& dqTaskMeta);

} // namespace NYql::NPq
