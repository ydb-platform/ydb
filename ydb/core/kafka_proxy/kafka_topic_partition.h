#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

namespace NKafka {
    struct TTopicPartition {
        TString TopicPath;
        ui32 PartitionId;

        bool operator==(const TTopicPartition &other) const = default;
    };

    struct TTopicPartitionHashFn {
        size_t operator()(const TTopicPartition& partition) const {
            return std::hash<TString>()(partition.TopicPath) ^ std::hash<int64_t>()(partition.PartitionId);
        }
    };
} // namespace NKafka