#pragma once

#include "topic_impl.h"
#include "transaction.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/read_events.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NYdb::inline Dev::NTopic {

class TOffsetsCollector {
public:
    std::vector<TTopicOffsets> GetOffsets() const;

    void CollectOffsets(const std::vector<TReadSessionEvent::TEvent>& events);
    void CollectOffsets(const TReadSessionEvent::TEvent& event);

private:
    // topic -> partition -> (begin, end)
    using TOffsetRanges = std::unordered_map<std::string, std::unordered_map<uint64_t, TDisjointIntervalTree<uint64_t>>>;

    void CollectOffsets(const TReadSessionEvent::TDataReceivedEvent& event);

    TOffsetRanges Ranges;
};

}
