#pragma once

#include "topic_impl.h"
#include "transaction.h"

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/include/read_events.h>

#include <library/cpp/containers/disjoint_interval_tree/disjoint_interval_tree.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NYdb::inline V2::NTopic {

class TOffsetsCollector {
public:
    TVector<TTopicOffsets> GetOffsets() const;

    void CollectOffsets(const TVector<TReadSessionEvent::TEvent>& events);
    void CollectOffsets(const TReadSessionEvent::TEvent& event);

private:
    // topic -> partition -> (begin, end)
    using TOffsetRanges = THashMap<TString, THashMap<ui64, TDisjointIntervalTree<ui64>>>;

    void CollectOffsets(const TReadSessionEvent::TDataReceivedEvent& event);

    TOffsetRanges Ranges;
};

}
