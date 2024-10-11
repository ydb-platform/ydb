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

namespace NYdb::NTopic {

class TOffsetsCollector {
public:
    // topic -> partition -> (begin, end)
    using TOffsetRanges = THashMap<TString, THashMap<ui64, TDisjointIntervalTree<ui64>>>;

    struct TTransactionInfo {
        TSpinLock Lock;
        bool IsActive = false;
        bool Subscribed = false;
        TOffsetRanges Ranges;
    };

    using TTransactionInfoPtr = std::shared_ptr<TTransactionInfo>;

    TTransactionInfoPtr GetOrCreateTransactionInfo(const TTransactionId& txId);

    TVector<TTopicOffsets> ExtractOffsets(const TTransactionId& txId);

    void CollectOffsets(const TTransactionId& txId,
                        const TVector<TReadSessionEvent::TEvent>& events);
    void CollectOffsets(const TTransactionId& txId,
                        const TReadSessionEvent::TEvent& event);

private:
    using TTransactionMap = THashMap<TTransactionId, TTransactionInfoPtr>;

    void CollectOffsets(TTransactionInfoPtr txInfo,
                        const TReadSessionEvent::TEvent& event);
    void CollectOffsets(TTransactionInfoPtr txInfo,
                        const TReadSessionEvent::TDataReceivedEvent& event);

    TTransactionMap Txs;
};

}
