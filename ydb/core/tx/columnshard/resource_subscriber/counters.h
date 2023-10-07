#pragma once
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {

class TSubscriberCounters;

class TSubscriberTypeCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr RequestsCount;
    NMonitoring::TDynamicCounters::TCounterPtr RequestBytes;
    std::shared_ptr<NColumnShard::TValueAggregationClient> CountRequested;
    std::shared_ptr<NColumnShard::TValueAggregationClient> BytesRequested;

    NMonitoring::TDynamicCounters::TCounterPtr RepliesCount;
    NMonitoring::TDynamicCounters::TCounterPtr ReplyBytes;

    YDB_READONLY_DEF(std::shared_ptr<NColumnShard::TValueAggregationClient>, BytesAllocated);
    YDB_READONLY_DEF(std::shared_ptr<NColumnShard::TValueAggregationClient>, CountAllocated);

public:
    TSubscriberTypeCounters(const TSubscriberCounters& owner, const TString& resourceType);

    void OnRequest(const ui64 bytes) const {
        RequestsCount->Add(1);
        RequestBytes->Add(bytes);
        CountRequested->Add(1);
        BytesRequested->Add(bytes);
    }

    void OnReply(const ui64 bytes) const {
        RepliesCount->Add(1);
        ReplyBytes->Add(bytes);
        CountRequested->Remove(1);
        BytesRequested->Remove(bytes);
    }
};

class TSubscriberCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    THashMap<TString, std::shared_ptr<TSubscriberTypeCounters>> ResourceTypeCounters;
    TMutex Mutex;
public:
    TSubscriberCounters()
        : TBase("ResourcesSubscriber")
    {
    }

    std::shared_ptr<TSubscriberTypeCounters> GetTypeCounters(const TString& resourceType);
};

}
