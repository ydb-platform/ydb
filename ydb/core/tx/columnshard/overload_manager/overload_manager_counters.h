#pragma once

#include <ydb/library/signals/owner.h>

namespace NKikimr::NColumnShard::NOverload {

class TCSOverloadManagerCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadSubscribesCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadUnsubscribesCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadReadyCount;
    NMonitoring::TDynamicCounters::TCounterPtr CompactionOverloadCount;
    NMonitoring::TDynamicCounters::TCounterPtr CompactionReadyCount;
    NMonitoring::TDynamicCounters::TCounterPtr CompactionOverloadedTablets;

public:
    TCSOverloadManagerCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
        : TBase("CSOverloadManager", countersGroup)
        , OverloadSubscribesCount(TBase::GetDeriviative("Overload/Shard/Subscribes/Count"))
        , OverloadUnsubscribesCount(TBase::GetDeriviative("Overload/Shard/Unsubscribes/Count"))
        , OverloadReadyCount(TBase::GetDeriviative("Overload/Shard/Ready/Count"))
        , CompactionOverloadCount(TBase::GetDeriviative("Overload/Compaction/Overload/Count"))
        , CompactionReadyCount(TBase::GetDeriviative("Overload/Compaction/Ready/Count"))
        , CompactionOverloadedTablets(TBase::GetValue("Overload/Compaction/Tablets/Count"))
    {
    }

    void OnOverloadSubscribe() const {
        OverloadSubscribesCount->Inc();
    }

    void OnOverloadUnsubscribe() const {
        OverloadUnsubscribesCount->Inc();
    }

    void OnOverloadReady() const {
        OverloadReadyCount->Inc();
    }

    void OnCompactionOverload() const {
        CompactionOverloadCount->Inc();
    }

    void OnCompactionReady() const {
        CompactionReadyCount->Inc();
    }

    void SetCompactionOverloadedTablets(ui64 count) const {
        CompactionOverloadedTablets->Set(count);
    }
};

}   // namespace NKikimr::NColumnShard::NOverload
