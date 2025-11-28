#pragma once

#include <ydb/library/signals/owner.h>

namespace NKikimr::NColumnShard::NOverload {

class TCSOverloadManagerCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadSubscribesCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadUnsubscribesCount;
    NMonitoring::TDynamicCounters::TCounterPtr OverloadReadyCount;

public:
    TCSOverloadManagerCounters(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
        : TBase("CSOverloadManager", countersGroup)
        , OverloadSubscribesCount(TBase::GetDeriviative("Overload/Shard/Subscribes/Count"))
        , OverloadUnsubscribesCount(TBase::GetDeriviative("Overload/Shard/Unsubscribes/Count"))
        , OverloadReadyCount(TBase::GetDeriviative("Overload/Shard/Ready/Count")) {
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
};

} // namespace NKikimr::NColumnShard::NOverload