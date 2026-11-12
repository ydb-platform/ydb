#pragma once
#include "client.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/system/mutex.h>
#include <deque>

namespace NKikimr::NColumnShard {
class TCommonCountersOwner;
class TLazyValue;

class TSignalAggregations {
public:
    const i64 Sum = 0;
    const i64 Min = 0;
    const i64 Max = 0;
    TSignalAggregations(const i64 sum, const i64 min, const i64 max)
        : Sum(sum)
        , Min(min)
        , Max(max) {

    }
};

// The SUM/MIN/MAX signals are registered on the first status after a client was written,
// so an aggregation nobody writes is not exported.
class TValueAggregationAgent: TNonCopyable {
private:
    const std::shared_ptr<TLazyValue> ValueSignalSum;
    const std::shared_ptr<TLazyValue> ValueSignalMin;
    const std::shared_ptr<TLazyValue> ValueSignalMax;
    mutable bool Exported = false;
    mutable std::list<std::shared_ptr<TValueAggregationClient>> Values;
    TMutex Mutex;

    bool CalcAggregationsAndClean(i64& sum, i64& minValue, i64& maxValue) const;
    std::optional<TSignalAggregations> GetAggregations() const;
    bool HasWrittenClient() const;

public:
    TValueAggregationAgent(const TString& signalName, const TCommonCountersOwner& signalsOwner);
    void ResendStatus() const;
    void UnregisterClient(std::list<TValueAggregationClient*>::iterator it);

    std::shared_ptr<TValueAggregationClient> GetClient();
};

}
