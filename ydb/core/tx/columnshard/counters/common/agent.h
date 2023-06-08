#pragma once
#include "client.h"
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/system/mutex.h>
#include <deque>

namespace NKikimr::NColumnShard {
class TCommonCountersOwner;

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

class TValueAggregationAgent: TNonCopyable {
private:
    ::NMonitoring::TDynamicCounters::TCounterPtr ValueSignalSum;
    ::NMonitoring::TDynamicCounters::TCounterPtr ValueSignalMin;
    ::NMonitoring::TDynamicCounters::TCounterPtr ValueSignalMax;
    std::deque<i64> Values;
    TMutex Mutex;

    bool CalcAggregations(i64& sum, i64& minValue, i64& maxValue) const;
    std::optional<TSignalAggregations> GetAggregations() const;

public:
    TValueAggregationAgent(const TString& signalName, const TCommonCountersOwner& signalsOwner);

    i64* RegisterValue(const i64 zeroValue = 0);
    void ResendStatus() const;

    std::shared_ptr<TValueAggregationClient> GetClient(std::shared_ptr<TValueAggregationAgent> selfPtr);
};

}
