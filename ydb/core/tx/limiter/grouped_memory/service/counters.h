#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
public:
    const NMonitoring::TDynamicCounters::TCounterPtr MemoryUsageBytes;
    const NMonitoring::TDynamicCounters::TCounterPtr MemoryWaitingBytes;
    TCounters(const TString& limiterName, TIntrusivePtr<::NMonitoring::TDynamicCounters> baseSignals)
        : TBase(NColumnShard::TCommonCountersOwner("grouped_memory_limiter", baseSignals), "limiter_name", limiterName)
        , MemoryUsageBytes(TBase::GetValue("Memory/Usage/Bytes"))
        , MemoryWaitingBytes(TBase::GetValue("Memory/Waiting/Bytes")) {
    }
};

}
