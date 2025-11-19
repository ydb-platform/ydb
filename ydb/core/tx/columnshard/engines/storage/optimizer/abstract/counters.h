#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NStorageOptimizer {

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    const NMonitoring::TDynamicCounters::TCounterPtr NodePortionsCount;
    const NMonitoring::TDynamicCounters::TCounterPtr NodePortionsCountLimit;

public:
    TGlobalCounters()
        : TBase("CompactionOptimizer")
        , NodePortionsCount(TBase::GetValue("Node/Portions/Count"))
        , NodePortionsCountLimit(TBase::GetValue("Node/Portions/Limit/Count"))
    {
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetNodePortionsCount() {
        return Singleton<TGlobalCounters>()->NodePortionsCount;
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetNodePortionsCountLimit() {
        return Singleton<TGlobalCounters>()->NodePortionsCountLimit;
    }
};

class TCounters {
public:
    const NMonitoring::TDynamicCounters::TCounterPtr NodePortionsCount;
    const NMonitoring::TDynamicCounters::TCounterPtr NodePortionsCountLimit;

    TCounters()
        : NodePortionsCount(TGlobalCounters::GetNodePortionsCount())
        , NodePortionsCountLimit(TGlobalCounters::GetNodePortionsCountLimit())
    {
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer
