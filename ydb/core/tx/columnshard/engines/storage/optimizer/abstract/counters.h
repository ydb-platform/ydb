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
    const NMonitoring::TDynamicCounters::TCounterPtr BadPortionsCountLimit;

public:
    TGlobalCounters()
        : TBase("CompactionOptimizer")
        , NodePortionsCount(TBase::GetValue("Node/Portions/Count"))
        , NodePortionsCountLimit(TBase::GetValue("Node/Portions/Limit/Count"))
        , BadPortionsCountLimit(TBase::GetValue("Node/BadPortions/Limit/Count"))
    {
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetNodePortionsCount() {
        return Singleton<TGlobalCounters>()->NodePortionsCount;
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetNodePortionsCountLimit() {
        return Singleton<TGlobalCounters>()->NodePortionsCountLimit;
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetNodeBadPortionsCountLimit() {
        return Singleton<TGlobalCounters>()->BadPortionsCountLimit;
    }
};

class TCounters {
public:
    const NMonitoring::TDynamicCounters::TCounterPtr NodePortionsCount;
    const NMonitoring::TDynamicCounters::TCounterPtr NodePortionsCountLimit;
    const NMonitoring::TDynamicCounters::TCounterPtr BadPortionsCountLimit;

    TCounters()
        : NodePortionsCount(TGlobalCounters::GetNodePortionsCount())
        , NodePortionsCountLimit(TGlobalCounters::GetNodePortionsCountLimit())
        , BadPortionsCountLimit(TGlobalCounters::GetNodeBadPortionsCountLimit())
    {
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer
