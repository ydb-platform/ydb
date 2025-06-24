#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap::NStorageOptimizer {

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    const NMonitoring::TDynamicCounters::TCounterPtr NodePortionsCount;

public:
    TGlobalCounters()
        : TBase("CompactionOptimizer")
        , NodePortionsCount(TBase::GetValue("Node/Portions/Count")) {
    }

    static const NMonitoring::TDynamicCounters::TCounterPtr& GetNodePortionsCount() {
        return Singleton<TGlobalCounters>()->NodePortionsCount;
    }
};

class TCounters {
public:
    const NMonitoring::TDynamicCounters::TCounterPtr NodePortionsCount;

    TCounters()
        : NodePortionsCount(TGlobalCounters::GetNodePortionsCount()) {
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer
