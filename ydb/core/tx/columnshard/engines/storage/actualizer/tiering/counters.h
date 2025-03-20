#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

namespace NKikimr::NOlap::NActualizer {

class TTieringGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

    std::shared_ptr<NColumnShard::TValueAggregationAgent> QueueSizeToEvict;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> QueueSizeToDelete;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> DifferenceWaitToEvict;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> DifferenceWaitToDelete;
    NMonitoring::TDynamicCounters::TCounterPtr SkipEvictionForCompaction;
    NMonitoring::TDynamicCounters::TCounterPtr SkipEvictionForLimit;
    NMonitoring::TDynamicCounters::TCounterPtr SkipEvictionForTooEarly;
    NMonitoring::TDynamicCounters::TCounterPtr SkipEvictionForNoLongerNeeded;
public:
    TTieringGlobalCounters()
        : TBase("TieringActualizer")
    {
        QueueSizeToEvict = TBase::GetValueAutoAggregations("Granule/Eviction/QueueSize");
        QueueSizeToDelete = TBase::GetValueAutoAggregations("Granule/Deletion/QueueSize");
        DifferenceWaitToEvict = TBase::GetValueAutoAggregations("Granule/Eviction/WaitingInSeconds");
        DifferenceWaitToDelete = TBase::GetValueAutoAggregations("Granule/Deletion/WaitingInSeconds");
        SkipEvictionForCompaction = TBase::GetDeriviative("Eviction/SkipForCompaction");
        SkipEvictionForLimit = TBase::GetDeriviative("Eviction/SkipForLimit");
        SkipEvictionForTooEarly = TBase::GetDeriviative("Eviction/SkipForTooEarly");
        SkipEvictionForNoLongerNeeded = TBase::GetDeriviative("Eviction/SkipForNoLongerNeeded");
    }

    static NMonitoring::TDynamicCounters::TCounterPtr GetSkipEvictionForLimit() {
        return Singleton<TTieringGlobalCounters>()->SkipEvictionForLimit;
    }

    static NMonitoring::TDynamicCounters::TCounterPtr GetSkipEvictionForTooEarly() {
        return Singleton<TTieringGlobalCounters>()->SkipEvictionForTooEarly;
    }

    static NMonitoring::TDynamicCounters::TCounterPtr GetSkipEvictionForNoLongerNeeded() {
        return Singleton<TTieringGlobalCounters>()->SkipEvictionForNoLongerNeeded;
    }

    static NMonitoring::TDynamicCounters::TCounterPtr GetSkipEvictionForCompaction() {
        return Singleton<TTieringGlobalCounters>()->SkipEvictionForCompaction;
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildQueueSizeToEvict() {
        return Singleton<TTieringGlobalCounters>()->QueueSizeToEvict->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildQueueSizeToDelete() {
        return Singleton<TTieringGlobalCounters>()->QueueSizeToDelete->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildDifferenceWaitToEvict() {
        return Singleton<TTieringGlobalCounters>()->DifferenceWaitToEvict->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildDifferenceWaitToDelete() {
        return Singleton<TTieringGlobalCounters>()->DifferenceWaitToDelete->GetClient();
    }

};

class TTieringCounters {
public:
    const std::shared_ptr<NColumnShard::TValueAggregationClient> QueueSizeToEvict;
    const std::shared_ptr<NColumnShard::TValueAggregationClient> QueueSizeToDelete;
    const std::shared_ptr<NColumnShard::TValueAggregationClient> DifferenceWaitToEvict;
    const std::shared_ptr<NColumnShard::TValueAggregationClient> DifferenceWaitToDelete;
    const NMonitoring::TDynamicCounters::TCounterPtr SkipEvictionForCompaction;
    const NMonitoring::TDynamicCounters::TCounterPtr SkipEvictionForLimit;
    const NMonitoring::TDynamicCounters::TCounterPtr SkipEvictionForTooEarly;
    const NMonitoring::TDynamicCounters::TCounterPtr SkipEvictionForNoLongerNeeded;

    TTieringCounters()
        : QueueSizeToEvict(TTieringGlobalCounters::BuildQueueSizeToEvict())
        , QueueSizeToDelete(TTieringGlobalCounters::BuildQueueSizeToDelete())
        , DifferenceWaitToEvict(TTieringGlobalCounters::BuildDifferenceWaitToEvict())
        , DifferenceWaitToDelete(TTieringGlobalCounters::BuildDifferenceWaitToDelete())
        , SkipEvictionForCompaction(TTieringGlobalCounters::GetSkipEvictionForCompaction())
        , SkipEvictionForLimit(TTieringGlobalCounters::GetSkipEvictionForLimit())
        , SkipEvictionForTooEarly(TTieringGlobalCounters::GetSkipEvictionForTooEarly())
        , SkipEvictionForNoLongerNeeded(TTieringGlobalCounters::GetSkipEvictionForNoLongerNeeded()) {
    }

};

}
