#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

namespace NKikimr::NOlap::NStorageOptimizer::NLevels {

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr SmallPortionsCount;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> SmallPortionsCountByGranule;

    std::shared_ptr<NColumnShard::TValueAggregationAgent> CriticalRecordsCount;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> NormalRecordsCount;

    std::shared_ptr<NColumnShard::TValueAggregationAgent> OldestCriticalActuality;
public:
    TGlobalCounters()
        : TBase("LevelsStorageOptimizer")
    {
        SmallPortionsCount = TBase::GetValue("SmallPortions/Count");
        CriticalRecordsCount = TBase::GetValueAutoAggregations("Granule/CriticalRecord/Count");
        NormalRecordsCount = TBase::GetValueAutoAggregations("Granule/NormalRecord/Count");
        OldestCriticalActuality = TBase::GetValueAutoAggregations("Granule/ActualityMs");
        SmallPortionsCountByGranule = TBase::GetValueAutoAggregations("Granule/SmallPortions/Count");
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildOldestCriticalActualityAggregation() {
        return Singleton<TGlobalCounters>()->OldestCriticalActuality->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildClientSmallPortionsAggregation() {
        return Singleton<TGlobalCounters>()->SmallPortionsCountByGranule->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueGuard> BuildSmallPortionsGuard() {
        return std::make_shared<NColumnShard::TValueGuard>(Singleton<TGlobalCounters>()->SmallPortionsCount);
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildCriticalRecordsCountAggregation() {
        return Singleton<TGlobalCounters>()->CriticalRecordsCount->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildNormalRecordsCountAggregation() {
        return Singleton<TGlobalCounters>()->NormalRecordsCount->GetClient();
    }

};

class TCounters {
private:
    std::shared_ptr<NColumnShard::TValueAggregationClient> CriticalRecordsCount;
    std::shared_ptr<NColumnShard::TValueAggregationClient> NormalRecordsCount;

    std::shared_ptr<NColumnShard::TValueAggregationClient> OldestCriticalActuality;

    std::shared_ptr<NColumnShard::TValueGuard> SmallPortionsCount;
    std::shared_ptr<NColumnShard::TValueAggregationClient> SmallPortionsByGranule;
public:
    i64 GetSmallCounts() const {
        return SmallPortionsByGranule->GetValueSimple();
    }

    TCounters() {
        CriticalRecordsCount = TGlobalCounters::BuildCriticalRecordsCountAggregation();
        NormalRecordsCount = TGlobalCounters::BuildNormalRecordsCountAggregation();
        SmallPortionsCount = TGlobalCounters::BuildSmallPortionsGuard();
        SmallPortionsByGranule = TGlobalCounters::BuildClientSmallPortionsAggregation();
        OldestCriticalActuality = TGlobalCounters::BuildOldestCriticalActualityAggregation();
    }

    void OnMinProblemSnapshot(const TDuration d) {
        OldestCriticalActuality->SetValue(d.MilliSeconds(), TInstant::Now() + TDuration::Seconds(10));
    }

    void OnAddCriticalCount(const ui32 count) {
        CriticalRecordsCount->Add(count);
    }

    void OnAddNormalCount(const ui32 count) {
        NormalRecordsCount->Add(count);
    }

    void OnRemoveCriticalCount(const ui32 count) {
        CriticalRecordsCount->Remove(count);
    }

    void OnRemoveNormalCount(const ui32 count) {
        NormalRecordsCount->Remove(count);
    }

    void OnAddSmallPortion() {
        SmallPortionsCount->Add(1);
        SmallPortionsByGranule->Add(1);
    }

    void OnRemoveSmallPortion() {
        SmallPortionsCount->Sub(1);
        SmallPortionsByGranule->Remove(1);
    }

};

}
