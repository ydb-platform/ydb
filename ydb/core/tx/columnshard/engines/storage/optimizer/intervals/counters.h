#pragma once
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>
#include <ydb/core/formats/arrow/replace_key.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/core/tx/columnshard/splitter/settings.h>
#include <ydb/core/tx/columnshard/counters/engine_logs.h>

namespace NKikimr::NOlap::NStorageOptimizer {

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::TDynamicCounters::TCounterPtr SmallPortionsCount;
    std::shared_ptr<NColumnShard::TIncrementalHistogram> HistogramOverlappedIntervalsCount;
    std::shared_ptr<NColumnShard::TIncrementalHistogram> HistogramOverlappedIntervalsPackedSizeCount;
    std::shared_ptr<NColumnShard::TIncrementalHistogram> HistogramOverlappedIntervalsRawSizeCount;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> SmallPortionsCountByGranule;
public:
    TGlobalCounters()
        : TBase("IntervalsStorageOptimizer") {
        SmallPortionsCountByGranule = TBase::GetValueAutoAggregations("Granule/SmallPortions/Count");
        SmallPortionsCount = TBase::GetValue("SmallPortions/Count");

        const std::set<i64> borders = {0, 1, 2, 4, 8, 16, 32, 64};
        HistogramOverlappedIntervalsCount = std::make_shared<NColumnShard::TIncrementalHistogram>("IntervalsStorageOptimizer", "OverlappedIntervals/Count", "", borders);
        HistogramOverlappedIntervalsPackedSizeCount = std::make_shared<NColumnShard::TIncrementalHistogram>("IntervalsStorageOptimizer", "OverlappedIntervals/Size/Packed", "", borders);
        HistogramOverlappedIntervalsRawSizeCount = std::make_shared<NColumnShard::TIncrementalHistogram>("IntervalsStorageOptimizer", "OverlappedIntervals/Size/Raw", "", borders);
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildClientSmallPortionsAggregation() {
        return Singleton<TGlobalCounters>()->SmallPortionsCountByGranule->GetClient();
    }

    static std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> BuildGuardIntervalsOverlapping() {
        return Singleton<TGlobalCounters>()->HistogramOverlappedIntervalsCount->BuildGuard();
    }

    static std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> BuildGuardIntervalsPackedSizeOverlapping() {
        return Singleton<TGlobalCounters>()->HistogramOverlappedIntervalsPackedSizeCount->BuildGuard();
    }

    static std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> BuildGuardIntervalsRawSizeOverlapping() {
        return Singleton<TGlobalCounters>()->HistogramOverlappedIntervalsRawSizeCount->BuildGuard();
    }

    static std::shared_ptr<NColumnShard::TValueGuard> BuildSmallPortionsGuard() {
        return std::make_shared<NColumnShard::TValueGuard>(Singleton<TGlobalCounters>()->SmallPortionsCount);
    }

};

class TCounters {
private:
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> IntervalsGuard;
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> IntervalsPackedSizeGuard;
    std::shared_ptr<NColumnShard::TIncrementalHistogram::TGuard> IntervalsRawSizeGuard;
    std::shared_ptr<NColumnShard::TValueGuard> SmallPortionsCount;
public:
    std::shared_ptr<NColumnShard::TValueAggregationClient> SmallPortionsByGranule;
    i64 GetSmallCounts() const {
        return SmallPortionsByGranule->GetValueSimple();
    }

    TCounters() {
        IntervalsGuard = TGlobalCounters::BuildGuardIntervalsOverlapping();
        IntervalsPackedSizeGuard = TGlobalCounters::BuildGuardIntervalsPackedSizeOverlapping();
        IntervalsRawSizeGuard = TGlobalCounters::BuildGuardIntervalsRawSizeOverlapping();
        SmallPortionsCount = TGlobalCounters::BuildSmallPortionsGuard();
        SmallPortionsByGranule = TGlobalCounters::BuildClientSmallPortionsAggregation();
    }

    void OnRemoveIntervalsCount(const ui32 count, const ui64 rawSize, const ui64 packedSize) {
        IntervalsGuard->Sub(count, 1);
        IntervalsPackedSizeGuard->Sub(count, packedSize);
        IntervalsRawSizeGuard->Sub(count, rawSize);
    }

    void OnAddIntervalsCount(const ui32 count, const ui64 rawSize, const ui64 packedSize) {
        IntervalsGuard->Add(count, 1);
        IntervalsPackedSizeGuard->Add(count, packedSize);
        IntervalsRawSizeGuard->Add(count, rawSize);
    }

    void OnAddSmallPortion() {
        SmallPortionsCount->Add(1);
        SmallPortionsByGranule->Add(1);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("add_opt_count", SmallPortionsByGranule->GetValueSimple())("counter", (ui64)SmallPortionsByGranule.get());
    }

    void OnRemoveSmallPortion() {
        SmallPortionsCount->Sub(1);
        SmallPortionsByGranule->Remove(1);
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("remove_opt_count", SmallPortionsByGranule->GetValueSimple())("counter", (ui64)SmallPortionsByGranule.get());
    }

};

}
