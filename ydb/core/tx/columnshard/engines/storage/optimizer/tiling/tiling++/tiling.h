#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct Tiling : ICompactionUnit<TKey, TPortion> {
    struct TilingSettings {
        typename Accumulator<TKey, TPortion>::AccumulatorSettings AccumulatorSettings;
        typename LastLevel<TKey, TPortion>::LastLevelSettings LastLevelSettings;
        typename MiddleLevel<TKey, TPortion>::MiddleLevelSettings MiddleLevelSettings;
        ui64 AccumulatorPortionSizeLimit = 512ULL * 1024;
        ui8 K = 10;
        /// Exclusive upper bound on middle-level index (allowed middle indices: 2 .. MiddleLevelCount - 1).
        ui64 MiddleLevelCount = TILING_LAYERS_COUNT;
    };

    Tiling(TilingSettings settings, std::shared_ptr<TCounters> counters);

    /// Sub-levels update their own counters; skip ICompactionUnit's global counter to avoid double counting.
    void AddPortion(typename TPortion::TConstPtr p) override {
        DoAddPortion(p);
    }
    void RemovePortion(typename TPortion::TConstPtr p) override {
        DoRemovePortion(p);
    }

    TilingSettings Settings;
    Accumulator<TKey, TPortion> Accumulator;
    LastLevel<TKey, TPortion> LastLevel;
    THashMap<ui64, MiddleLevel<TKey, TPortion>> MiddleLevels;
    std::shared_ptr<TCounters> SharedCounters;

    TIntersectionTree<TKey, ui64> Intersections;
    THashMap<ui64, typename TPortion::TPtr> PortionById;
    THashMap<ui64, ui8> InternalLevel;
    std::shared_ptr<NLBuckets::TSimplePortionsGroupInfo> PortionsInfo;

    void DoActualize() override;
    void DoAddPortion(typename TPortion::TConstPtr) override;
    void DoRemovePortion(typename TPortion::TConstPtr) override;
    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)>) const override;
    TOptimizationPriority DoGetUsefulMetric() const override;
    std::pair<TOptimizationPriority, ui64> GetMiddleUsefulMetric() const;
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
