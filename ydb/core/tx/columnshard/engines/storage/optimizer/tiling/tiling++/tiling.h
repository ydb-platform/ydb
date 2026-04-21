#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/abstract/optimizer.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

/// Owns a single TCounters instance for one tiling compaction graph. Declared as the first base of Tiling so
/// ICompactionUnit can bind to tiling-level counters while sub-levels share the same TCounters via constructor args.
struct TTilingCompactionCountersHolder {
    TCounters Counters;
};

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct Tiling : private TTilingCompactionCountersHolder, ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
    using TLevelCounters = typename TBase::TLevelCounters;

    using TAccumulatorSettings = typename Accumulator<TKey, TPortion>::AccumulatorSettings;
    using TLastLevelSettings = typename LastLevel<TKey, TPortion>::LastLevelSettings;
    using TMiddleLevelSettings = typename MiddleLevel<TKey, TPortion>::MiddleLevelSettings;

    struct TilingSettings {
        TAccumulatorSettings AccumulatorSettings;
        TLastLevelSettings LastLevelSettings;
        TMiddleLevelSettings MiddleLevelSettings;
        ui64 AccumulatorPortionSizeLimit = 512ULL * 1024;
        ui8 K = 10;
        /// Exclusive upper bound on middle-level index (allowed middle indices: 2 .. MiddleLevelCount - 1).
        ui64 MiddleLevelCount = TILING_LAYERS_COUNT;
    };

    Tiling(TilingSettings settings);

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

    TIntersectionTree<TKey, ui64> Intersections;
    THashMap<ui64, typename TPortion::TPtr> PortionById;
    /// DEBUG routing snapshot: must match physical home (Accumulator / LastLevel / MiddleLevel) for counter Add/Sub.
    struct TPortionPlacementForDebug {
        ui8 Level = 0;
        ui64 Width = 0;
    };
    THashMap<ui64, TPortionPlacementForDebug> InternalLevelForDebug;
    // NLBuckets::TSimplePortionsGroupInfo PortionsInfo;

    void DoActualize() override;
    void DoAddPortion(typename TPortion::TConstPtr) override;
    void DoRemovePortion(typename TPortion::TConstPtr) override;
    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)>) const override;
    TOptimizationPriority DoGetUsefulMetric() const override;
    std::pair<TOptimizationPriority, ui64> GetMiddleUsefulMetric() const;
};

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
