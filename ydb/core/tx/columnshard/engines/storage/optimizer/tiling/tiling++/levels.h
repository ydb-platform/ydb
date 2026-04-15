#pragma once

#include <util/generic/function_ref.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/library/intersection_tree/intersection_tree.h>
namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
struct LastLevel : ICompactionUnit<TKey, TPortion, TCounter> {
    using TBase = ICompactionUnit<TKey, TPortion, TCounter>;
    using TLevelCounters = typename TBase::TLevelCounters;
    struct LastLevelSettings {
        struct Limit {
            ui64 Portions;
            ui64 Bytes;
        };

        Limit Compaction {1'000, 64 * 1024 * 1024};
        ui64 CandidatePortionsOverload = 10;
    };

    LastLevelSettings Settings;

    LastLevel(LastLevelSettings settings);

    struct TPortionByIndexKeyEndComparator {
        using is_transparent = void; // Enable heterogeneous lookup

        bool operator()(const typename TPortion::TConstPtr& left, const typename TPortion::TConstPtr& right) const {
            return left->IndexKeyEnd() < right->IndexKeyEnd();
        }

        bool operator()(const typename TPortion::TConstPtr& left, const TKey& right) const {
            return left->IndexKeyEnd() < right;
        }

        bool operator()(const TKey& left, const typename TPortion::TConstPtr& right) const {
            return left < right->IndexKeyEnd();
        }
    };

    using PortionsEndSorted = TSet<typename TPortion::TConstPtr, TPortionByIndexKeyEndComparator>;
    PortionsEndSorted Portions;
    PortionsEndSorted Candidates;

    THashMap<ui64, ui64> WidthByPortionId;

    void DoActualize() override;
    void DoAddPortion(typename TPortion::TConstPtr) override;
    void DoRemovePortion(typename TPortion::TConstPtr) override;
    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)>) const override;
    TOptimizationPriority DoGetUsefulMetric() const override;
    std::pair<typename PortionsEndSorted::iterator, typename PortionsEndSorted::iterator> Borders(typename TPortion::TConstPtr p) const;
    ui64 Measure(typename TPortion::TConstPtr p) const;
};

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
struct Accumulator : ICompactionUnit<TKey, TPortion, TCounter> {
    using TBase = ICompactionUnit<TKey, TPortion, TCounter>;
    using TLevelCounters = typename TBase::TLevelCounters;
    struct AccumulatorSettings {
        struct Limit {
            ui64 Portions;
            ui64 Bytes;
        };

        Limit Compaction {1'000, 64 * 1024 * 1024};
        Limit Trigger {1'000, 2 * 1024 * 1024};
        Limit Overload {10'000, 256 * 1024 * 1024};
    };

    AccumulatorSettings Settings;

    Accumulator(AccumulatorSettings settings, ui32 accIdx = 0);

    void DoActualize() override;
    void DoAddPortion(typename TPortion::TConstPtr) override;
    void DoRemovePortion(typename TPortion::TConstPtr) override;
    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)>) const override;
    TOptimizationPriority DoGetUsefulMetric() const override;

    TSet<typename TPortion::TConstPtr> Portions;
    ui64 TotalBlobBytes = 0;
};

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
struct MiddleLevel : ICompactionUnit<TKey, TPortion, TCounter> {
    using TBase = ICompactionUnit<TKey, TPortion, TCounter>;
    using TLevelCounters = typename TBase::TLevelCounters;
    struct MiddleLevelSettings {
        ui64 TriggerHight = 10;
        ui64 OverloadHight = 15;
    };

    MiddleLevelSettings Settings;
    ui32 LevelIdx;

    MiddleLevel(MiddleLevelSettings settings, ui32 levelIdx);

    TIntersectionTree<TKey, ui64> Intersections;
    THashMap<ui64, typename TPortion::TConstPtr> PortionById;
    THashMap<ui64, ui64> WidthByPortionId;

    void DoActualize() override;
    void DoAddPortion(typename TPortion::TConstPtr) override;
    void DoRemovePortion(typename TPortion::TConstPtr) override;
    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(TFunctionRef<bool(typename TPortion::TConstPtr)>) const override;
    TOptimizationPriority DoGetUsefulMetric() const override;
};

} // NKikimr::NOlap::NStorageOptimizer::NTiling
