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

// template <std::totally_ordered T>
// struct MiddleLevels : ICompactionUnit<T> {

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

//     void DoActualize() override;
//     void DoAddPortion(typename IPortionInfo<T>::TConstPtr) override;
//     void DoRemovePortion(typename IPortionInfo<T>::TConstPtr) override;
//     std::vector<CompactionTask<T>> DoGetOptimizationTasks(TFunctionRef<bool(typename IPortionInfo<T>::TConstPtr)>) const override;
//     TOptimizationPriority DoGetUsefulMetric() const override;

//     std::vector<MiddleLevel> Levels;
//     THashMap<ui64, ui8> InternalLevel;

//     // Each middle level i (i >= 2) maps directly to level counter index i.
//     // Indices 0 and 1 are reserved for accumulator and last-level respectively.
//     MiddleLevels(MiddleLevel::MiddleLevelSettings settings, const TCounters& counters);


//     void Add(TPortionInfo::TPtr p, ui8 level) {
//         Levels.at(level).Add(p);
//     }

//     void Remove(TPortionInfo::TPtr p, ui8 level) {
//         Levels.at(level).Remove(p);
//     }

//     ui8 GetMaxLevel() const {
//         auto maxPriority = TOptimizationPriority::Zero();
//         ui8 maxPriorityLevelIdx = 0;
//         for (ui8 i = 0; i < (ui8)Levels.size(); ++i) {
//             auto levelPriority = Levels[i].DoGetUsefulMetric();
//             if (maxPriority < levelPriority) {
//                 maxPriority = levelPriority;
//                 maxPriorityLevelIdx = i;
//             }
//         }
//         return maxPriorityLevelIdx;
//     }

//     TOptimizationPriority DoGetUsefulMetric() const {
//         ui8 maxLevel = GetMaxLevel();
//         auto maxPriority = Levels.at(maxLevel).DoGetUsefulMetric();
//         AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)(
//             "event", "middle_levels_get_useful_metric")(
//             "any_overloaded", maxPriority.IsCritical())(
//             "max_priority_level_idx", (ui32)maxLevel)(
//             "max_priority_level", maxPriority.GetLevel())(
//             "max_priority_weight", maxPriority.GetInternalLevelWeight())(
//             "levels_count", Levels.size());
//         return maxPriority;
//     }

//     std::vector<TPortionInfo::TPtr> GetOptimizationTasks(const std::shared_ptr<NDataLocks::TManager>& locksManager) const {
//         ui8 maxLevel = GetMaxLevel();
//         return Levels.at(maxLevel).GetOptimizationTasks(locksManager);
//     }
// };



} // NKikimr::NOlap::NStorageOptimizer::NTiling
