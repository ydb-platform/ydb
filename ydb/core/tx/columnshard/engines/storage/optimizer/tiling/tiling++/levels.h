#pragma once

#include <util/generic/function_ref.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/library/intersection_tree/intersection_tree.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct LastLevel : ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
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

    LastLevel(LastLevelSettings settings, const TCounters& counters)
        : TBase(counters.GetLastLevelCounters())
        , Settings(settings) {
    }

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
    /// Width (LastLevel.Measure at insert time) for incremental width histogram; paired with DoAdd/DoRemove.
    THashMap<ui64, ui64> WidthByPortionId;

    void DoActualize() override {
        return;
    }

    void DoAddPortion(typename TPortion::TConstPtr p) override {
        const ui64 measure = Measure(p);
        this->Counters.Portions->AddWidth(measure);
        AFL_VERIFY(WidthByPortionId.emplace(p->GetPortionId(), measure).second)("portion_id", p->GetPortionId());
        if (measure == 0) {
            Portions.insert(p);
        } else {
            Candidates.insert(p);
        }
        this->Counters.Portions->SetHeight(Candidates.size());
    }

    void DoRemovePortion(typename TPortion::TConstPtr p) override {
        const auto wit = WidthByPortionId.find(p->GetPortionId());
        AFL_VERIFY(wit != WidthByPortionId.end())("portion_id", p->GetPortionId());
        this->Counters.Portions->RemoveWidth(wit->second);
        WidthByPortionId.erase(wit);
        if (Portions.contains(p)) {
            Portions.erase(p);
        } else if (Candidates.contains(p)) {
            Candidates.erase(p);
        } else {
            AFL_VERIFY(false);
        }
        this->Counters.Portions->SetHeight(Candidates.size());
    }

    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(
        TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const override {
        for (auto candidate : Candidates) {
            if (isLocked(candidate)) {
                continue;
            }
            std::vector<typename TPortion::TConstPtr> result;
            result.push_back(candidate);
            ui64 currentBlobBytes = candidate->GetTotalBlobBytes();
            auto [begin, end] = Borders(candidate);

            bool success = true;
            for (auto it = begin; it != end; ++it) {
                if (isLocked(*it)) {
                    success = false;
                    break;
                }
                result.push_back(*it);
                currentBlobBytes += (*it)->GetTotalBlobBytes();
                if (currentBlobBytes > Settings.Compaction.Bytes || result.size() > Settings.Compaction.Portions) {
                    break;
                }
            }
            if (success) {
                return {CompactionTask<TKey, TPortion>{result, 1}};
            }
        }
        AFL_VERIFY(!DoGetUsefulMetric().IsCritical());
        return {};
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        return TOptimizationPriority::Normalize(1, Settings.CandidatePortionsOverload, Candidates.size());
    }

    std::pair<typename PortionsEndSorted::iterator, typename PortionsEndSorted::iterator> Borders(typename TPortion::TConstPtr p) const {
        auto begin = Portions.lower_bound(p->IndexKeyStart());
        auto end = Portions.upper_bound(p->IndexKeyEnd());
        if (end != Portions.end() && (*end)->IndexKeyStart() <= p->IndexKeyEnd()) {
            ++end;
        }
        return std::make_pair(begin, end);
    }

    ui64 Measure(typename TPortion::TConstPtr p) const {
        auto [begin, end] = Borders(p);
        return std::distance(begin, end);
    }

    /// DEBUG: portion must live here before last-level counters are adjusted.
    bool HasPortion(typename TPortion::TConstPtr p) const {
        // Use WidthByPortionId for efficient ID-based lookup instead of set comparator
        return WidthByPortionId.contains(p->GetPortionId());
    }
};

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct Accumulator : ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
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

    Accumulator(AccumulatorSettings settings, const TCounters& counters)
        : TBase(counters.GetAccumulatorCounters(0))
        , Settings(settings) {
    }

    void DoActualize() override {
        return;
    }

    void DoAddPortion(typename TPortion::TConstPtr p) override {
        Portions.insert(p);
        TotalBlobBytes += p->GetTotalBlobBytes();
        this->Counters.Portions->SetHeight(Portions.size());
    }

    void DoRemovePortion(typename TPortion::TConstPtr p) override {
        Portions.erase(p);
        TotalBlobBytes -= p->GetTotalBlobBytes();
        this->Counters.Portions->SetHeight(Portions.size());
    }

    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(
        TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const override {
        if (TotalBlobBytes < Settings.Trigger.Bytes && Portions.size() < Settings.Trigger.Portions) {
            return {};
        }
        CompactionTask<TKey, TPortion> result;
        ui64 currentBlobBytes = 0;
        for (auto it : Portions) {
            if (isLocked(it)) {
                continue;
            }
            result.Portions.push_back(it);
            currentBlobBytes += it->GetTotalBlobBytes();
            if (currentBlobBytes > Settings.Compaction.Bytes || result.Portions.size() > Settings.Compaction.Portions) {
                result.TargetLevel = 0;
                return {result};
            }
        }
        AFL_VERIFY(!DoGetUsefulMetric().IsCritical());
        return {};
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        auto portionPriority = TOptimizationPriority::Normalize(Settings.Trigger.Portions, Settings.Overload.Portions, Portions.size());
        auto bytestPriority = TOptimizationPriority::Normalize(Settings.Trigger.Bytes, Settings.Overload.Bytes, TotalBlobBytes);
        return std::max(portionPriority, bytestPriority);
    }

    TSet<typename TPortion::TConstPtr> Portions;
    ui64 TotalBlobBytes = 0;

    /// DEBUG: portion must live here before accumulator counters are adjusted.
    bool HasPortion(typename TPortion::TConstPtr p) const {
        // Check by portion ID to avoid false positives from pointer comparison
        const ui64 portionId = p->GetPortionId();
        for (const auto& existing : Portions) {
            if (existing->GetPortionId() == portionId) {
                return true;
            }
        }
        return false;
    }
};

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
struct MiddleLevel : ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
    using TLevelCounters = typename TBase::TLevelCounters;
    struct MiddleLevelSettings {
        ui64 TriggerHight = 10;
        ui64 OverloadHight = 15;
    };

    MiddleLevelSettings Settings;
    ui32 LevelIdx;

    MiddleLevel(MiddleLevelSettings settings, ui32 levelIdx, const TCounters& counters)
        : TBase(counters.GetLevelCounters(levelIdx))
        , Settings(settings)
        , LevelIdx(levelIdx) {
    }

    TIntersectionTree<TKey, ui64> Intersections;
    THashMap<ui64, typename TPortion::TConstPtr> PortionById;
    /// Width at insert time (same value tiling used for routing); paired with AddWidth/RemoveWidth on this bucket.
    THashMap<ui64, ui64> WidthByPortionId;

    void RegisterRoutingWidth(ui64 portionId, ui64 width) {
        AFL_VERIFY(!WidthByPortionId.contains(portionId))("portion_id", portionId);
        WidthByPortionId.emplace(portionId, width);
        this->Counters.Portions->AddWidth(width);
    }

    void UnregisterRoutingWidth(ui64 portionId) {
        const auto it = WidthByPortionId.find(portionId);
        AFL_VERIFY(it != WidthByPortionId.end())("portion_id", portionId);
        this->Counters.Portions->RemoveWidth(it->second);
        WidthByPortionId.erase(it);
    }

    /// DEBUG: portion must be registered here before middle-level counters are adjusted.
    bool HasPortion(typename TPortion::TConstPtr p) const {
        const auto it = PortionById.find(p->GetPortionId());
        return it != PortionById.end() && it->second == p;
    }

    void DoActualize() override {
        return;
    }

    void DoAddPortion(typename TPortion::TConstPtr p) override {
        const ui64 id = p->GetPortionId();
        PortionById.emplace(id, p);
        Intersections.Add(id, p->IndexKeyStart(), p->IndexKeyEnd());
        const ui64 maxCount = Intersections.GetMaxCount();
        this->Counters.Portions->SetHeight(maxCount);
    }

    void DoRemovePortion(typename TPortion::TConstPtr p) override {
        const ui64 id = p->GetPortionId();
        Intersections.Remove(id);
        PortionById.erase(id);
        const ui64 maxCount = Intersections.GetMaxCount();
        this->Counters.Portions->SetHeight(maxCount);
    }

    std::vector<CompactionTask<TKey, TPortion>> DoGetOptimizationTasks(
        TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const override {
        CompactionTask<TKey, TPortion> result;
        auto range = Intersections.GetMaxRange();
        range.ForEachValue([&](const ui64 id) {
            auto it = PortionById.find(id);
            if (it == PortionById.end()) {
                return true;
            }
            const typename TPortion::TConstPtr& p = it->second;
            if (!isLocked(p)) {
                result.Portions.push_back(p);
            }
            return true;
        });
        if (result.Portions.size() < 2) {
            return {};
        }
        result.TargetLevel = LevelIdx;
        return {result};
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        const ui64 maxCount = Intersections.GetMaxCount();
        return TOptimizationPriority::Normalize(Settings.TriggerHight, Settings.OverloadHight, maxCount);
    }
};

} // NKikimr::NOlap::NStorageOptimizer::NTiling
