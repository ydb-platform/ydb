#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling_pp/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling_pp/settings.h>

#include <ydb/library/intersection_tree/intersection_tree.h>

#include <util/generic/function_ref.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
struct LastLevel: ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
    using TLevelCounters = typename TBase::TLevelCounters;
    TLastLevelSettings Settings;

    LastLevel(TLastLevelSettings settings, const TCounters& counters)
        : TBase(counters.GetLastLevelCounters())
        , Settings(settings)
    {
    }

    struct TPortionByIndexKeyEndComparator {
        using is_transparent = void;   // Enable heterogeneous lookup

        bool operator()(const typename TPortion::TConstPtr& left, const typename TPortion::TConstPtr& right) const {
            if (left->IndexKeyEnd() != right->IndexKeyEnd()) {
                return left->IndexKeyEnd() < right->IndexKeyEnd();
            }
            return left->GetPortionId() < right->GetPortionId();
        }

        bool operator()(const typename TPortion::TConstPtr& left, const TKey& right) const {
            return left->IndexKeyEnd() < right;
        }

        bool operator()(const TKey& left, const typename TPortion::TConstPtr& right) const {
            return left < right->IndexKeyEnd();
        }
    };

    /// Right-border-sorted views — used by Borders()/Measure() and the candidate scan.
    using PortionsEndSorted = TSet<typename TPortion::TConstPtr, TPortionByIndexKeyEndComparator>;
    PortionsEndSorted Portions;
    PortionsEndSorted Candidates;
    THashSet<ui64> PortionIds;
    THashSet<ui64> CandidateIds;
    /// Width (LastLevel.Measure at insert time) for incremental width histogram; paired with DoAdd/DoRemove.
    THashMap<ui64, ui64> WidthByPortionId;

    void DoActualize() override {
        return;
    }

    void DoAddPortion(typename TPortion::TConstPtr p) override {
        const ui64 portionId = p->GetPortionId();
        const ui64 measure = Measure(p);
        this->Counters.Portions->AddWidth(measure);
        AFL_VERIFY(WidthByPortionId.emplace(portionId, measure).second)("portion_id", portionId);
        if (measure == 0) {
            AFL_VERIFY(PortionIds.insert(portionId).second)("portion_id", portionId);
            Portions.insert(p);
        } else {
            AFL_VERIFY(CandidateIds.insert(portionId).second)("portion_id", portionId);
            Candidates.insert(p);
        }
        this->Counters.Portions->SetHeight(CandidateIds.size());
    }

    void DoRemovePortion(typename TPortion::TConstPtr p) override {
        const ui64 portionId = p->GetPortionId();
        const auto wit = WidthByPortionId.find(portionId);
        AFL_VERIFY(wit != WidthByPortionId.end())("portion_id", portionId);
        this->Counters.Portions->RemoveWidth(wit->second);
        WidthByPortionId.erase(wit);
        // Route the bucket choice via identity-keyed presence sets, not via border-sorted membership.
        if (PortionIds.erase(portionId)) {
            AFL_VERIFY(Portions.erase(p))("portion_id", portionId);
        } else if (CandidateIds.erase(portionId)) {
            AFL_VERIFY(Candidates.erase(p))("portion_id", portionId);
        } else {
            AFL_VERIFY(false)("portion_id", portionId);
        }
        this->Counters.Portions->SetHeight(CandidateIds.size());
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
                return { CompactionTask<TKey, TPortion>{ result, 1 } };
            }
        }
        return {};
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        return TOptimizationPriority::Normalize(1, Settings.CandidatePortionsOverload, CandidateIds.size());
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
    /// Identity-keyed lookup; the border-sorted set must not be queried for presence.
    bool HasPortion(typename TPortion::TConstPtr p) const {
        const ui64 id = p->GetPortionId();
        return PortionIds.contains(id) || CandidateIds.contains(id);
    }
};

template <std::totally_ordered TKey, typename TPortion>
struct Accumulator: ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
    using TLevelCounters = typename TBase::TLevelCounters;
    TAccumulatorSettings Settings;

    Accumulator(TAccumulatorSettings settings, const TCounters& counters)
        : TBase(counters.GetAccumulatorCounters(0))
        , Settings(settings)
    {
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
        AFL_VERIFY(Portions.erase(p))("portion_id", p->GetPortionId());
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
                return { result };
            }
        }
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
struct MiddleLevel: ICompactionUnit<TKey, TPortion> {
    using TBase = ICompactionUnit<TKey, TPortion>;
    using TLevelCounters = typename TBase::TLevelCounters;
    TMiddleLevelSettings Settings;
    ui32 LevelIdx;

    MiddleLevel(TMiddleLevelSettings settings, ui32 levelIdx, const TCounters& counters)
        : TBase(counters.GetLevelCounters(levelIdx))
        , Settings(settings)
        , LevelIdx(levelIdx)
    {
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
        AFL_VERIFY(PortionById.erase(id))("portion_id", id);
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
        return { result };
    }

    TOptimizationPriority DoGetUsefulMetric() const override {
        const ui64 maxCount = Intersections.GetMaxCount();
        return TOptimizationPriority::Normalize(Settings.TriggerHeight, Settings.OverloadHeight, maxCount);
    }
};

}   // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
