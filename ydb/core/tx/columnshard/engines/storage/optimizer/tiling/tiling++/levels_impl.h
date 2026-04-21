#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
LastLevel<TKey, TPortion>::LastLevel(
    typename LastLevel<TKey, TPortion>::LastLevelSettings settings,
    const TCounters& counters)
    : TBase(counters.GetLastLevelCounters())
    , Settings(settings) {
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
TOptimizationPriority LastLevel<TKey, TPortion>::DoGetUsefulMetric() const {
    return TOptimizationPriority::Normalize(1, Settings.CandidatePortionsOverload, Candidates.size());
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void LastLevel<TKey, TPortion>::DoAddPortion(typename TPortion::TConstPtr p) {
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

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void LastLevel<TKey, TPortion>::DoRemovePortion(typename TPortion::TConstPtr p) {
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

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
std::pair<typename LastLevel<TKey, TPortion>::PortionsEndSorted::iterator,
    typename LastLevel<TKey, TPortion>::PortionsEndSorted::iterator>
LastLevel<TKey, TPortion>::Borders(typename TPortion::TConstPtr p) const {
    auto begin = Portions.lower_bound(p->IndexKeyStart());
    auto end = Portions.upper_bound(p->IndexKeyEnd());
    if (end != Portions.end() && (*end)->IndexKeyStart() <= p->IndexKeyEnd()) {
        ++end;
    }
    return std::make_pair(begin, end);
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
ui64 LastLevel<TKey, TPortion>::Measure(typename TPortion::TConstPtr p) const {
    auto [begin, end] = Borders(p);
    return std::distance(begin, end);
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
std::vector<CompactionTask<TKey, TPortion>> LastLevel<TKey, TPortion>::DoGetOptimizationTasks(
    TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
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

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void LastLevel<TKey, TPortion>::DoActualize() {
    return;
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
Accumulator<TKey, TPortion>::Accumulator(
    typename Accumulator<TKey, TPortion>::AccumulatorSettings settings,
    const TCounters& counters)
    : TBase(counters.GetAccumulatorCounters(0))
    , Settings(settings) {
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void Accumulator<TKey, TPortion>::DoActualize() {
    return;
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void Accumulator<TKey, TPortion>::DoAddPortion(typename TPortion::TConstPtr p) {
    Portions.insert(p);
    TotalBlobBytes += p->GetTotalBlobBytes();
    this->Counters.Portions->SetHeight(Portions.size());
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void Accumulator<TKey, TPortion>::DoRemovePortion(typename TPortion::TConstPtr p) {
    Portions.erase(p);
    TotalBlobBytes -= p->GetTotalBlobBytes();
    this->Counters.Portions->SetHeight(Portions.size());
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
std::vector<CompactionTask<TKey, TPortion>> Accumulator<TKey, TPortion>::DoGetOptimizationTasks(
    TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
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

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
TOptimizationPriority Accumulator<TKey, TPortion>::DoGetUsefulMetric() const {
    auto portionPriority = TOptimizationPriority::Normalize(Settings.Trigger.Portions, Settings.Overload.Portions, Portions.size());
    auto bytestPriority = TOptimizationPriority::Normalize(Settings.Trigger.Bytes, Settings.Overload.Bytes, TotalBlobBytes);
    return std::max(portionPriority, bytestPriority);
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
MiddleLevel<TKey, TPortion>::MiddleLevel(
    typename MiddleLevel<TKey, TPortion>::MiddleLevelSettings settings,
    const ui32 levelIdx,
    const TCounters& counters)
    : TBase(counters.GetLevelCounters(levelIdx))
    , Settings(settings)
    , LevelIdx(levelIdx) {
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void MiddleLevel<TKey, TPortion>::DoActualize() {
    return;
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void MiddleLevel<TKey, TPortion>::DoAddPortion(typename TPortion::TConstPtr p) {
    const ui64 id = p->GetPortionId();
    PortionById.emplace(id, p);
    Intersections.Add(id, p->IndexKeyStart(), p->IndexKeyEnd());
    const ui64 maxCount = Intersections.GetMaxCount();
    this->Counters.Portions->SetHeight(maxCount);
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void MiddleLevel<TKey, TPortion>::DoRemovePortion(typename TPortion::TConstPtr p) {
    const ui64 id = p->GetPortionId();
    Intersections.Remove(id);
    PortionById.erase(id);
    const ui64 maxCount = Intersections.GetMaxCount();
    this->Counters.Portions->SetHeight(maxCount);
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
std::vector<CompactionTask<TKey, TPortion>> MiddleLevel<TKey, TPortion>::DoGetOptimizationTasks(
    TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
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

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
TOptimizationPriority MiddleLevel<TKey, TPortion>::DoGetUsefulMetric() const {
    const ui64 maxCount = Intersections.GetMaxCount();
    return TOptimizationPriority::Normalize(Settings.TriggerHight, Settings.OverloadHight, maxCount);
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
