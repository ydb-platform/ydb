#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
LastLevel<TKey, TPortion, TCounter>::LastLevel(
    typename LastLevel<TKey, TPortion, TCounter>::LastLevelSettings settings)
    : TBase(TCounter::GetLastCounter())
    , Settings(settings) {
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
TOptimizationPriority LastLevel<TKey, TPortion, TCounter>::DoGetUsefulMetric() const {
    return TOptimizationPriority::Normalize(1, Settings.CandidatePortionsOverload, Candidates.size());
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void LastLevel<TKey, TPortion, TCounter>::DoAddPortion(typename TPortion::TConstPtr p) {
    const ui64 measure = Measure(p);
    WidthByPortionId.emplace(p->GetPortionId(), measure);
    if (measure == 0) {
        Portions.insert(p);
    } else {
        Candidates.insert(p);
    }
    this->Counters.Portions->SetHeight(Candidates.size());
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void LastLevel<TKey, TPortion, TCounter>::DoRemovePortion(typename TPortion::TConstPtr p) {
    const ui64 id = p->GetPortionId();
    auto widthIt = WidthByPortionId.find(id);
    AFL_VERIFY(widthIt != WidthByPortionId.end());
    WidthByPortionId.erase(widthIt);
    if (Portions.contains(p)) {
        Portions.erase(p);
    } else if (Candidates.contains(p)) {
        Candidates.erase(p);
    } else {
        AFL_VERIFY(false);
    }
    this->Counters.Portions->SetHeight(Candidates.size());
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
std::pair<typename LastLevel<TKey, TPortion, TCounter>::PortionsEndSorted::iterator,
    typename LastLevel<TKey, TPortion, TCounter>::PortionsEndSorted::iterator>
LastLevel<TKey, TPortion, TCounter>::Borders(typename TPortion::TConstPtr p) const {
    auto begin = Portions.lower_bound(p->IndexKeyStart());
    auto end = Portions.upper_bound(p->IndexKeyEnd());
    if (end != Portions.end() && (*end)->IndexKeyStart() <= p->IndexKeyEnd()) {
        ++end;
    }
    return std::make_pair(begin, end);
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
ui64 LastLevel<TKey, TPortion, TCounter>::Measure(typename TPortion::TConstPtr p) const {
    auto [begin, end] = Borders(p);
    return std::distance(begin, end);
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
std::vector<CompactionTask<TKey, TPortion>> LastLevel<TKey, TPortion, TCounter>::DoGetOptimizationTasks(
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
            return {CompactionTask<TKey, TPortion>{result}};
        }
    }
    AFL_VERIFY(!DoGetUsefulMetric().IsCritical());
    return {};
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void LastLevel<TKey, TPortion, TCounter>::DoActualize() {
    return;
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
Accumulator<TKey, TPortion, TCounter>::Accumulator(
    typename Accumulator<TKey, TPortion, TCounter>::AccumulatorSettings settings,
    const ui32 accIdx)
    : TBase(TCounter::GetAccumulatorCounter(accIdx))
    , Settings(settings) {
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void Accumulator<TKey, TPortion, TCounter>::DoActualize() {
    return;
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void Accumulator<TKey, TPortion, TCounter>::DoAddPortion(typename TPortion::TConstPtr p) {
    Portions.insert(p);
    TotalBlobBytes += p->GetTotalBlobBytes();
    this->Counters.Portions->SetHeight(Portions.size());
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void Accumulator<TKey, TPortion, TCounter>::DoRemovePortion(typename TPortion::TConstPtr p) {
    Portions.erase(p);
    TotalBlobBytes -= p->GetTotalBlobBytes();
    this->Counters.Portions->SetHeight(Portions.size());
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
std::vector<CompactionTask<TKey, TPortion>> Accumulator<TKey, TPortion, TCounter>::DoGetOptimizationTasks(
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
            return {result};
        }
    }
    AFL_VERIFY(!DoGetUsefulMetric().IsCritical());
    return {};
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
TOptimizationPriority Accumulator<TKey, TPortion, TCounter>::DoGetUsefulMetric() const {
    auto portionPriority = TOptimizationPriority::Normalize(Settings.Trigger.Portions, Settings.Overload.Portions, Portions.size());
    auto bytestPriority = TOptimizationPriority::Normalize(Settings.Trigger.Bytes, Settings.Overload.Bytes, TotalBlobBytes);
    return std::max(portionPriority, bytestPriority);
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
MiddleLevel<TKey, TPortion, TCounter>::MiddleLevel(
    typename MiddleLevel<TKey, TPortion, TCounter>::MiddleLevelSettings settings,
    const ui32 levelIdx)
    : TBase(TCounter::GetMiddleCounter(levelIdx))
    , Settings(settings)
    , LevelIdx(levelIdx) {
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void MiddleLevel<TKey, TPortion, TCounter>::DoActualize() {
    return;
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void MiddleLevel<TKey, TPortion, TCounter>::DoAddPortion(typename TPortion::TConstPtr p) {
    const ui64 id = p->GetPortionId();
    PortionById.emplace(id, p);
    WidthByPortionId.emplace(id, 0);
    Intersections.Add(id, p->IndexKeyStart(), p->IndexKeyEnd());
    const ui64 maxCount = Intersections.GetMaxCount();
    this->Counters.Portions->SetHeight(maxCount);
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void MiddleLevel<TKey, TPortion, TCounter>::DoRemovePortion(typename TPortion::TConstPtr p) {
    const ui64 id = p->GetPortionId();
    auto widthIt = WidthByPortionId.find(id);
    AFL_VERIFY(widthIt != WidthByPortionId.end());
    WidthByPortionId.erase(widthIt);
    Intersections.Remove(id);
    PortionById.erase(id);
    const ui64 maxCount = Intersections.GetMaxCount();
    this->Counters.Portions->SetHeight(maxCount);
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
std::vector<CompactionTask<TKey, TPortion>> MiddleLevel<TKey, TPortion, TCounter>::DoGetOptimizationTasks(
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
    return {result};
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
TOptimizationPriority MiddleLevel<TKey, TPortion, TCounter>::DoGetUsefulMetric() const {
    const ui64 maxCount = Intersections.GetMaxCount();
    return TOptimizationPriority::Normalize(Settings.TriggerHight, Settings.OverloadHight, maxCount);
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
