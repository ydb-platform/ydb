#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/tiling.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels_impl.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>

#include <ydb/library/actors/core/log.h>

#include <algorithm>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
Tiling<TKey, TPortion>::Tiling(typename Tiling<TKey, TPortion>::TilingSettings settings, std::shared_ptr<TCounters> counters)
    : ICompactionUnit<TKey, TPortion>(counters->GetLevelCounters(0))
    , Settings(std::move(settings))
    , Accumulator(Settings.AccumulatorSettings, counters->GetAccumulatorCounters(0))
    , LastLevel(Settings.LastLevelSettings, counters->GetLevelCounters(1))
    , SharedCounters(std::move(counters))
    , PortionsInfo(std::make_shared<NLBuckets::TSimplePortionsGroupInfo>()) {
    for (ui64 i = 2; i < TILING_LAYERS_COUNT; ++i) {
        MiddleLevels.emplace(i, MiddleLevel<TKey, TPortion>(Settings.MiddleLevelSettings, SharedCounters->GetLevelCounters(i)));
    }
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void Tiling<TKey, TPortion>::DoActualize() {
    Accumulator.DoActualize();
    LastLevel.DoActualize();
    for (auto& middleLevel : MiddleLevels) {
        middleLevel.second.DoActualize();
    }
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void Tiling<TKey, TPortion>::DoAddPortion(typename TPortion::TConstPtr p) {
    switch (p->GetProduced()) {
        case NPortion::EVICTED:
        case NPortion::INACTIVE:
            AFL_VERIFY(false)("reason", "Unexpected portion type");
            break;
        case NPortion::UNSPECIFIED:
        case NPortion::COMPACTED:
            AFL_VERIFY(false)("reason", "Legacy portions");
            break;
        default:
            break;
    }

    PortionsInfo->AddPortion(p);

    if (p->GetTotalBlobBytes() < Settings.AccumulatorPortionSizeLimit) {
        Accumulator.AddPortion(p);
        InternalLevel[p->GetPortionId()] = 0;
    } else {
        const ui64 measure = LastLevel.Measure(p);
        ui8 level = 1;
        if (measure > 0) {
            ui64 threshold = 1;
            while (threshold * Settings.K <= measure) {
                threshold *= Settings.K;
                ++level;
            }
        }
        if (level <= 1) {
            LastLevel.AddPortion(p);
            InternalLevel[p->GetPortionId()] = 1;
        } else {
            level = std::min(level, static_cast<ui8>(Settings.MiddleLevelCount - 1));
            MiddleLevels.at(level).AddPortion(p);
            InternalLevel[p->GetPortionId()] = level;
        }
    }
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
void Tiling<TKey, TPortion>::DoRemovePortion(typename TPortion::TConstPtr p) {
    switch (p->GetProduced()) {
        case NPortion::EVICTED:
        case NPortion::INACTIVE:
        case NPortion::UNSPECIFIED:
        case NPortion::COMPACTED:
            AFL_VERIFY(false)("reason", "Remove not tracked portions");
            break;
        default:
            break;
    }

    PortionsInfo->RemovePortion(p);

    auto lit = InternalLevel.find(p->GetPortionId());
    if (lit == InternalLevel.end()) {
        AFL_VERIFY(false)("reason", "Remove unknown portion");
        return;
    }
    if (lit->second == 0) {
        Accumulator.RemovePortion(p);
    } else if (lit->second == 1) {
        LastLevel.RemovePortion(p);
    } else {
        auto mit = MiddleLevels.find(lit->second);
        if (mit != MiddleLevels.end()) {
            mit->second.RemovePortion(p);
        } else {
            AFL_VERIFY(false)("reason", "Bad level info");
        }
    }
    InternalLevel.erase(lit);
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
std::pair<TOptimizationPriority, ui64> Tiling<TKey, TPortion>::GetMiddleUsefulMetric() const {
    auto middleLevelsPriority = TOptimizationPriority::Zero();
    ui64 maxMiddleLevelKey = 0;
    for (const auto& [level, middleLevel] : MiddleLevels) {
        auto priority = middleLevel.DoGetUsefulMetric();
        if (middleLevelsPriority < priority) {
            middleLevelsPriority = priority;
            maxMiddleLevelKey = level;
        }
    }
    return {middleLevelsPriority, maxMiddleLevelKey};
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
std::vector<CompactionTask<TKey, TPortion>> Tiling<TKey, TPortion>::DoGetOptimizationTasks(
    TFunctionRef<bool(typename TPortion::TConstPtr)> isLocked) const {
    const auto accumulatorPriority = Accumulator.DoGetUsefulMetric();
    const auto lastLevelPriority = LastLevel.DoGetUsefulMetric();
    const auto [middleLevelsPriority, maxMiddleLevel] = GetMiddleUsefulMetric();

    if (lastLevelPriority < accumulatorPriority && middleLevelsPriority < accumulatorPriority) {
        return Accumulator.GetOptimizationTasks(isLocked);
    } else if (lastLevelPriority < middleLevelsPriority) {
        return MiddleLevels.at(maxMiddleLevel).GetOptimizationTasks(isLocked);
    } else {
        return LastLevel.GetOptimizationTasks(isLocked);
    }

    return {};
}

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
TOptimizationPriority Tiling<TKey, TPortion>::DoGetUsefulMetric() const {
    return std::max(Accumulator.DoGetUsefulMetric(), std::max(LastLevel.DoGetUsefulMetric(), GetMiddleUsefulMetric().first));
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
