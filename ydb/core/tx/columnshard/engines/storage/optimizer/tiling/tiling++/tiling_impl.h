#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/tiling.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels_impl.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>

#include <ydb/library/actors/core/log.h>

#include <algorithm>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
Tiling<TKey, TPortion, TCounter>::Tiling(typename Tiling<TKey, TPortion, TCounter>::TilingSettings settings)
    : TBase(TCounter::GetMainCounter())
    , Settings(std::move(settings))
    , Accumulator(Settings.AccumulatorSettings)
    , LastLevel(Settings.LastLevelSettings) {
    for (ui64 i = 2; i < Settings.MiddleLevelCount; ++i) {
        MiddleLevels.emplace(i, MiddleLevel<TKey, TPortion, TCounter>(Settings.MiddleLevelSettings, i));
    }
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void Tiling<TKey, TPortion, TCounter>::DoActualize() {
    Accumulator.DoActualize();
    LastLevel.DoActualize();
    for (auto& middleLevel : MiddleLevels) {
        middleLevel.second.DoActualize();
    }
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void Tiling<TKey, TPortion, TCounter>::DoAddPortion(typename TPortion::TConstPtr p) {
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

    // PortionsInfo.AddPortion(p);

    if (p->GetTotalBlobBytes() < Settings.AccumulatorPortionSizeLimit) {
        Accumulator.AddPortion(p);
        InternalLevel[p->GetPortionId()] = {.Level = 0, .Width = 0};
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
            LastLevel.Counters.Portions->AddWidth(measure);
            InternalLevel[p->GetPortionId()] = {.Level = 1, .Width = measure};
        } else {
            level = std::min(level, static_cast<ui8>(Settings.MiddleLevelCount - 1));
            MiddleLevels.at(level).AddPortion(p);
            MiddleLevels.at(level).Counters.Portions->AddWidth(measure);
            MiddleLevels.at(level).WidthByPortionId[p->GetPortionId()] = measure;
            InternalLevel[p->GetPortionId()] = {.Level = level, .Width = measure};
        }
    }
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
void Tiling<TKey, TPortion, TCounter>::DoRemovePortion(typename TPortion::TConstPtr p) {
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

    // PortionsInfo.RemovePortion(p);

    auto lit = InternalLevel.find(p->GetPortionId());
    if (lit == InternalLevel.end()) {
        AFL_VERIFY(false)("reason", "Remove unknown portion");
        return;
    }
    if (lit->second.Level == 0) {
        Accumulator.RemovePortion(p);
    } else if (lit->second.Level == 1) {
        LastLevel.Counters.Portions->RemoveWidth(lit->second.Width);
        LastLevel.RemovePortion(p);
    } else {
        auto mit = MiddleLevels.find(lit->second.Level);
        if (mit != MiddleLevels.end()) {
            mit->second.Counters.Portions->RemoveWidth(lit->second.Width);
            mit->second.WidthByPortionId[p->GetPortionId()] = lit->second.Width;
            mit->second.RemovePortion(p);
        } else {
            AFL_VERIFY(false)("reason", "Bad level info");
        }
    }
    InternalLevel.erase(lit);
}

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
std::pair<TOptimizationPriority, ui64> Tiling<TKey, TPortion, TCounter>::GetMiddleUsefulMetric() const {
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

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
std::vector<CompactionTask<TKey, TPortion>> Tiling<TKey, TPortion, TCounter>::DoGetOptimizationTasks(
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

template <std::totally_ordered TKey, typename TPortion, typename TCounter>
    requires CPortionInfoSlice<TKey, TPortion>
TOptimizationPriority Tiling<TKey, TPortion, TCounter>::DoGetUsefulMetric() const {
    return std::max(Accumulator.DoGetUsefulMetric(), std::max(LastLevel.DoGetUsefulMetric(), GetMiddleUsefulMetric().first));
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
