#pragma once

#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/tiling.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling++/levels_impl.h>
#include <ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets/planner/optimizer.h>

#include <algorithm>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

template <std::totally_ordered TKey, typename TPortion>
    requires CPortionInfoSlice<TKey, TPortion>
Tiling<TKey, TPortion>::Tiling(typename Tiling<TKey, TPortion>::TilingSettings settings)
    : TTilingCompactionCountersHolder{}
    , TBase(Counters.GetTilingCounters())
    , Settings(std::move(settings))
    , Accumulator(Settings.AccumulatorSettings, Counters)
    , LastLevel(Settings.LastLevelSettings, Counters) {
    for (ui64 i = 2; i < Settings.MiddleLevelCount; ++i) {
        MiddleLevels.emplace(i, MiddleLevel<TKey, TPortion>(Settings.MiddleLevelSettings, i, Counters));
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

    // Check if portion already exists - if so, remove it first to avoid duplicates
    const ui64 portionId = p->GetPortionId();
    const auto existingIt = InternalLevelForDebug.find(portionId);
    if (existingIt != InternalLevelForDebug.end()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)(
            "event", "tiling++_portion_already_exists")(
            "portion_id", portionId)(
            "existing_level", (ui32)existingIt->second.Level)(
            "existing_width", existingIt->second.Width)(
            "blob_bytes", p->GetTotalBlobBytes())(
            "action", "removing_before_readd");
        DoRemovePortion(p);
    }

    // PortionsInfo.AddPortion(p);

    if (p->GetTotalBlobBytes() < Settings.AccumulatorPortionSizeLimit) {
        Accumulator.AddPortion(p);
        InternalLevelForDebug[portionId] = {.Level = 0, .Width = 0};
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
            InternalLevelForDebug[portionId] = {.Level = 1, .Width = measure};
        } else {
            level = std::min(level, static_cast<ui8>(Settings.MiddleLevelCount - 1));
            MiddleLevels.at(level).RegisterRoutingWidth(portionId, measure);
            MiddleLevels.at(level).AddPortion(p);
            InternalLevelForDebug[portionId] = {.Level = level, .Width = measure};
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

    // PortionsInfo.RemovePortion(p);

    auto lit = InternalLevelForDebug.find(p->GetPortionId());
    if (lit == InternalLevelForDebug.end()) {
        AFL_VERIFY(false)("reason", "Remove unknown portion");
        return;
    }
    if (lit->second.Level == 0) {
        Accumulator.RemovePortion(p);
    } else if (lit->second.Level == 1) {
        LastLevel.RemovePortion(p);
    } else {
        auto mit = MiddleLevels.find(lit->second.Level);
        if (mit != MiddleLevels.end()) {
            mit->second.UnregisterRoutingWidth(p->GetPortionId());
            mit->second.RemovePortion(p);
        } else {
            AFL_VERIFY(false)("reason", "Bad level info");
        }
    }
    InternalLevelForDebug.erase(lit);
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
